const {
  AolLogger,
  AolError,
  AolPubsub,
  AolFirestore,
  AolUtils,
} = require('@ilo-aol/utils');
const { OpenTelemetry } = require('@ilo-aol/utils/components/opentelemetry');
const { propagation, context } = require('@opentelemetry/api');
const { unsuppressTracing } = require('@opentelemetry/core');
const {
  createSubscriberSpanOptions,
} = require('@ilo-aol/utils/components/opentelemetry/gcp/pubsub/otGcpPubSubSpanBuilder');

const logger = new AolLogger();
let pubsub;
const KNOWN_SYSTEMS = ['iws'];
const serviceName =
  process.env.SERVICE_NAME ??
  process.env.npm_package_name ??
  'aol-pick-request-service';
const {
  configureOpenTelemetry,
} = require('@ilo-aol/utils/components/opentelemetry');

configureOpenTelemetry({
  serviceName,
  ignoreIncomingPaths: [/\//],
});

const subscription = 'ingka-ilo-aol-pick-request-topic';

const getMessageAttributes = (message) => {
  const attributes = message.attributes ?? [];

  const trace = attributes['X-Cloud-Trace-Context'];
  if (!attributes['x-cloud-trace-context']) {
    attributes['x-cloud-trace-context'] = trace;
  }

  return attributes;
};

const withOpenTelemetry = (callback) => (message, event) => {
  if (!message || !message.attributes) {
    throw new Error(
      `Invalid message received from the push-subscription[${subscription}]`
    );
  }

  const attributes = getMessageAttributes(message);

  const parentContext = propagation.extract(
    unsuppressTracing(context.active()),
    attributes
  );
  const spanOptions = createSubscriberSpanOptions(subscription, {
    messageId: event.eventId,
    ...message,
  });
  const spanName = `Message received from subscription[${subscription}]`;

  return OpenTelemetry.getTracer().startActiveSpan(
    spanName,
    spanOptions,
    parentContext,
    async (span) => {
      logger.logDebug(
        `Successfully initialised OT context from PubSub push request: ` +
          `[traceId: ${OpenTelemetry.getActiveTraceId()}, spanId: ${OpenTelemetry.getActiveSpanId()}]`
      );

      let result;
      try {
        result = await callback(message, event);
      } catch (err) {
        span.recordException(err);
        throw err;
      }

      span.end();

      return result;
    }
  );
};

/**
 * @desc pullMessage
 * @param message
 * @param event
 * @returns {Promise<void>}
 */
async function pullMessage(message, event) {
  let pickRequestPayload;
  try {
    pubsub = new AolPubsub(
      {},
      {
        event,
        data: message.data,
        attributes: message.attributes,
        sourceTopic: 'pick-request-topic',
      }
    );

    if (!(await pubsub.isDuplicateMessage(event.eventId))) {
      const operation = message.attributes['operation'];
      const wmsIdentifier = message.attributes['wmsIdentifier'];
      const automationSystemIdentifier =
        message.attributes['automationSystemIdentifier'];
      const targetType = automationSystemIdentifier
        .split('-')[0]
        .toLocaleLowerCase();

      pickRequestPayload = AolUtils.parsePayload(message.data); // Decode base64 message
      const wmsPickRequestIdentifier =
        pickRequestPayload.wmsPickRequestIdentifier;

      const aolFirestore = new AolFirestore();

      if (operation === 'cancel') {
        const pickRequestIdentifier = pickRequestPayload.pickRequestIdentifier;
        const pickRequest = await aolFirestore.getPickRequest(
          pickRequestIdentifier,
          wmsIdentifier
        );
        if (!pickRequest) {
          throw new AolError(
            'Could not find pick request with ID: ' + pickRequestIdentifier
          );
        }

        const pickRequestData = pickRequest.data();
        const cancellableStatuses = ['allocated', 'requested', 'created'];

        if (cancellableStatuses.includes(pickRequestData.status)) {
          logger.logInfo(
            'Processed pick-request cancellation. | Status: cancellation-requested',
            { pickRequestData }
          );
          await pubsub
            .publish('routing-topic', pickRequestData, {
              target: pickRequestData.automationSystemIdentifier,
              ...message.attributes,
            })
            .catch((err) => {
              throw new AolError(
                'Could not publish message for routing service.',
                err,
                true
              );
            });
          return;
        }
        logger.logInfo(
          'Did not process pick-request cancellation as picking is already in progress or finished',
          { pickRequestData }
        );
      }

      if (operation === 'update') {
        const pickRequestIdentifier = pickRequestPayload.pickRequestIdentifier;
        const pickRequest = await aolFirestore.getPickRequest(
          pickRequestIdentifier,
          wmsIdentifier
        );
        if (!pickRequest) {
          throw new AolError(
            'Could not find pick request with ID: ' + pickRequestIdentifier
          );
        }
        const pickRequestData = pickRequest.data();
        pickRequestPayload.automationPickRequestIdentifier =
          pickRequestData.automationPickRequestIdentifier;
        pickRequestPayload.orderReference = pickRequestData.orderReference;
        pickRequestPayload.pickIdentifier = pickRequestData.pickIdentifier;

        const updatePickRequest = Object.assign(
          {
            wmsPickRequestIdentifier: pickRequestPayload.pickRequestIdentifier,
          },
          pickRequestPayload.cutOffTime
            ? {
                cutOffTime: AolUtils.parseDateTime(
                  pickRequestPayload.cutOffTime
                ),
              }
            : null,
          pickRequestPayload.pickBeforeTime
            ? {
                pickBeforeTime: AolUtils.parseDateTime(
                  pickRequestPayload.pickBeforeTime
                ),
              }
            : null,
          pickRequestPayload.handoverLocation
            ? { handoverLocation: pickRequestPayload.handoverLocation }
            : null,
          pickRequestPayload.manuallyPicked
            ? { status: 'manually-picked' }
            : null
        );
        await aolFirestore
          .updateOrInsertDoc(
            'pick-request',
            `${wmsIdentifier}-${pickRequestIdentifier}`,
            updatePickRequest
          )
          .catch((err) => {
            throw new AolError('Could not save to firestore.', err);
          });

        if (pickRequestPayload.manuallyPicked) {
          const cancellableStatuses = ['allocated', 'requested', 'created'];

          if (cancellableStatuses.includes(pickRequestData.status)) {
            await pubsub
              .publish('routing-topic', pickRequestPayload, message.attributes)
              .catch((err) => {
                throw new AolError(
                  'Could not publish message for routing service.',
                  err,
                  true
                );
              });
          }
        }

        if (
          !pickRequestPayload.manuallyPicked &&
          KNOWN_SYSTEMS.includes(targetType)
        ) {
          pickRequestPayload.manuallyPicked = false;
          await pubsub
            .publish('routing-topic', pickRequestPayload, message.attributes)
            .catch((err) => {
              throw new AolError(
                'Could not publish message for routing service.',
                err,
                true
              );
            });
        }
        logger.logInfo('Processed pick-request update.', {
          pickRequestPayload,
        });
      }

      if (operation === 'create') {
        // check if pick request already exists. Considering duplicated messages send by the WMS.
        // The creation of a pick request should happen only once.
        const pickRequestDocument = await aolFirestore.getPickRequest(
          wmsPickRequestIdentifier,
          wmsIdentifier
        );
        if (!pickRequestDocument) {
          // Build firestore document
          const pickRequest = buildPickRequest(
            pickRequestPayload,
            automationSystemIdentifier,
            wmsIdentifier
          );

          // Save to firestore
          const doc = await aolFirestore
            .updateOrInsertDoc(
              'pick-request',
              `${pickRequest.wmsIdentifier}-${wmsPickRequestIdentifier}`,
              pickRequest
            )
            .catch((err) => {
              throw new AolError('Could not save to firestore.', err);
            });

          logger.logInfo('Processed pick-request. | Status: requested', {
            pickRequest: pickRequest,
            documentId: doc.id,
          });

          // Send message to routing service
          await pubsub
            .publish('routing-topic', pickRequest, {
              target: automationSystemIdentifier,
              ...message.attributes,
            })
            .catch((err) => {
              throw new AolError(
                'Could not publish message for routing service.',
                err,
                true
              );
            });
        } else {
          logger.logWarn(
            'Duplicate create pick-request message for pick-request ' +
              `${wmsIdentifier}-${wmsPickRequestIdentifier}`
          );
        }
      }
    }
  } catch (error) {
    logger.logError(error);
    return await pubsub.handleFlawedPubSubMessage(error);
  }
}

/**
 * Build a generic pick request object to persist within AOL.
 * @param payload - payload consisting of raw data and enriched pick request lines
 * @param automationSystemIdentifier Automation-System identifier
 * @param wmsIdentifier WMS identifier
 * @return JSON object containing a generic pick request
 */
function buildPickRequest(payload, automationSystemIdentifier, wmsIdentifier) {
  // Convert all ISO String Data Times to Date
  const dateTimeFields = [
    'pickBeforeTime',
    'cutOffTime',
    'service.fromTime',
    'service.toTime',
  ];
  AolUtils.parseDateTimes(payload, dateTimeFields);
  return {
    ...payload,
    wmsIdentifier,
    automationSystemIdentifier,
    status: 'requested',
  };
}

module.exports = {
  buildPickRequest,
  pullMessage: withOpenTelemetry(pullMessage),
};
