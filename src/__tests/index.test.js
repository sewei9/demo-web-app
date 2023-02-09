require('./initialize.test');
const { assert } = require('chai');
const index = require('../index');
const payload = require('./resources/payload.json');
const plainPayload = require('./resources/plainPayload.json');
const payloadWithRequestId2 = require('./resources/payload_id2.json');
const payloadDataNull = require('./resources/payload_data_null.json');
const payloadDataEmptyString = require('./resources/payload_data_empty_string.json');
const updatePayloadWithManualPickedStatus = require('./resources/updateManuallyPicked.json');
const {
  AolPubsub,
  AolLogger,
  AolError,
  AolFirestore,
} = require('@ilo-aol/utils');
const payloadDataEmptyObject = require('./resources/payload_data_empty_object.json');
const cancelPayload = require('./resources/cancelPayload.json');
const updatePayload = require('./resources/updatePayload.json');
const updatePayloadWithoutCutOffTime = require('./resources/updatePayloadWithoutCutOffTime.json');
const UpdatePayloadWrongAutomation = require('./resources/UpdatePayloadWrongAutomation.json');

let aolPubsubDuplicateMessageStub;
let aolPubsubPublishStub;
let aolPubsubHandleFlawedPubSubMessageStub;
let aolLoggerLogInfoStub;
let aolLoggerLogErrorStub;

beforeEach(() => {
  aolPubsubDuplicateMessageStub = sinon.stub(
    AolPubsub.prototype,
    'isDuplicateMessage'
  );
  aolPubsubDuplicateMessageStub.withArgs('existingId').resolves(true);
  aolPubsubDuplicateMessageStub.withArgs('notExistingId').resolves(false);

  aolPubsubPublishStub = sinon.stub(AolPubsub.prototype, 'publish');
  aolPubsubPublishStub.resolves();

  aolPubsubHandleFlawedPubSubMessageStub = sinon.stub(
    AolPubsub.prototype,
    'handleFlawedPubSubMessage'
  );
  aolLoggerLogInfoStub = sinon.stub(AolLogger.prototype, 'logInfo');
  aolLoggerLogInfoStub.resolves();

  aolLoggerLogErrorStub = sinon.stub(AolLogger.prototype, 'logError');
  aolLoggerLogErrorStub.resolves();
});

afterEach(() => sinon.restore());

describe('pick-request-service', () => {
  describe('Handle duplicated messages from client', () => {
    it('should not consider a duplicate create message coming from MHS', async () => {
      // a message with the same pickrequest a;ready exists for that wms. see initialize.test.js
      // but the eventid of pubsub is new. for example when the client sends the same message twice
      await index.pullMessage(payloadWithRequestId2.data, {
        event: 'notExistingId',
      });
      const document = await _firestoreEmulator.getDoc(
        'pick-request',
        'TEST-WMS-1-2'
      );
      document.data().status.should.equal('allocated');
    });
  });
  describe('pick request cancel', () => {
    it('should return a pick request if pickRequestIdentifier and wmsIdentifier is existing', async () => {
      // Given
      await index.pullMessage(cancelPayload.data, { eventId: 'notExistingId' });

      // Then
      aolPubsubPublishStub.should.have.been.calledOnce;
      aolLoggerLogInfoStub.should.have.been.calledOnce;
      aolLoggerLogErrorStub.should.not.have.been.called;
    });
  });

  describe('pick request update', () => {
    it('should return a pick request if pickRequestIdentifier and wmsIdentifier is existing', async () => {
      // Given
      await index.pullMessage(updatePayload.data, { eventId: 'notExistingId' });

      // Then
      aolPubsubPublishStub.should.have.been.calledOnce;
      aolLoggerLogInfoStub.should.have.been.calledOnce;
      aolLoggerLogErrorStub.should.not.have.been.called;

      const document = await _firestoreEmulator.getDoc(
        'pick-request',
        'TEST-WMS-1-2'
      );
      assert.deepEqual(
        new Date('2021-10-08T22:00:00Z'),
        document.data().cutOffTime.toDate()
      );
      assert.deepEqual(
        new Date('2021-10-08T21:00:00Z'),
        document.data().pickBeforeTime.toDate()
      );
    });

    it('should return a pick request even if cutOffTime is missing when there is a pickBeforeTime', async () => {
      // Given
      await index.pullMessage(updatePayloadWithoutCutOffTime.data, {
        eventId: 'notExistingId',
      });

      // Then
      aolPubsubPublishStub.should.have.been.calledOnce;
      aolLoggerLogInfoStub.should.have.been.calledOnce;
      aolLoggerLogErrorStub.should.not.have.been.called;

      const document = await _firestoreEmulator.getDoc(
        'pick-request',
        'TEST-WMS-1-2'
      );
      assert.isUndefined(document.data().cutOffTime);
      assert.deepEqual(
        new Date('2021-10-08T21:00:00Z'),
        document.data().pickBeforeTime.toDate()
      );
    });
    it('should not publish pick-update  to routing-topic for  automation other than iws', async () => {
      // Given
      await index.pullMessage(UpdatePayloadWrongAutomation.data, {
        eventId: 'notExistingId',
      });

      // Then
      aolPubsubPublishStub.should.not.have.been.called;
      aolLoggerLogInfoStub.should.have.been.calledOnce;
      aolLoggerLogErrorStub.should.not.have.been.called;
    });
    it('should check for manually picked status update', async () => {
      const input = updatePayloadWithManualPickedStatus.data;

      // Given
      await index.pullMessage(input, { eventId: 'notExistingId' });

      // Then
      aolPubsubPublishStub.should.have.been.calledOnce;
      aolLoggerLogInfoStub.should.have.been.calledOnce;
      aolLoggerLogErrorStub.should.not.have.been.called;

      const document = await _firestoreEmulator.getDoc(
        'pick-request',
        'TEST-WMS-1-3'
      );

      assert.deepEqual('manually-picked', document.data().status);
    });

    it('should send for manually picked cancellation request if status in allocated,requested,created', async () => {
      const input = updatePayloadWithManualPickedStatus.data;
      // using stub only here because the rest uses firebase tools
      const aolFirestoreStub = sinon.stub(
        AolFirestore.prototype,
        'getPickRequest'
      );
      aolFirestoreStub.resolves({ status: 'cancellation-locked' });

      // Given
      await index.pullMessage(input, { eventId: 'notExistingId' });

      // Then
      aolPubsubPublishStub.should.not.have.been.called;
    });
  });

  describe('pullmessage should handle existing and notExistingId', () => {
    it('should call dbTargetSystem, firestore and pubsub if pick request is not existing', async () => {
      // Given
      // using stub only here because the rest uses firebase tools
      const aolFirestoreStub = sinon.stub(
        AolFirestore.prototype,
        'updateOrInsertDoc'
      );
      aolFirestoreStub.resolves({ id: '' });
      // When
      await index.pullMessage(payload.data, { eventId: 'notExistingId' });

      // Then
      aolFirestoreStub.should.have.been.calledOnce;
      aolPubsubPublishStub.should.have.been.calledOnce;
      aolLoggerLogInfoStub.should.have.been.calledOnce;
      aolLoggerLogErrorStub.should.not.have.been.called;
    });

    it('should not call dbTargetSystem, firestore and pubsub if pick request is dublicate', async () => {
      // Given

      // When
      await index.pullMessage(payload.data, { eventId: 'existingId' });

      // When
      aolPubsubPublishStub.should.not.have.been.called;
    });
  });

  describe('pullmessage should throw exceptions', () => {
    it('should throw AOLError for empty JSON Payload', async () => {
      aolPubsubHandleFlawedPubSubMessageStub.throws(new AolError('some error'));
      await index
        .pullMessage(payloadDataNull.data, {
          eventId: 'notExistingId',
          timestamp: Date.now(),
        })
        .should.be.rejectedWith(AolError);
      aolLoggerLogErrorStub.should.have.been.called;
    });

    it('should throw AOLError for empty string JSON Payload', async () => {
      aolPubsubHandleFlawedPubSubMessageStub.throws(new AolError('some error'));
      await index
        .pullMessage(payloadDataEmptyString.data, {
          eventId: 'notExistingId',
          timestamp: Date.now(),
        })
        .should.be.rejectedWith(Error);
      aolLoggerLogErrorStub.should.have.been.called;
    });

    it('should throw Error if save document in firestore throw an Error', async () => {
      // Given
      // using stub only here because the rest uses firebase tools
      const aolFirestoreStub = sinon.stub(
        AolFirestore.prototype,
        'updateOrInsertDoc'
      );
      aolFirestoreStub.rejects(new Error('SomeFirestoreError'));
      aolPubsubHandleFlawedPubSubMessageStub.throws(new AolError('some error'));

      // When
      await index
        .pullMessage(payload.data, {
          eventId: 'notExistingId',
          timestamp: Date.now(),
        })
        .should.be.rejectedWith(Error);

      // Then
      aolLoggerLogErrorStub.should.have.been.called;
      aolLoggerLogInfoStub.should.not.have.been.called;
    });

    it('should throw Error if pubsub publishing throws an Error', async () => {
      // Given
      aolPubsubHandleFlawedPubSubMessageStub.throws(new AolError('some error'));
      aolPubsubPublishStub.rejects(new Error('SomePubsubError'));

      await index
        .pullMessage(payload.data, {
          eventId: 'notExistingId',
          timestamp: Date.now(),
        })
        .should.be.rejectedWith(AolError);
      aolLoggerLogErrorStub.should.have.been.called;
    });

    it('should throw Error if client identifier is missing', async () => {
      // Given
      aolPubsubHandleFlawedPubSubMessageStub.throws(
        new AolError(
          'Client Identifier is missing in the attributes of pubsub message'
        )
      );
      const testPayload = {
        data: {
          attributes: {
            'X-Cloud-Trace-Context': 'jwjwjwjqii',
            clientIdentifier: null,
          },
        },
      };

      await index
        .pullMessage(testPayload.data, {
          eventId: 'notExistingId',
          timestamp: Date.now(),
        })
        .should.be.rejectedWith(
          'Client Identifier is missing in the attributes of pubsub message'
        );
      aolLoggerLogErrorStub.should.have.been.called;
    });

    it('should throw Error if client identifier is an empty string', async () => {
      // Given
      const testPayload = {
        data: {
          attributes: {
            'X-Cloud-Trace-Context': 'jwjwjwjqii',
            clientIdentifier: '',
          },
        },
      };
      aolPubsubHandleFlawedPubSubMessageStub.throws(
        new AolError(
          'Client Identifier is missing in the attributes of pubsub message'
        )
      );

      await index
        .pullMessage(testPayload.data, {
          eventId: 'notExistingId',
          timestamp: Date.now(),
        })
        .should.be.rejectedWith(
          'Client Identifier is missing in the attributes of pubsub message'
        );
      aolLoggerLogErrorStub.should.have.been.called;
    });

    it('should throw Error if payload data contains invalid data', async () => {
      // Given
      aolPubsubHandleFlawedPubSubMessageStub.throws(
        new AolError('Message does not contain valid data.')
      );
      await index
        .pullMessage(payloadDataEmptyObject.data, {
          eventId: 'notExistingId',
          timestamp: Date.now(),
        })
        .should.be.rejectedWith('Message does not contain valid data.');

      aolLoggerLogErrorStub.should.have.been.called;
    });
  });

  describe('pullmessage should log exceptions', () => {
    it('should log AOLError and stop retry for old event with invalid data', async () => {
      await index.pullMessage(payloadDataEmptyObject.data, {
        eventId: 'notExistingId',
      }).should.be.fulfilled;
      aolLoggerLogErrorStub.should.have.been.called;
    });
  });

  describe('verify buildPickRequest', () => {
    it('should correctly map the pick request and convert dateTimes', async () => {
      const wmsIdentifier = 'MHS-STO-TEST';
      const automationSystemIdentifier = 'AS-STO-TEST';
      const result = await index.buildPickRequest(
        plainPayload,
        automationSystemIdentifier,
        wmsIdentifier
      );
      new Date(plainPayload.cutOffTime).should.be.deep.equal(result.cutOffTime);
      new Date(plainPayload.pickBeforeTime).should.be.deep.equal(
        result.pickBeforeTime
      );
      new Date(plainPayload.service.fromTime).should.be.deep.equal(
        result.service.fromTime
      );
      new Date(plainPayload.service.toTime).should.be.deep.equal(
        result.service.toTime
      );
      wmsIdentifier.should.be.deep.equal(result.wmsIdentifier);
      automationSystemIdentifier.should.be.deep.equal(
        result.automationSystemIdentifier
      );
    });
  });
});
