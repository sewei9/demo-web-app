// Test resources
global.chai = require('chai'); // Assertions
global.sinon = require('sinon'); // Stubs and spies
const sinonChai = require('sinon-chai'); // Improves test handling for sinon-stubs in chai
const chaiAsPromised = require('chai-as-promised'); // Promise support for chai

chai.should();
chai.use(sinonChai);
chai.use(chaiAsPromised);

// Helper classes
const { AolFirestore } = require('@ilo-aol/utils');

beforeEach(async () => {
  global._firestoreEmulator = new AolFirestore({ projectId: 'test' });
  await _firestoreEmulator
    .collection('pick-request')
    .doc('test-pick-request')
    .set({
      orderReference: 'test-orderReference',
      pickIdentifier: 'test-pickIdentifier',
      status: 'allocated',
      wmsIdentifier: 'TEST-WMS-1',
      wmsPickRequestIdentifier: 'test-pick-request',
      automationSystemIdentifier: 'test-automationSystemIdentifier',
    });

  // creating second pickrequest wir id 2 in TEST-WMS-1
  await _firestoreEmulator.collection('pick-request').doc('TEST-WMS-1-2').set({
    orderReference: 'test-orderReference',
    pickIdentifier: 'test-pickIdentifier',
    status: 'allocated',
    wmsIdentifier: 'TEST-WMS-1',
    wmsPickRequestIdentifier: '2',
    automationSystemIdentifier: 'test-automationSystemIdentifier',
  });
  await _firestoreEmulator.collection('pick-request').doc('TEST-WMS-1-3').set({
    orderReference: 'test-orderReference',
    pickIdentifier: 'test-pickIdentifier',
    status: 'manually-picked',
    wmsIdentifier: 'TEST-WMS-3',
    wmsPickRequestIdentifier: 'test-pick-request',
    automationSystemIdentifier: 'test-automationSystemIdentifier',
  });
});

afterEach(() => {
  sinon.restore();
});
