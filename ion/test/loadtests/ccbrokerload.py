#!/usr/bin/env python

"""
@file ion/test/loadtests/brokerload.py
@author Michael Meisinger
@author Adam R. Smith
@brief Creates load on an AMQP broker
"""

import uuid
import sys
import time

from twisted.internet import defer, reactor
from carrot import connection, messaging

from ion.test.loadtest import LoadTest, LoadTestOptions
import ion.util.procutils as pu
from ion.core import bootstrap, ioninit
from ion.core.cc import service
from ion.core.cc import container
from ion.core.cc.container import Id, Container
from ion.core.messaging.receiver import Receiver
from ion.core.process import process
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

simple_type = object_utils.create_type_identifier(object_id=30001, version=1)
lorem_ipsum = '''Lorem ipsum dolor sit amet, consectetur adipisicing elit,
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut
    enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut
    aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit
    in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
    Excepteur sint occaecat cupidatat non proident, sunt in culpa qui
    officia deserunt mollit anim id est laborum.'''

class CCBrokerTestError(Exception):
    """ An exception class for the Capability Container load test. """

class CCBrokerTestProcess(ServiceProcess):
    """ A load test that attempts to simulate typical Capability Container activity. """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='cc_broker_load', version='0.4.0', dependencies=[])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        self.mc = MessageClient(proc=self)

        log.info('SLC_INIT CCBrokerTestProcess')

    @defer.inlineCallbacks
    def op_message_simple(self, simple_msg, headers, msg):
        log.info('op_message_simple: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if simple_msg.MessageType != simple_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise CCBrokerTestError('Unexpected type received \n %s' % str(simple_msg))

        # Make sure to log stuff in case there are performance issues with logging.
        log.info('Received a simple message of length %d.' % (len(msg.body)))
        reply = yield self.mc.create_instance(simple_type, MessageName='simple message reply')

        reply.MessageObject = simple_msg.MessageObject
        # Just reverse the contents.
        reply.body = reply.body[::-1]

        # The following line shows how to reply to a message
        # The api for reply may be refactored later on so that there is just the one argument...
        yield self.reply_ok(msg, reply)

class CCBrokerTestClient(ServiceClient):
    """ A load test that attempts to simulate typical Capability Container activity. """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'cc_broker_load'
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def message_simple(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('message_simple', msg)

        defer.returnValue(content)

factory = ProcessFactory(CCBrokerTestProcess)

class CCBrokerTestOptions(LoadTestOptions):
    optParameters = [
          ['host', 'h', 'localhost', 'Broker host name.']
        , ['port', 'p', 5672, 'Broker port.']
        , ['vhost', 'v', '/', 'Broker vhost.']
        , ['heartbeat', None, 0, 'Heartbeat rate [seconds].']
        , ['monitor', 'm', 3, 'Monitor poll rate [seconds].']

        , ['procs', None, 1, 'Number of capability container service processes to run.']
        , ['clients', None, 1, 'Number of capability container service clients to run.']
    ]
    optFlags = [
    ]


class CCBrokerTest(LoadTest):

    procRegistry = process.procRegistry
    container = None
    twisted_container_service = None #hack

    @defer.inlineCallbacks
    def _start_container(self):
        """
        Starting and initialzing the container with a connection to a broker.
        """
        #mopt = {}
        mopt = service.Options()
        
        mopt['broker_host'] = self.opts['host']
        mopt['broker_port'] = self.opts['port']
        mopt['broker_vhost'] = self.opts['vhost']
        mopt['broker_heartbeat'] = self.opts['heartbeat']
        mopt['no_shell'] = True
        mopt['scripts'] = None

        # Little trick to have no consecutive failures if previous setUp() failed
        if Container._started:
            log.error("PROBLEM: Previous test did not stop container. Fixing...")
            yield self._stop_container()

        twisted_container_service = service.CapabilityContainer(mopt)
        yield twisted_container_service.startService()
        self.twisted_container_service = twisted_container_service

        # Manually perform some ioncore initializations
        yield bootstrap.init_ioncore()

        self.procRegistry = process.procRegistry
        self.sup = yield bootstrap.create_supervisor()

        log.info("============ %s ===" % self.container)

    @defer.inlineCallbacks
    def _stop_container(self):
        """
        Taking down the container's connection to the broker an preparing for
        reinitialization.
        """
        log.info("Closing ION container")
        if self.twisted_container_service: #hack
            yield self.twisted_container_service.stopService()

        self.test_sup = None
        # Cancel any delayed calls, such as timeouts, looping calls etc.
        dcs = reactor.getDelayedCalls()
        if len(dcs) > 0:
            log.debug("Cancelling %s delayed reactor calls!" % len(dcs))
        for dc in dcs:
            # Cancel the registered delayed call (this is non-async)
            dc.cancel()

        # The following is waiting a bit for any currently consumed messages
        # It also prevents the arrival of new messages
        Receiver.rec_shutoff = True
        msgstr = ""
        if len(Receiver.rec_messages) > 0:
            for msg in Receiver.rec_messages.values():
                msgstr += str(msg.payload) + ", \n"
            #log.warn("Content rec_messages: "+str(Receiver.rec_messages))
            log.warn("%s messages still being processed: %s" % (len(Receiver.rec_messages), msgstr))

        num_wait = 0
        while len(Receiver.rec_messages) > 0 and num_wait<10:
            yield pu.asleep(0.2)
            num_wait += 1

        #if self.container:
        #    yield self.container.terminate()
        #elif ioninit.container_instance:
        #    yield ioninit.container_instance.terminate()


        # Reset static module values back to initial state for next test case
        bootstrap.reset_container()

        log.info("============ION container closed============")

        if msgstr:
            raise RuntimeError("Unexpected message processed during container shutdown: "+msgstr)

    @defer.inlineCallbacks
    def setUp(self, argv=None):
        fullPath = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        if argv is None and fullPath in sys.argv:
            argv = sys.argv[sys.argv.index(fullPath) + 1:]
        self.opts = opts = CCBrokerTestOptions()
        opts.parseOptions(argv)

        self.monitor_rate = opts['monitor']
        self.proc_count = int(opts['procs'])
        self.client_count = int(opts['clients'])

        self.cur_state['connects'] = 0
        self.cur_state['msgsend'] = 0
        self.cur_state['msgrecv'] = 0
        self.cur_state['errors'] = 0

        self.publishers, self.consumers = [], []

        yield self._start_container()

        self._enable_monitor(self.monitor_rate)

    def guid(self):
        return '%s:%s' % (self.load_id, pu.create_guid())

    @defer.inlineCallbacks
    def generate_load(self):
        serviceData = {'name': 'cc_broker_load', 'module': 'ion.test.loadtests.ccbrokerload', 'class': 'CCBrokerTestProcess'}
        services = [{'name': 'cc_broker_load_%d' % (i), 'module': 'ion.test.loadtests.ccbrokerload',
                     'class': 'CCBrokerTestProcess'} for i in range(self.proc_count)]

        sup = yield bootstrap.spawn_processes(services, self.sup)

        for name,receiver in sup.receivers.iteritems():
            x = name

        self.mc = MessageClient(proc=sup)
        self.btcs = [CCBrokerTestClient(proc=sup) for i in range(self.client_count)]

        while True:
            if self.is_shutdown():
                break

            yield self._send_messages(lorem_ipsum, 5)

    @defer.inlineCallbacks
    def _create_message(self, MessageContentTypeID, name='', **kwargs):
        """ Convenience wrapper to create a message with prepopulated attributes. """
        
        msg = yield self.mc.create_instance(MessageContentTypeID, name)
        for k,v in kwargs.iteritems():
            setattr(msg, k, v)
        defer.returnValue(msg)

    @defer.inlineCallbacks
    def _send_messages(self, body, count=1):
        """ Create and send messages in a batch. """
        
        name = 'simple message'
        msgCreates = [self._create_message(simple_type, name, id=pu.create_guid(), body=body) for i in range(count)]
        msgs = [d[1] for d in (yield defer.DeferredList(msgCreates))]
        msgSends = [btc.message_simple(msg) for msg in msgs for btc in [btc for btc in self.btcs]]
        yield defer.DeferredList(msgSends)

        self.cur_state['msgsend'] += len(msgSends)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        
        '''

        self._disable_monitor()
        self._call_monitor(False)
        self.summary()
        '''

    def monitor(self, output=True):
        interval = self._get_interval()
        rates = self._get_state_rate()

        if output:
            pieces = []
            if rates['errors']: pieces.append('had %.2f errors/sec' % rates['errors'])
            if rates['connects']: pieces.append('made %.2f connects/sec' % rates['connects'])
            if rates['msgsend']: pieces.append('sent %.2f msgs/sec' % rates['msgsend'])
            if rates['msgrecv']: pieces.append('received %.2f msgs/sec' % rates['msgrecv'])
            print '#%s] (%s) %s' % (self.load_id, time.strftime('%H:%M:%S'), ', '.join(pieces))

    def summary(self):
        state = self.cur_state
        secsElapsed = (state['_time'] - self.start_state['_time']) or 0.0001

        print '\n'.join([
              '-'*80
            , '#%s Summary' % (self.load_id)
            , 'Test ran for %.2f seconds, with a total of %d sent messages and %d received messages.' % (
                  secsElapsed, state['msgsend'], state['msgrecv'])
            , 'The average messages/second was %.2f sent and %.2f received.' % (
                state['msgsend']/secsElapsed, state['msgrecv']/secsElapsed)
            , '-'*80
        ])


"""
python -m ion.test.load_runner -s -c ion.test.loadtests.ccbrokerload.CCBrokerTest -
"""
