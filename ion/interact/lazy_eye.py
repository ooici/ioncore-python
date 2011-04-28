#!/usr/bin/env python

"""
@file ion/interact/lazy_eye.py
@author Paul Hubbard
@date 4/25/11
@brief LazyEye is a RESTful interface on top of ion.interact.int_observer; a way to command
and control the generation and viewing of message sequence charts.
@note "RESTful observer = lazy eye" - get it? Sure ya do.
"""

import os
import time


from twisted.internet import defer, reactor
from twisted.internet import protocol

from ion.core import ioninit
import ion.util.ionlog
from ion.core.process.process import ProcessFactory, ProcessClient
from ion.core.process.service_process import ServiceProcess
from ion.core.messaging.receiver import FanoutReceiver
from ion.interact.int_observer import InteractionObserver

import simplejson as json

# Globals
log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)
BINARY_NAME = CONF['mscgen']

class MscProcessProtocol(protocol.ProcessProtocol):
    """
    Wrapper around the mscgen application, saves output
    """
    def __init__(self, callback, msg):
        self.output = []
        self.running = False
        self.cb = callback
        self.msg = msg

    def connectionMade(self):
        log.debug('mscgen started ok')
        self.running = True

    def processEnded(self, reason):
        pass
    
    def processExited(self, reason):
        log.debug('mscgen exited, "%s"' % reason.value)
        self.cb(self.msg, str(self.output))
        self.running = False

    def outReceived(self, data):
        """
        Called when mscgen prints to the screen
        """
        log.debug('got "%s"' % data)
        self.output.append(data)

class LazyEye(InteractionObserver):
    """
    @brief LazyEye is a RESTful interface on top of ion.interact.int_observer; a way to command
    and control the generation and viewing of message sequence charts.
    @note "RESTful observer = lazy eye" - get it? Sure ya do.
    """

    declare = ServiceProcess.service_declare(name='lazyeye', version='0.1', dependencies=[])
    
    def __init__(self, *args, **kwargs):
        InteractionObserver.__init__(self, *args, **kwargs)
        self.running = False
        self.filename = 'msc.txt'
        self.imagename = 'msc.png'
        self.binding_key = '#'
        self.last_graph_size = 0
        self.start_time = 0
        self.end_time = 0

        self.main_receiver = FanoutReceiver(
                name='lazyeye',
                label='lazyeye',
                process=self,
                handler=self.receive,
                error_handler=self.receive_error)
        self.add_receiver(self.main_receiver)
        

    def _reset_receiver(self):
        """
        Recreate the listener, erase the old message log.
        """
        # Save the last size before erasing old messages
        self.last_graph_size = len(self.msg_log)
        self.msg_log = []

        self.msg_receiver = FanoutReceiver(
                name=self.binding_key,
                label='lazyeye_listener',
                process=self,
                handler=self.msg_receive)
        self.add_receiver(self.msg_receiver)

    # Stomp the plc_* hooks in the parent class; msg_receiver controlled in op_start/stop
    @defer.inlineCallbacks
    def plc_init(self):
        # Start up the op_* listener
        yield self.main_receiver.initialize()

    @defer.inlineCallbacks
    def plc_activate(self):
        yield self.main_receiver.activate()

    def plc_terminate(self):
        pass

    def _mscgen_callback(self, msg, reply_text):
        """
        Send reply to caller when mscgen is finished. Callback hook.
        """
        log.debug('mscgen callback fired, %s' % reply_text)
        self.running = False
        self._reset_receiver()
        self.reply_ok(msg, reply_text)

    #noinspection PyUnusedLocal
    @defer.inlineCallbacks
    def op_start(self, request, headers, msg):

        binding_key = request
        self.start_time = time.time()

        # Strip off any path; don't want overwriting
        log.debug('Got a start request: binding key "%s"' % binding_key)

        # Remove any stale output file
        try:
            log.debug('Removing old image "%s"...' % self.imagename)
            os.remove(self.imagename)
        except OSError:
            pass

        if self.running:
            log.debug('Duplicate start message received, ignoring')
            return

        self.running = True
        if binding_key != self.binding_key:
            log.debug('Resetting receiver binding key')
            self.binding_key = binding_key
            self._reset_receiver()

        log.debug('Starting up the message receiver...')
        yield self.msg_receiver.initialize()
        yield self.msg_receiver.activate()

        log.debug('Started OK')
        self.reply_ok(msg)

    #noinspection PyUnusedLocal
    @defer.inlineCallbacks
    def op_stop(self, request, headers, msg):
        log.debug('Stop request received')

        if not self.running:
            log.error('Stop receieved but not started, ignoring')
            yield self.reply_ok(msg, 'Not started')
            return

        self.end_time = time.time()

        log.debug('Stopping receiver')
        yield self.msg_receiver.deactivate()
        yield self.msg_receiver.terminate()

        log.debug('writing datafile %s' % self.filename)
        f = open(self.filename, 'w')
        f.write(self.writeout_msc())

        self.mpp = MscProcessProtocol(self._mscgen_callback, msg)
        log.debug('Spawing mscgen to render the graph, %d edges or so...' % len(self.msg_log))
        # @bug spawnProcess drops the first element in the tuple, so pad with blank
        args = ['', '-T', 'png', '-i', self.filename, '-o', self.imagename]
        log.debug(args)
        yield reactor.spawnProcess(self.mpp, BINARY_NAME, args)
        log.debug('%s started' % BINARY_NAME)

    #noinspection PyUnusedLocal
    def op_get_results(self, request, headers, msg):
        """
        Return imagename, elapsed time, number of messages and rate as a
        single JSON dictionary.
        """
        log.debug('Returning results')
        delta_t = self.end_time - self.start_time
        payload = {'imagename': self.imagename,
                   'num_edges' : self.last_graph_size,
                   'elapsed_time' : delta_t,
                   'msg_rate' : self.last_graph_size / delta_t}
        self.reply_ok(msg, json.dumps(payload))

class LazyEyeClient(ProcessClient):
    """
    Minimal process client, start/stop/query. Does not use GPB messages!
    """
    @defer.inlineCallbacks
    def start(self, binding_key='#'):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('start', binding_key)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def stop(self):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('stop', '')
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_results(self):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_results', '')
        rc = json.loads(content)
        defer.returnValue(rc)

# Spawn off the process using the module name
factory = ProcessFactory(LazyEye)
