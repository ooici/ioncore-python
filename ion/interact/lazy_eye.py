#!/usr/bin/env python

"""
@file ion/interact/lazy_eye.py
@author Paul Hubbard
@date 4/25/11
@brief LazyEye is a RESTful interface on top of ion.interact.int_observer; a way to command
and control the generation and viewing of message sequence charts.
@note "RESTful observer = lazy eye" - get it? Sure ya do.
"""

from os import path

from twisted.internet import defer, reactor
from twisted.internet import protocol

import ion.util.ionlog
from ion.core.process.process import ProcessFactory, ProcessClient
from ion.interact.int_observer import InteractionObserver

# Globals
log = ion.util.ionlog.getLogger(__name__)

# @todo move this into ion.config
BINARY_NAME = '/Users/hubbard/bin/mscgen'

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

    def __init__(self, *args, **kwargs):
        InteractionObserver.__init__(self, *args, **kwargs)
        self.running = False
        self.filename = None
        self.imagename = None

    # Stomp the plc_* hooks in the parent class; msg_receiver controlled in op_start/stop
    def plc_init(self):
        pass

    def plc_activate(self):
        pass

    def plc_terminate(self):
        pass

    #noinspection PyUnusedLocal
    @defer.inlineCallbacks
    def op_start(self, request, headers, msg):
        log.debug('Got a start request: %s' % request)

        if self.running:
            if request == self.filename:
                log.debug('Duplicate start message received, ignoring')
                return
            else:
                log.error('Start received with different filename! Ignoring.')
                return

        self.running = True
        self.filename = request

        # change 'msc.txt' to 'msc.png'
        base, ext = path.splitext(request)
        self.imagename = base +'.png'

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
            return

        log.debug('Stopping receiver')
        yield self.msg_receiver.deactivate()
        yield self.msg_receiver.terminate()

        log.debug('writing datafile %s' % self.filename)
        f = open(self.filename, 'w')
        f.write(self.writeout_msc())

        self.mpp = MscProcessProtocol(self._mscgen_callback, msg)
        log.debug('Spawing mscgen to render the graph...')
        # @bug spawnProcess drops the first element in the tuple, so pad with blank
        args = ['', '-T', 'png', '-i', self.filename, '-o', self.imagename]
        log.debug(args)
        yield reactor.spawnProcess(self.mpp, BINARY_NAME, args)
        log.debug('%s started' % BINARY_NAME)

    #noinspection PyUnusedLocal
    def op_get_image_name(self, request, headers, msg):
        log.debug('image name requested, returning %s' % self.imagename)
        self.reply_ok(msg, self.imagename)

    def _mscgen_callback(self, msg, reply_text):
        """
        Send reply to caller when mscgen is finished. Callback hook.
        """
        log.debug('mscgen callback fired, %s' % reply_text)
        self.reply_ok(msg, reply_text)

class LazyEyeClient(ProcessClient):
    """
    Minimal process client, start/stop/query. Does not use GPB messages!
    """
    @defer.inlineCallbacks
    def start(self, filename='msc.txt'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('start', filename)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def stop(self):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('stop', '')
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_image_name(self):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_image_name', '')
        defer.returnValue(content)


# Spawn off the process using the module name
factory = ProcessFactory(LazyEye)
