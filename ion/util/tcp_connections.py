#!/usr/bin/env python

"""
@file ion/core/managedconnections/tcp.py
@author David Stuebe
@author Matt Rodriguez
@brief base classes for objects with a tcp connection using a life cycle
"""

from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import failure

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.util.state_object import BasicLifecycleObject



class TCPConnection(BasicLifecycleObject):
    
    def __init__(self, host, port, factory, timeout=30, bindAddress=None):
        BasicLifecycleObject.__init__(self)
        self._host = host
        self._port = port
        self._factory = factory
        self._timeout = timeout
        self._bindAddress=bindAddress
        
        self._connector = None

    def on_initialize(self, *args, **kwargs):
        log.info('on_initialize')

    def on_activate(self, *args, **kwargs):
        self._connector = reactor.connectTCP(self._host, self._port, self._factory, self._timeout, self._bindAddress)
        log.info('on_activate: connected TCP')

    def on_deactivate(self, *args, **kwargs):
        self._connector.disconnect()
        log.info('on_deactivate: disconnected TCP')

    def on_terminate(self, *args, **kwargs):
        log.info('on_terminate: disconnected TCP')
        self._connector.disconnect()
        log.info('on_terminate: disconnected TCP')

    def on_error(self, *args, **kwargs):
        self._connector.disconnect()
        log.info('on_error')

class TCPListen(BasicLifecycleObject):
    
    def __init__(self, port, factory, backlog=50, interface=''):
        BasicLifecycleObject.__init__(self)
        self._port = port
        self._factory = factory
        self._backlog = backlog
        self._interface=interface
        
        self._listener = None

    def on_initialize(self, *args, **kwargs):
        log.info('on_initialize')

    def on_activate(self, *args, **kwargs):
        self._listener = reactor.listenTCP(self._port, self._factory, self._backlog, self._interface)
        log.info('on_activate: Listening TCP')

    def on_deactivate(self, *args, **kwargs):
        self._listener.stopListening()
        log.info('on_deactivate: disconnected TCP')
        
    def on_terminate(self, *args, **kwargs):
        self._listener.stopListening()
        log.info('on_terminate: disconnected TCP')

    def on_error(self, *args, **kwargs):
        log.info('on_error')

