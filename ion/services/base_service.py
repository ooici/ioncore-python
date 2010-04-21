#!/usr/bin/env python

"""
@file ion/services/base_service.py
@author Michael Meisinger
@package ion.services  abstract base classes for all service interfaces, implementations and provider.
"""

import logging

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import ion.util.procutils as pu
from ion.core.base_process import BaseProcess

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class BaseService(BaseProcess):
    """
    This is the abstract superclass for all service processes.
    
    A service process is a Capability Container process that can be spawned
    anywhere in the network and that provides a service.
    """
    def __init__(self, receiver=None):
        """Constructor using a given name for the spawnable receiver.
        """
        BaseProcess.__init__(self, receiver)
     
    def op_init(self, content, headers, msg):
        """Init operation, on receive of the init message
        """
        logging.info('BaseService.op_init: '+str(content))
        if hasattr(self,'_isInitialized'):
            self.slc_init()
            self._isInitialized = True
        
    def slc_init(self):
        """Service life cycle event: on initialization of process (once)
        """
        logging.info('BaseService.slc_start()')
        pass

    @classmethod
    def _add_messages(cls):
        none
        
    @classmethod
    def _add_conv_type(cls):
        none

class BaseServiceClient(object):
    """This is the abstract base class for service client libraries.
    """

class RpcClient(BaseServiceClient):
    """Service client providing a RPC methaphor
    """
    
    def __init__(self):
        self.clientRecv = Receiver(__name__)
        self.clientRecv.handle(self.receive)
        self.deferred = None
    
    @defer.inlineCallbacks
    def spawnClient(self):
        self.id = yield spawn(self.clientRecv)

    def rpc_send(self, to, op, cont='', headers={}):
        """
        @return a deferred with the message value
        """
        pu.send_message(self.clientRecv, self.id, to, op, cont, headers)
        self.deferred = defer.Deferred()
        return self.deferred

    def receive(self, content, msg):
        logging.info('RpcClient.receive(), calling callback in defer')
        self.deferred.callback(content)

class BaseServiceImplementation(object):
    """This is the abstract base class for all service provider implementations
    of a service provider interface.
    """