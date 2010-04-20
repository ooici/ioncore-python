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

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class BaseService(object):
    """
    This is the abstract superclass for all service processes.
    
    A service process is a Capability Container process that can be spawned
    anywhere in the network and that provides a service.
    """
    
    receiver = None
    svcMessages = {}


    def op_init(self, content, headers, msg):
        logging.info('BaseService.op_init: '+str(content))
        self.slc_start()
        
    def slc_start(self):
        pass

    def slc_stop(self):
        pass

    def slc_config(self):
        pass
    
    def _add_messages(self):
        none
        
    def _add_conv_type(self):
        none

    def op_noop_catch(self, content, headers, msg):
        """The method called if operation is not defined
        """
        logging.info('Catch message')

class BaseServiceClient(object):
    """This is the abstract base class for service client libraries.
    """

class BaseServiceImplementation(object):
    """This is the abstract base class for all service provider implementations
    of a service provider interface.
    """