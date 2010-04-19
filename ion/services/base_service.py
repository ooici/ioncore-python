#!/usr/bin/env python

"""
@file ion/services/base_service.py
@author Michael Meisinger
@package ion.services  abstract base classes for all service interfaces, implementations and provider.
"""

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

class BaseService(object):
    """
    This is the abstract superclass for all service processes.
    
    A service process is a Capability Container process that can be spawned
    anywhere in the network and that provides a service.
    """
    
    svcReceiver = None
    svcMessages = {}
    
    def __init__(self, rec):
        self.svcReceiver = rec
    
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

class ServiceClient(object):
    """This is the abstract base class for service client libraries.
    """

class ServiceInterface(object):
    """This is the abstract base class for all service provider messaging interface.
    """

class ServiceImplementation(object):
    """This is the abstract base class for all service provider implementations
    of a service provider interface.
    """