#!/usr/bin/env python


"""
@file ion/services/dm/preservation/preservation_registry.py
@author David Stuebe
@brief registry for preservation service resources
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect
from magnet.spawnable import Receiver

from ion.data import dataobject
from ion.data.datastore import registry
from ion.data import store

from ion.core import ioninit
from ion.core import base_process
from ion.core.base_process import ProtocolFactory, BaseProcess
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

from ion.resources import dm_resource_descriptions

CONF = ioninit.config(__name__)

class PreservationRegistryService(registry.BaseRegistryService):
    """
    @Brief Preservation registry service interface
    """
        
     # Declaration of service
    declare = BaseService.service_declare(name='preservation_registry', version='0.1.0', dependencies=[])

    op_define_archive = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update a archive resource.
    """
    op_get_archive = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get an archive resource
    """
    op_find_archive = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find an archive resource by characteristics
    """
    
# Spawn of the process using the module name
factory = ProtocolFactory(PreservationRegistryService)


class PreservationRegistryClient(registry.BaseRegistryClient):
    """
    Class for the client accessing the Preservation registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'preservation_registry'
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_registry')


    def define_archive(self,archive):
        """
        @Brief Client method to Register an archive
        
        @param archive is an instance of a Archive Resource
        """
        return  self.base_register_resource('define_archive', archive)    

    
    def get_archive(self,archive_reference):
        """
        @Brief Get a archive by reference
        @param archive_reference is the unique reference object for a registered
        archive
        """
        return self.base_get_resource('get_archive',archive_reference)
        
    def find_archive(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered archive which match the attributes of description
        @param see the registry docs for params
        """
        return self.base_find_resource('find_archive',description,regex,ignore_defaults,attnames)


