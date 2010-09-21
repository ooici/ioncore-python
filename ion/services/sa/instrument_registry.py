#!/usr/bin/env python

"""
@file ion/services/sa/instrument_registry.py
@author Michael Meisinger
@brief service for registering instruments and platforms
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from ion.core.cc.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.data.datastore import registry

from ion.resources import sa_resource_descriptions

class InstrumentRegistryService(registry.BaseRegistryService):
    """
    Service that provides a registry for instrument devices, types etc.
    Based on the BaseRegistryService.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='instrument_registry',
                                          version='0.1.0',
                                          dependencies=[])


    op_clear_instrument_registry = registry.BaseRegistryService.base_clear_registry
    """
    Service operation: Clears all records from the instrument registry.
    """

    op_register_instrument_instance = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update an instrument instance with the registry.
    """

    op_register_instrument_type = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update an instrument type with the registry.
    """

    op_get_instrument_instance = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a instrument instance.
    """

    op_get_instrument_by_id = registry.BaseRegistryService.base_get_resource_by_id

    op_get_instrument_type = registry.BaseRegistryService.base_get_resource #changed
    """
    Service operation: Get a instrument type.
    """

    op_find_instrument_instance = registry.BaseRegistryService.base_find_resource
    """
    Service operation: find instrument instances by characteristics
    """

    op_find_instrument_type = registry.BaseRegistryService.base_find_resource
    """
    Service operation: find instrument types by characteristics
    """


class InstrumentRegistryClient(registry.BaseRegistryClient, registry.LCStateMixin):
    """
    Class for the client accessing the instrument registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)


    def clear_registry(self):
        return self.base_clear_registry('clear_instrument_registry')

    def register_instrument_instance(self, instrument_instance):
        """
        Client method to Register a Resource instance
        """
        return self.base_register_resource('register_instrument_instance', instrument_instance)

    def register_instrument_type(self,instrument_type):
        """
        Client method to register the Definition of a Resource Type
        """
        return self.base_register_resource('register_instrument_type', instrument_type)

    def get_instrument_type(self,instrument_type_reference):
        """
        Get a instrument type
        """
        return self.base_get_resource('get_instrument_type', instrument_type_reference)

    def get_instrument_instance(self,instrument_instance_reference):
        """
        Get a resource instance
        """
        return self.base_get_resource('get_instrument_instance',resource_reference)

    def get_instrument_by_id(self, id):
        return self.base_get_resource_by_id('get_instrument_by_id', id)
        
    #def set_resource_lcstate(self, resource_reference, lcstate):
    #    return self.base_set_resource_lcstate(resource_reference, lcstate, 'set_instrument_lcstate')

    def find_instrument_type(self, description, regex=True, ignore_defaults=True):
        return self.base_find_resource('find_instrument_type', description, regex, ignore_defaults)

    def find_instrument_instance(self, description, regex=True, ignore_defaults=True):
        return self.base_find_resource('find_instrument_instance', description, regex, ignore_defaults)


# Spawn of the process using the module name
factory = ProtocolFactory(InstrumentRegistryService)

