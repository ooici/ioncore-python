#!/usr/bin/env python

"""
@file ion/services/coi/service_registry.py
@author Michael Meisinger
@brief service for registering service (types and instances).
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import inspect

from ion.core import base_process
import ion.util.procutils as pu
from ion.core.base_process import ProcessFactory
from ion.services.base_service import BaseService, BaseServiceClient


from ion.data.datastore import registry
from ion.data import dataobject
from ion.data import store

from ion.resources import coi_resource_descriptions


from ion.core import ioninit
CONF = ioninit.config(__name__)


class ServiceRegistryService(registry.BaseRegistryService):
    """
    Service registry service interface
    @todo a service is a resource and should also be living in the resource registry
    """
    # Declaration of service
    declare = BaseService.service_declare(name='service_registry', version='0.1.0', dependencies=[])

    op_clear_registry = registry.BaseRegistryService.base_clear_registry

    op_register_service_definition = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register a service definition with the registry.
    """
    op_get_service_definition = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a service definition.
    """

    op_register_service_instance = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register a service instance with the registry.
    """
    op_get_service_instance = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a service instance.
    """

    op_set_service_lcstate = registry.BaseRegistryService.base_set_resource_lcstate
    """
    Service operation: Set a service life cycle state
    """

    op_find_registered_service_definition_from_service = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the definition of a service
    """
    op_find_registered_service_definition_from_description = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find service definitions which meet a description
    """

    op_find_registered_service_instance_from_service = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the registered instance that matches the service instance
    """
    op_find_registered_service_instance_from_description = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find all the registered service instances which match a description
    """
# Spawn of the process using the module name
factory = ProcessFactory(ServiceRegistryService)


class ServiceRegistryClient(registry.BaseRegistryClient):
    """
    Client class for accessing the service registry. This is most important for
    finding and accessing any other services. This client knows how to find the
    service registry - what does that mean, don't all clients have targetname
    assigned?
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "service_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    def clear_registry(self):
        return self.base_clear_registry('clear_registry')

    def register_container_services(self):
        """
        This method is called when the container is started to inspect the
        services of lca arch and register descriptions for each service.
        """

    @defer.inlineCallbacks
    def register_service_definition(self,service):
        """
        Client method to register the Definition of a Service Class
        """
        if isinstance(service, coi_resource_descriptions.ServiceDescription):
            service_description = service
            assert service_description.RegistryIdentity, 'Service Description must have a registry Identity'

        else:
            service_class = service
            # Build a new description of the service
            service_description = self.describe_service(service_class)

            found_sd = yield self.find_registered_service_definition_from_description(service_description)

            if found_sd:
                assert len(found_sd) == 1
                defer.returnValue(found_sd[0])
            else:
                service_description.create_new_reference()

        service_description = yield self.base_register_resource('register_service_definition', service_description)
        defer.returnValue(service_description)


    def describe_service(self,service_class):
        if type(service_class) is str:
            service_class = pu.get_class(service_class)

        assert issubclass(service_class, BaseService)

        # Do not make a new resource idenity - this is a generic method which
        # is also used to look for an existing description
        service_description = coi_resource_descriptions.ServiceDescription()

        service_description.name = service_class.declare['name']
        service_description.version = service_class.declare['version']

        service_description.class_name = service_class.__name__
        service_description.module = service_class.__module__

        service_description.description = inspect.getdoc(service_class)

        #@note need to improve inspection of service!
        #kind and name are accessors added in python 2.6
        #they are taken out here, to be 2.5 compatible
        for attr in inspect.classify_class_attrs(service_class):
            #if attr.kind == 'method' and 'op_' in attr.name :
            if attr[1] == 'method' and 'op_' in attr[0] :

                opdesc = coi_resource_descriptions.ServiceMethodInterface()
                #opdesc.name = attr.name
                opdesc.name = attr[0]
                #print 'INSEPCT',inspect.getdoc(attr.object)
                #opdesc.description = inspect.getdoc(attr.object)
                #Can't seem to get the arguments in any meaningful way...
                #opdesc.arguments = inspect.getargspec(attr.object)

                service_description.interface.append(opdesc)
        return service_description

    def get_service_definition(self, service_description_reference):
        """
        Get a service definition
        """
        return self.base_get_resource('get_service_definition', service_description_reference)

    @defer.inlineCallbacks
    def register_service_instance(self,service):
        """
        Client method to register a Service Instance
        """
        if isinstance(service, coi_resource_descriptions.ServiceInstance):
            service_resource = service
            assert resource_description.RegistryIdentity, 'Service Resource must have a registry Identity'
        else:
            service_instance = service
            # Build a new description of this service instance
            service_resource = yield self.describe_instance(service_instance)

            found_sir = yield self.find_registered_service_instance_from_description(service_resource)
            if found_sir:
                assert len(found_sir) == 1
                defer.returnValue(found_sir[0])
            else:
                service_resource.create_new_reference()
                service_resource.set_lifecyclestate(dataobject.LCStates.developed)

        service_resource = yield self.base_register_resource('register_service_instance',service_resource)
        defer.returnValue(service_resource)

    @defer.inlineCallbacks
    def describe_instance(self,service_instance):
        """
        @param service_instance is actually a ProcessDesc object!
        """

        # Do not make a new resource idenity - this is a generic method which
        # is also used to look for an existing description
        service_resource = coi_resource_descriptions.ServiceInstance()

        service_class = service_instance.proc_class

        sd = yield self.register_service_definition(service_class)
        service_resource.description = sd.reference(head=True)


        if service_instance.proc_node:
            service_resource.proc_node = service_instance.proc_node
        service_resource.proc_id = service_instance.proc_id
        service_resource.proc_name = service_instance.proc_name
        if service_instance.spawn_args:
            service_resource.spawn_args = service_instance.spawn_args
        service_resource.proc_state = service_instance._get_state()

        # add a reference to the supervisor - can't base process does not have the same fields as ProcessDesc
        #if service_resource.sup_process:
        #    print service_instance.sup_process.__dict__
        #    sr = yield self.register_service_instance(service_instance.sup_process)
        #    service_resource.sup_process = sr.reference(head=True)

        # Not sure what to do with name?
        service_resource.name=service_instance.proc_module

        defer.returnValue(service_resource)

    def get_service_instance(self, service_reference):
        """
        Get a service instance
        """
        return self.base_get_resource('get_service_instance',service_reference)

    def set_service_lcstate(self, service_reference, lcstate):
        return self.base_set_service_lcstate('set_service_lcstate',service_reference, lcstate)

    def set_service_lcstate_new(self, service_reference):
        return self.set_service_lcstate(service_reference, dataobject.LCStates.new)

    def set_service_lcstate_active(self, service_reference):
        return self.set_service_lcstate(service_reference, dataobject.LCStates.active)

    def set_service_lcstate_inactive(self, service_reference):
        return self.set_service_lcstate(service_reference, dataobject.LCStates.inactive)

    def set_service_lcstate_decomm(self, service_reference):
        return self.set_service_lcstate(service_reference, dataobject.LCStates.decomm)

    def set_service_lcstate_retired(self, service_reference):
        return self.set_service_lcstate(service_reference, dataobject.LCStates.retired)

    def set_service_lcstate_developed(self, service_reference):
        return self.set_service_lcstate(service_reference, dataobject.LCStates.developed)

    def set_service_lcstate_commissioned(self, service_reference):
        return self.set_service_lcstate(service_reference, dataobject.LCStates.commissioned)


    @defer.inlineCallbacks
    def find_registered_service_definition_from_service(self, service_class):
        """
        Find the definition of a service
        """
        service_description = self.describe_service(service_class)

        alist = yield self.base_find_resource('find_registered_service_definition_from_service', service_description,regex=False,ignore_defaults=True)
        # Find returns a list but only one service should match!
        if alist:
            assert len(alist) == 1
            defer.returnValue(alist[0])
        else:
            defer.returnValue(None)

    def find_registered_service_definition_from_description(self, service_description,regex=True,ignore_defaults=True):
        """
        Find service definitions which meet a description
        """
        return self.base_find_resource('find_registered_service_definition_from_description', service_description,regex,ignore_defaults)


    @defer.inlineCallbacks
    def find_registered_service_instance_from_service(self, service_instance):
        """
        Find service instances
        """
        service_resource = yield self.describe_instance(service_instance)
        alist = yield self.base_find_resource('find_registered_service_instance_from_service', service_resource, regex=False,ignore_defaults=True)
        # Find returns a list but only one service should match!
        if alist:
            assert len(alist) == 1
            defer.returnValue(alist[0])
        else:
            defer.returnValue(None)

    def find_registered_service_instance_from_description(self, service_instance_description,regex=True,ignore_defaults=True):
        """
        Find service instances which meet a description
        """
        return self.base_find_resource('find_registered_service_instance_from_description', service_instance_description,regex,ignore_defaults)
