#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry/resource_registry.py
@author Michael Meisinger
@author David Stuebe
@brief service for registering resources
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect

from ion.services.coi import datastore

from ion.core.object import gpb_wrapper

from net.ooici.resource import resource_pb2
from net.ooici.core.type import type_pb2

from ion.core.process.process import ProcessFactory, Process
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.procutils as pu

from ion.core import ioninit
CONF = ioninit.config(__name__)


class ResourceRegistryService(ServiceProcess):
    """
    Resource registry service interface
    The resource registry uses the underlieing push and pull ops of the datastore
    to fetch, retrieve and create resource objects.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='resource_registry_2', version='0.1.0', dependencies=[])

    TypeClassType = gpb_wrapper.set_type_from_obj(type_pb2.GPBType())

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        #assert isinstance(backend, store.IStore)
        #self.backend = backend
        ServiceProcess.__init__(self, *args, **kwargs)

        self.push = self.workbench.push
        self.pull = self.workbench.pull
        self.fetch_linked_objects = self.workbench.fetch_linked_objects
        self.op_fetch_linked_objects = self.workbench.op_fetch_linked_objects

        log.info('ResourceRegistryService.__init__()')

    @defer.inlineCallbacks
    def _register_resource_instance(self, content):
        
        # Get the repository that the object is in
        msg_repo = content.repository
            
        # Get the class for this type of resource
        cls = msg_repo._load_class_from_type(content)
        
        # Create a new repository to hold this resource
        new_repo, resource = self.workbench.init_repository(rootclass=resource_pb2.OOIResource)
        
        # Set the identity of the resource
        resource.identity = new_repo.repository_key
            
        # Create the new resources object
        res_obj = new_repo.create_wrapped_object(cls)
        # Set the object as the child of the resource
        resource.set_link_by_name('resource', res_obj)
        
        resource.name = res_obj.GPBMessage.DESCRIPTOR.file.name # get name?
        
        # State is set to new by default
        resource.lcs = resource_pb2.New
        
        new_repo.commit('Created a new resource!')

        # push the new resource to the data store        
        response, exception = yield self.push('datastore', resource.identity)
        
        assert response == self.ION_SUCCESS, 'Push to datastore failed!'
            
        defer.returnValue(resource.identity)


    @defer.inlineCallbacks
    def op_register_resource_instance(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        The interceptor will unpack a type object. The registry will create a
        new resource of this type and return the identifier for it to the process
        that requested it.
        
        """
        
        # Check that we got the correct kind of content!
        assert isinstance(content, gpb_wrapper.Wrapper)
        assert content.GPBType == TypeClassType
    
        id = yield _register_resource_instance(content)

        yield self.reply(id)

    



    #@defer.inlineCallbacks
    def op_lookup_resource_instance(self,content, headers, msg):
        """
        Service operation: Get a resource instance.
        """




    def op_register_resource_type(self,content, headers, msg):
        """
        Service operation: Create or update a resource definition with the registry.
        """

    def op_lookup_resource_type(self,content, headers, msg):
        """
        Service operation: Get a resource definition.
        """

    def op_find_registered_resource(self,content, headers, msg):
        """
        Service operation: Find the registered definition of a resource
        """

    def op_find_registered_resource_instance(self,content, headers, msg):
        """
        Service operation: Find the registered instances that matches the service class
        """
    
    
    
class ResourceRegistryClient(ServiceClient):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "resource_registry_2"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def register_resource_instance(self,resource_type):
        """
        @brief Client method to Register a Resource Instance
        This method is used to generate a new resource instance of type
        Resource Type
        @param resource_type
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('register_resource_instance', resource_type)
        log.info('Resource Registry Service reply: '+str(content))
        # Return value should be a resource identity
        defer.returnValue(str(content))
        
        

    #@defer.inlineCallbacks
    def lookup_resource_instance(self):
        """
        @brief Lookup an instance of a resource by the resource ID
        Forward a pull request to the datastore?
        """

    #@defer.inlineCallbacks
    def register_resource_type(self,resource):
        """
        @brief Client method to register the definition of a Resource Type
        @param resource can be either an instance of a Resource Description or
        the class object of the resource to be described.
        """

    #@defer.inlineCallbacks
    def find_registered_resource_instance(self, query):
        """
        @brief find the registered definition of a resoruce
        @param query is a query object
        """

    #@defer.inlineCallbacks
    def find_registered_resource_type(self, query):
        """
        @brief find all registered resources which match the attributes of description
        @param query object
        """


# Spawn of the process using the module name
factory = ProcessFactory(ResourceRegistryService)

