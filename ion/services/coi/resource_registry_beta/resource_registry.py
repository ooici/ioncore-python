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

from ion.core.process.process import ProcessFactory, Process
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.procutils as pu

from ion.core.object import object_utils
resource_type = object_utils.create_type_identifier(object_id=1102, version=1)
resource_description_type = object_utils.create_type_identifier(object_id=1101, version=1)

from ion.core import ioninit
CONF = ioninit.config(__name__)


class ResourceRegistryError(Exception):
    """
    An exception class for errors in the resource registry
    """

class ResourceRegistryService(ServiceProcess):
    """
    Resource registry service interface
    The resource registry uses the underlieing push and pull ops of the datastore
    to fetch, retrieve and create resource objects.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='resource_registry_2', version='0.1.0', dependencies=[])

    typeobject_type = object_utils.create_type_identifier(object_id=9, version=1)
    resource_type = object_utils.create_type_identifier(object_id=1102, version=1)
    resource_description_type = object_utils.create_type_identifier(object_id=1101, version=1)

    RESOURCE_CLASS = object_utils.get_gpb_class_from_type_id(resource_type)


    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        #assert isinstance(backend, store.IStore)
        #self.backend = backend
        ServiceProcess.__init__(self, *args, **kwargs)
        
        self.push = self.workbench.push
        self.pull = self.workbench.pull
        self.fetch_linked_objects = self.workbench.fetch_linked_objects
        self.op_fetch_linked_objects = self.workbench.op_fetch_linked_objects
        
        self.datastore_service = self.spawn_args.get('datastore_service', CONF.getValue('datastore_service', default='No Data Store service name provided!'))
        
        log.info('ResourceRegistryService.__init__()')
        
    @defer.inlineCallbacks
    def op_register_resource_instance(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        The interceptor will unpack a resource description object. The registry
        will create a new resource of the described type and return the
        identifier for it to the process that requested it.
        """
        
        # Check that we got the correct kind of content!
        assert isinstance(content, gpb_wrapper.Wrapper)
        assert content.GPBType == self.resource_description_type
        
        try:
            id = yield self._register_resource_instance(content)
        except object_utils.ObjectUtilException, ex:
            yield self.reply(msg, response_code=self.APP_FAILED, exception=ex)
            return
        
        yield self.reply(msg, content=id)
        
    @defer.inlineCallbacks
    def _register_resource_instance(self, resource_description):
        
        # Get the repository that the object is in
        msg_repo = resource_description.Repository
            
        
        # Create a new repository to hold this resource
        resource_repository, resource = self.workbench.init_repository(resource_type)
        
        # Set the identity of the resource
        resource.identity = resource_repository.repository_key
            
        # Create the new resource object
        res_obj = resource_repository.create_object(resource_description.type)
        # Set the object as the child of the resource
        resource.SetLinkByName('resource_object', res_obj)
            
        # Name and Description is set by the resource client
        resource.name = resource_description.name
        resource.description = resource_description.description
        
        object_utils.set_type_from_obj(res_obj, resource.type)
        
        # State is set to new by default
        resource.lcs = self.RESOURCE_CLASS.New
        
        resource_repository.commit('Created a new resource!')

        # push the new resource to the data store        
        response, exception = yield self.push(self.datastore_service, resource.identity)
        assert response == self.ION_SUCCESS, 'Push to datastore failed!'
            
        defer.returnValue(resource.identity)



    #@defer.inlineCallbacks
    def op_lookup_resource_instance(self,content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    def op_register_resource_type(self,content, headers, msg):
        """
        Service operation: Create or update a resource definition with the registry.
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    def op_lookup_resource_type(self,content, headers, msg):
        """
        Service operation: Get a resource definition.
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    def op_find_registered_resource(self,content, headers, msg):
        """
        Service operation: Find the registered definition of a resource
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    def op_find_registered_resource_instance(self,content, headers, msg):
        """
        Service operation: Find the registered instances that matches the service class
        """
        raise NotImplementedError, "Interface Method Not Implemented"
    
    
    
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
        
        log.info('Resource Registry Service reply with new resource ID: '+str(content))
        response = headers.get(self.MSG_RESPONSE)
        exception = headers.get(self.MSG_EXCEPTION)
        
        if response == self.ION_SUCCESS:
            defer.returnValue(str(content))
        else:
            log.debug('Exception in Resource Registry: %s' % exception)
            raise ResourceRegistryError('Error during Resource Registry service call: register_resource_instance')
        

    #@defer.inlineCallbacks
    def lookup_resource_instance(self):
        """
        @brief Lookup an instance of a resource by the resource ID
        Forward a pull request to the datastore?
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    #@defer.inlineCallbacks
    def register_resource_type(self,resource):
        """
        @brief Client method to register the definition of a Resource Type
        @param resource can be either an instance of a Resource Description or
        the class object of the resource to be described.
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    #@defer.inlineCallbacks
    def find_registered_resource_instance(self, query):
        """
        @brief find the registered definition of a resoruce
        @param query is a query object
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    #@defer.inlineCallbacks
    def find_registered_resource_type(self, query):
        """
        @brief find all registered resources which match the attributes of description
        @param query object
        """
        raise NotImplementedError, "Interface Method Not Implemented"


# Spawn of the process using the module name
factory = ProcessFactory(ResourceRegistryService)

