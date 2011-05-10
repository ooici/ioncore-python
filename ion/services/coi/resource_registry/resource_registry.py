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


from ion.core.messaging import message_client
from ion.core.exception import ReceivedError, ApplicationError

from ion.core.process.process import ProcessFactory, Process
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.object import workbench

from ion.services.coi.datastore_bootstrap.ion_preload_config import ANONYMOUS_USER_ID, ROOT_USER_ID, OWNED_BY_ID


from ion.core.object import object_utils
RESOURCE_TYPE = object_utils.create_type_identifier(object_id=1102, version=1)
RESOURCE_DESCRIPTION_TYPE = object_utils.create_type_identifier(object_id=1101, version=1)

TYPEOBJECT_TYPE = object_utils.create_type_identifier(object_id=9, version=1)


from ion.core import ioninit
CONF = ioninit.config(__name__)


class ResourceRegistryError(ApplicationError):
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
    declare = ServiceProcess.service_declare(name='resource_registry', version='0.1.0', dependencies=[])


    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.

        #assert isinstance(backend, store.IStore)
        #self.backend = backend
        ServiceProcess.__init__(self, *args, **kwargs)

        self.push = self.workbench.push
        self.pull = self.workbench.pull
        self.fetch_blobs = self.workbench.fetch_blobs
        self.op_fetch_blobs = self.workbench.op_fetch_blobs

        self.datastore_service = self.spawn_args.get('datastore_service', CONF.getValue('datastore_service', default='datastore'))

        self.owned_by = None

        log.info('ResourceRegistryService.__init__()')

    @defer.inlineCallbacks
    def op_register_resource_instance(self, request, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        The interceptor will unpack a resource description object. The registry
        will create a new resource of the described type and return the
        identifier for it to the process that requested it.
        """


        if not isinstance(request, message_client.MessageInstance) or request.MessageType != RESOURCE_DESCRIPTION_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise ResourceRegistryError('Expected message type RESOURCE_DESCRIPTION_TYPE, received %s'
                                     % type(request))#, request.ResponseCodes.BAD_REQUEST)


        response = yield self._register_resource_instance(request, headers)


        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def pull_owned_by(self):

        yield self.pull(self.datastore_service, OWNED_BY_ID)
        self.owned_by = self.workbench.get_repository(OWNED_BY_ID)
        self.owned_by.checkout('master')


    @defer.inlineCallbacks
    def _register_resource_instance(self, resource_description, headers):


        # only pull it once - predicates should not change!
        if self.owned_by is None:
            yield self.pull_owned_by()

        # Get the user to associate with this new resource
        user_id = headers.get('user-id', 'ANONYMOUS')
        if user_id ==  'ANONYMOUS':
            user_id = ANONYMOUS_USER_ID

        # Always get the latest
        try:
            yield self.pull(self.datastore_service, user_id)
            user = self.workbench.get_repository(user_id)
        except workbench.WorkBenchError, we:

            log.debug('Caught Workbench Error:'+str(we))
            raise ResourceRegistryError('User ID does not exist. Can not create resource!', resource_description.ResponseCodes.BAD_REQUEST)

        # Make sure we are associating to the latest state of the user
        user.checkout('master')


        # Create a new repository to hold this resource
        resource_repository = self.workbench.create_repository(RESOURCE_TYPE)
        resource = resource_repository.root_object

        # Set the identity of the resource
        resource.identity = resource_repository.repository_key

        # Create the new resource object
        try:
            res_obj = resource_repository.create_object(resource_description.object_type)
        except object_utils.ObjectUtilException, ex:
            raise ResourceRegistryError(ex, resource_description.ResponseCodes.NOT_FOUND)
        # Set the object as the child of the resource
        resource.SetLinkByName('resource_object', res_obj)

        # Name and Description is set by the resource client
        resource.name = resource_description.name
        resource.description = resource_description.description

        # Set the object type
        object_utils.set_type_from_obj(res_obj, resource.object_type)

        # Set the resource type
        resource.resource_type = resource_description.resource_type


        # State is set to new by default
        resource.lcs = resource.LifeCycleState.NEW

        resource_repository.commit('Created a new resource!')

        # Add the association to the user

        ownership_association = self.workbench.create_association(resource_repository, self.owned_by, user)


        # push the new resource to the data store
        yield self.push(self.datastore_service, resource_repository)
        # If the push fails hand back the workbench error

        # Create the response object...
        response = yield self.message_client.create_instance(MessageContentTypeID=None)

        response.MessageResponseCode = response.ResponseCodes.OK
        response.MessageResponseBody = resource.identity

        defer.returnValue(response)



    #@defer.inlineCallbacks
    def op_lookup_resource_instance(self,content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    def op_register_RESOURCE_TYPE(self,content, headers, msg):
        """
        Service operation: Create or update a resource definition with the registry.
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    def op_lookup_RESOURCE_TYPE(self,content, headers, msg):
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
            kwargs['targetname'] = "resource_registry"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def register_resource_instance(self,RESOURCE_TYPE):
        """
        @brief Client method to Register a Resource Instance
        This method is used to generate a new resource instance of type
        Resource Type
        @param RESOURCE_TYPE
        """
        yield self._check_init()

        content, headers, msg = yield self.rpc_send('register_resource_instance', RESOURCE_TYPE)

        log.info('Resource Registry Service reply with new resource ID: '+str(content))
        defer.returnValue(content)



    #@defer.inlineCallbacks
    def lookup_resource_instance(self):
        """
        @brief Lookup an instance of a resource by the resource ID
        Forward a pull request to the datastore?
        """
        raise NotImplementedError, "Interface Method Not Implemented"

    #@defer.inlineCallbacks
    def register_RESOURCE_TYPE(self,resource):
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
    def find_registered_RESOURCE_TYPE(self, query):
        """
        @brief find all registered resources which match the attributes of description
        @param query object
        """
        raise NotImplementedError, "Interface Method Not Implemented"


# Spawn of the process using the module name
factory = ProcessFactory(ResourceRegistryService)
