#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry_beta/resource_client.py
@author David Stuebe
@brief Resource Client and and Resource Instance classes are used to manage
resource objects in services and processes. They provide a simple interface to
create, get, put and update resources.

@ TODO
"""

from twisted.internet import defer, reactor
from twisted.python import failure
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ReceivedError
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject
from ion.core.messaging.ion_reply_codes import ResponseCodes
from ion.core.process import process
from ion.core.object import workbench
from ion.core.object.repository import RepositoryError

from ion.services.coi.resource_registry_beta.resource_registry import ResourceRegistryClient

from net.ooici.core.type import type_pb2
from net.ooici.services.coi import resource_framework_pb2
from net.ooici.core.link import link_pb2


from google.protobuf import message
from google.protobuf.internal import containers
from ion.core.object import gpb_wrapper
from ion.core.object import object_utils


CONF = ioninit.config(__name__)


class ResourceClientError(Exception):
    """
    A class for resource client exceptions
    """

class ResourceClient(object):
    """
    @brief This is the base class for a resource client. It is a factory for resource
    instances. The resource instance provides the interface for working with resources.
    The client helps create and manage resource instances.
    """
    
    IDRefType = object_utils.set_type_from_obj(link_pb2.IDRef)
    
    def __init__(self, proc=None, datastore_service='datastore'):
        """
        Initializes a process client
        @param proc a IProcess instance as originator of messages
        @param datastore the name of the datastore service with which you wish to
        interact with the OOICI.
        """
        if not proc:
            proc = process.Process()
        
        if not hasattr(proc, 'op_fetch_linked_objects'):
            setattr(proc, 'op_fetch_linked_objects', proc.workbench.op_fetch_linked_objects)
                        
        self.proc = proc
        
        self.datastore_service = datastore_service
                
        # The resource client is backed by a process workbench.
        self.workbench = self.proc.workbench        
        
        # What about the name of the index services to use?
        
        self.registry_client = ResourceRegistryClient(proc=self.proc)
        

    @defer.inlineCallbacks
    def _check_init(self):
        """
        Called in client methods to ensure that there exists a spawned process
        to send and receive messages
        """
        if not self.proc.is_spawned():
            yield self.proc.spawn()
        
        assert isinstance(self.workbench, workbench.WorkBench), \
        'Process workbench is not initialized'

    
    @defer.inlineCallbacks
    def create_instance(self, type_id, name, description=''):
        """
        @brief Ask the resource registry to create the instance!
        @param type_id is a type identifier object
        @param name is a string, a name for the new resource
        @param description is a string describing the resource
        @retval resource is a ResourceInstance object
        """
        yield self._check_init()
        
        # Create a sendable resource object
        description_repository, resource_description = self.workbench.init_repository(rootclass=resource_framework_pb2.ResourceDescription)
        
        # Set the description
        resource_description.name = name
        resource_description.description = description
            
        resource_description.type.GPBMessage.CopyFrom(type_id)
            
        # Use the registry client to make a new resource        
        res_id = yield self.registry_client.register_resource_instance(resource_description)
            
        response, exception = yield self.workbench.pull(self.datastore_service, res_id)
        if not response == self.proc.ION_SUCCESS:
            log.warn(exception)
            raise ResourceClientError('Pull from datastore failed in resource client!')
            
        repo = self.workbench.get_repository(res_id)
        
        self.workbench.set_repository_nickname(res_id, name)
            
        resource = ResourceInstance(repo.checkout('master'))
        
        defer.returnValue(resource)
        
        
    @defer.inlineCallbacks
    def get_instance(self, resource_id):
        """
        @brief Get the latest version of the identified resource from the data store
        @param resource_id can be either a string resource identity or an IDRef
        object which specifies the resource identity as well as optional parameters
        version and version state.
        @retval the specified ResourceInstance 
        """
        yield self._check_init()
        
        reference = None
        branch = 'master'
        commit = None
        
        # Get the type of the argument and act accordingly
        if hasattr(resource_id, 'GPBType') and resource_id.GPBType == self.IDRefType:
            # If it is a resource reference, unpack it.
            if resource_id.branch:
                branch = resource_id.branch
                
            reference = resource_id.key
            commit = resource_id.commit
            
        elif isinstance(resource_id, (str, unicode)):
            # if it is a string, us it as an identity
            reference = resource_id
            # @TODO Some reasonable test to make sure it is valid?
            
        else:
            raise ResourceClientError('''Illegal argument type in retrieve_resource_instance:
                                      \n type: %s \nvalue: %s''' % (type(resource_id), str(resource_id)))    
            
        # Pull the repository
        response, exception = yield self.workbench.pull(self.datastore_service, reference)
        if not response == self.proc.ION_SUCCESS:
            log.warn(exception)
            raise ResourceClientError('Pull from datastore failed in resource client!')
            
        # Get the repository
        repo = self.workbench.get_repository(reference)
        
        # Create a resource instance to return
        resource = ResourceInstance(repo.checkout(branch))
            
        self.workbench.set_repository_nickname(reference, resource.ResourceName)
        # Is this a good use of the resource name? Is it safe?
            
        defer.returnValue(resource)
        
    @defer.inlineCallbacks
    def put_instance(self, instance, comment=None):
        """
        @breif Write the current state of the resource to the data store
        @param instance is a ResourceInstance object to be written
        @param comment is a comment to add about the current state of the resource
        """
        if not comment:
            comment = 'Resource client default commit message'
            
        # Get the repository
        repository = instance._repository
            
        repository.commit(comment=comment)            
            
        response, exception = yield self.workbench.push(self.datastore_service, repository.repository_key)
        
        if not response == self.proc.ION_SUCCESS:
            raise ResourceInstanceError('Push to datastore failed during put_instance')
        


    @defer.inlineCallbacks
    def find_instance(self, **kwargs):
        """
        Use the index to find resource instances that match a set of constraints
        For R1 the constraints that may be used are very limited
        """
        yield self._check_init()
            
        raise NotImplementedError, "Interface Method Not Implemented"
        
    def reference_instance(self, instance, current_state=False):
        """
        @brief Reference Resource creates a data object which can be used as a
        message or part of a message or added to another data object or resource.
        @param instance is a ResourceInstance object
        @param current_state is a boolen argument which determines whether you
        intend to reference exactly the current state of the resource.
        @retval an Identity Reference object to the resource
        """
        
        return self.workbench.reference_repository(instance.ResourceIdentity, current_state)

    
    
    
class ResourceInstanceError(Exception):
    """
    Exceptoin class for Resource Instance Object
    """
    
class ResourceInstance(object):
    """
    @brief The resoure instance is the vehicle through which a process
    interacts with a resource instance. It hides the git semantics of the data
    store and deals with resource specific properties.
    """
    
    NEW='New'
    ACTIVE='Active'
    INACTIVE='Inactive'
    COMMISSIONED='Commissioned'
    DECOMMISSIONED='Decommissioned'
    RETIRED='Retired'
    DEVELOPED='Developed'
    
    def __init__(self, resource):
        """
        Resource Instance objects are created by the resource client
        """
        object.__setattr__(self,'_object',None)
        
        self._repository = resource.Repository
        
        self._resource = resource
        
        self._object = self._resource.resource_object
        
    def __str__(self):
        output  = '============== Resource ==============\n'
        output += str(self._resource) + '\n'
        output += '============== Object ==============\n'
        output += str(self._object) + '\n'
        output += '============ End Resource ============\n'
        return output
        
    def VersionResource(self):
        """
        @brief Create a new version of this resource - creates a new branch in
        the objects repository. This is purely local until the next push!
        @retval the key for the new version
        """
        
        branch_key = self._repository.branch()            
        return branch_key
    
    def CreateObject(self, type_id):
        """
        @brief CreateObject is used to make new locally create objects which can
        be added to the resource's data structure.
        @param type_id is the type_id of the object to be created
        @retval the new object which can now be attached to the resource
        """
        
        cls = self._repository._load_class_from_type(type_id)
        obj = self._repository.create_wrapped_object(cls)
        return obj
        
        
    def __getattribute__(self, key):
        """
        @brief We want to expose the resource and its object through a uniform
        interface. To do so we override getattr to expose the data fields of the
        resource object
        """
        # Because we have over-riden the default getattribute we must be extremely
        # careful about how we use it!
        res_obj = object.__getattribute__(self, '_object')
        
        gpbfields = []
        if res_obj:
            #gpbfields = object.__getattribute__(res_obj,'_gpbFields')
            gpbfields = res_obj._gpbFields
        if key in gpbfields:
            # If it is a Field defined by the gpb...
            #value = getattr(res_obj, key)
            value = res_obj.__getattribute__(key)
                
        else:
            # If it is a attribute of this class, use the base class's getattr
            value = object.__getattribute__(self, key)
        return value
        
        
    def __setattr__(self,key,value):
        """
        @brief We want to expose the resource and its object through a uniform
        interface. To do so we override getattr to expose the data fields of the
        resource object
        """
        res_obj = object.__getattribute__(self, '_object')
        
        gpbfields = []
        if res_obj:
            #gpbfields = object.__getattribute__(res_obj,'_gpbFields')
            gpbfields = res_obj._gpbFields
        
        if key in gpbfields:
            # If it is a Field defined by the gpb...
            #setattr(res_obj, key, value)
            res_obj.__setattr__(key,value)
                
        else:
            v = object.__setattr__(self, key, value)
        
        
    @property
    def ResourceIdentity(self):
        """
        @brief Return the resource identity as a string
        """
        return self._resource.identity
    
    @property
    def ResourceType(self):
        """
        @brief Returns the resource type, at present this is an object, but will
        be changed to a resource identifier in the near future.
        """
        return self._resource.type
    
    def _set_life_cycle_state(self, state):
        """
        @brief Set the Life Cycel State of the resource
        @param state is a resource life cycle state class variable defined in
        the ResourceInstance class.
        """
        # Using IS for comparison - I think this is better than the usual ==
        # Want to force the use of the self.XXXX as the argument!
        if state == self.NEW:        
            self._resource.lcs = resource_framework_pb2.New
        elif state == self.ACTIVE:
            self._resource.lcs = resource_framework_pb2.Active
        elif state == self.INACTIVE:
            self._resource.lcs = resource_framework_pb2.Inactive
        elif state == self.COMMISSIONED:
            self._resource.lcs = resource_framework_pb2.Commissioned
        elif state == self.DECOMMISSIONED:
            self.resource.lcs = resource_framework_pb2.Decommissioned
        elif state == self.RETIRED:
            self.resource.lcs = resource_framework_pb2.Retired
        elif state == self.DEVELOPED:
            self.resource.lcs = resource_framework_pb2.Developed
        else:
            raise Exception('''Invalid argument value state: %s. State must be 
                one of the class variables defined in Resource Instance''' % str(state))
        
    def _get_life_cycle_state(self):
        """
        @brief Get the life cycle state of the resource
        """
        state = None
        if self._resource.lcs == resource_framework_pb2.New:
            state = self.NEW    
        
        elif self._resource.lcs == resource_framework_pb2.Active:
            state = self.ACTIVE
            
        elif self._resource.lcs == resource_framework_pb2.Inactive:
            state = self.INACTIVE
            
        elif self._resource.lcs == resource_framework_pb2.Commissioned:
            state = self.COMMISSIONED
            
        elif self._resource.lcs == resource_framework_pb2.Decommissioned:
            state = self.DECOMMISSIONED
            
        elif self._resource.lcs == resource_framework_pb2.Retired:
            state = self.RETIRED
            
        elif self._resource.lcs == resource_framework_pb2.Developed:
            state = self.DEVELOPED
        
        return state
        
    ResourceLifeCycleState = property(_get_life_cycle_state, _set_life_cycle_state)
    """
    @var ResourceLifeCycleState is a getter setter property for the life cycle state of the resource
    """
        
    def _set_resource_name(self, name):
        """
        Set the name of the resource object
        """
        self._resource.name = name
        
    def _get_resource_name(self):
        """
        """
        return self._resource.name
    
    ResourceName = property(_get_resource_name, _set_resource_name)
    """
    @var ResourceName is a getter setter property for the name of the resource
    """
    
    def _set_resource_description(self, description):
        """
        """
        self._resource.description = description
        
    def _get_resource_description(self):
        """
        """
        return self._resource.description 
        
    
    ResourceDescription = property(_get_resource_description, _set_resource_description)
    """
    @var ResourceDescription is a getter setter property for the description of the resource
    """
