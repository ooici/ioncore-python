#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry_beta/resource_client.py
@author David Stuebe
@brief Resource Client and and Resource Instance classes are used to manage
resource objects in services and processes. They provide a simple interface to
create, get, put and update resources.

@ TODO
Add methods to access the state of updates which are merging...
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
from ion.core.object import repository
from ion.core.object.repository import RepositoryError

from ion.services.coi.resource_registry_beta.resource_registry import ResourceRegistryClient


from google.protobuf import message
from google.protobuf.internal import containers
from ion.core.object import gpb_wrapper
from ion.core.object import object_utils


resource_description_type = object_utils.create_type_identifier(object_id=1101, version=1)
resource_type = object_utils.create_type_identifier(object_id=1102, version=1)
idref_Type = object_utils.create_type_identifier(object_id=4, version=1)

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
        description_repository = self.workbench.create_repository(resource_description_type)
        
        resource_description = description_repository.root_object
        
        # Set the description
        resource_description.name = name
        resource_description.description = description
            
        # This is breaking some abstractions - using the GPB directly...
        resource_description.type.GPBMessage.CopyFrom(type_id)
            
        # Use the registry client to make a new resource        
        res_id = yield self.registry_client.register_resource_instance(resource_description)
            
        response, exception = yield self.workbench.pull(self.datastore_service, res_id)
        if not response == self.proc.ION_SUCCESS:
            log.warn(exception)
            raise ResourceClientError('Pull from datastore failed in resource client!')
            
        repo = self.workbench.get_repository(res_id)
        
        self.workbench.set_repository_nickname(res_id, name)
            
        yield repo.checkout('master')
        resource = ResourceInstance(repo)
        
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
        if hasattr(resource_id, 'ObjectType') and resource_id.ObjectType == idref_Type:
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
        yield repo.checkout(branch)
        
        # Create a resource instance to return
        resource = ResourceInstance(repo)
            
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
        repository = instance.Repository
            
        repository.commit(comment=comment)            
            
        response, exception = yield self.workbench.push(self.datastore_service, repository.repository_key)
        
        if not response == self.proc.ION_SUCCESS:
            raise ResourceClientError('Push to datastore failed during put_instance')
        


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
    Exception class for Resource Instance Object
    """
    
class ResourceFieldProperty(object):
    
    def __init__(self, name, doc=None):
        self.name = name
        if doc: self.__doc__ = doc
        
    def __get__(self, resource_instance, objtype=None):
        return getattr(resource_instance._repository.root_object.resource_object, self.name)
        
    def __set__(self, resource_instance, value):
        return setattr(resource_instance._repository.root_object.resource_object, self.name, value)
        
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Resource Instance property')
        
        
class ResourceEnumProperty(object):
    
    def __init__(self, name, doc=None):
        self.name = name
        if doc: self.__doc__ = doc
        
    def __get__(self, resource_instance, objtype=None):
        return getattr(resource_instance._repository.root_object.resource_object, self.name)
        
    def __set__(self, wrapper, value):
        raise AttributeError('Can not set a Resource Instance enum object')
        
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Resource Instance property')
    
class ResourceInstanceType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.
    
    This approach is generally applicable to wrap data structures. It is extremely powerful!
    """

    _type_cache = {}

    def __call__(cls, resource_repository, *args, **kwargs):
        # Cache the custom-built classes
        
        # Check that the object we are wrapping is a Google Message object
        if not isinstance(resource_repository, repository.Repository):
            raise ResourceInstanceError('ResourceInstance init argument must be an instance of a Repository')
        
        if resource_repository.status == repository.Repository.NOTINITIALIZED:
            raise ResourceInstanceError('ResourceInstance init Repository argument is in an invalid state - checkout first!')
        
        if resource_repository.root_object.ObjectType != resource_type:
            raise ResourceInstanceError('ResourceInstance init Repository is not a resource object!')
        
        resource_obj = resource_repository.root_object.resource_object
        
        msgType, clsType = type(resource_obj), None

        if msgType in ResourceInstanceType._type_cache:
            clsType = ResourceInstanceType._type_cache[msgType]
        else:
            
            
            # Get the class name
            clsName = '%s_%s' % (cls.__name__, msgType.__name__)
            clsDict = {}
                
            # Now setup the properties to map through to the GPB object
            resDict = msgType.__dict__
            
            for fieldName, resource_field in resDict.items():
                #print 'Key: %s; Type: %s' % (fieldName, type(resource_field))
                if isinstance(resource_field, gpb_wrapper.WrappedProperty):
                    prop = ResourceFieldProperty(fieldName )
                    
                    clsDict[fieldName] = prop
                    
                elif isinstance(resource_field, gpb_wrapper.EnumObject):
                    prop = ResourceEnumProperty(fieldName )
                    
                    clsDict[fieldName] = prop
            

            clsType = ResourceInstanceType.__new__(ResourceInstanceType, clsName, (cls,), clsDict)

            ResourceInstanceType._type_cache[msgType] = clsType

        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(ResourceInstanceType, clsType).__call__(resource_repository, *args, **kwargs)
        return obj
    
    
    
class ResourceInstance(object):
    """
    @brief The resoure instance is the vehicle through which a process
    interacts with a resource instance. It hides the git semantics of the data
    store and deals with resource specific properties.
    """
    __metaclass__ = ResourceInstanceType
    
    # Life Cycle States
    NEW='New'
    ACTIVE='Active'
    INACTIVE='Inactive'
    COMMISSIONED='Commissioned'
    DECOMMISSIONED='Decommissioned'
    RETIRED='Retired'
    DEVELOPED='Developed'
    UPDATE = 'Update'
    
    # Resource update mode
    APPEND = 'Appending new update'
    CLOBBER = 'Clobber current state with this update'
    MERGE = 'Merge modifications in this update'
    
    # Resource update Resolutions
    RESOLVED = 'Update resolved' # When a merger occurs with the previous state
    REJECTED = 'Update rejected' # When an update is rejected
    
    def __init__(self, resource_repository):
        """
        Resource Instance objects are created by the resource client
        """
        object.__setattr__(self,'_repository',None)
        
        self._repository = resource_repository
        
        
    @property
    def Repository(self):
        return self._repository
        
    @property
    def Resource(self):
        repo = self._repository
        return repo._workspace_root
        
    
    def _get_resource_object(self):
        repo = self._repository
        return repo._workspace_root.resource_object
        
    def _set_resource_object(self, value):
        repo = self._repository
        if value.ObjectType != self.ResourceType:
            raise ResourceInstanceError('Can not change the type of a resource object!')
        repo._workspace_root.resource_object = value
        
    ResourceObject = property(_get_resource_object, _set_resource_object)
        
        
    @property
    def CompareToUpdates(self):
        """
        @ Brief This methods provides access to the committed resource states that
        are bing merged into the current version of the Resource.
        @param ind is the index into list of merged states.
        @result The result is a list of update objects which are in a read only state.
        """
        
        updates = self.Repository.merge_objects
        if len(updates)==0:
            log.warn('Invalid index into MergingResource. Current number of merged states is: %d' % (len(self.Repository.merge_objects)))
            raise ResourceInstanceError('Invalid index access to Merging Resources. Either there is no merge or you picked an invalid index.')
        
        objects=[]
        for resource in updates:
            objects.append(resource.resource_object)
        
        return objects
        
    def __str__(self):
        output  = '============== Resource ==============\n'
        output += str(self.Resource) + '\n'
        output += '============== Object ==============\n'
        output += str(self.ResourceObject) + '\n'
        output += '============ End Resource ============\n'
        return output
        
    def VersionResource(self):
        """
        @brief Create a new version of this resource - creates a new branch in
        the objects repository. This is purely local until the next push!
        @retval the key for the new version
        """
        
        branch_key = self.Repository.branch()            
        return branch_key

    @defer.inlineCallbacks
    def MergeResourceUpdate(self, mode, *args):
        """
        Use this method when updating an existing resource.
        This is the recommended pattern for updating a resource. The Resource history will include a special
        Branch pattern showing the previous state, the update and the updated state...
        Once an update is commited, the update must be resolved before the instance
        can be put (pushed) to the public datastore.
        
        <Updated State>  
        |    \          \
        |    <Update1>  <Update2> ...
        |    /          /
        <Previous State>
        
        """
        if not self.Repository.status == self.Repository.UPTODATE:
            raise ResourceInstanceError('Can not merge while the resource is in a modified state')
        
        merge_branches = []
        for update in args:
        
            if update.ObjectType != self.ResourceType:
                log.debug ('Resource Type does not match update Type')
                log.debug ('Update type %s; Resource type %s' % (str(update.ObjectType), str(self.ResourceType)))
                raise ResourceInstanceError('update_instance argument "update" must be of the same type as the resource')
            
            current_branchname = self.Repository._current_branch.branchkey
            
            # Create and switch to a new branch
            merge_branches.append(self.Repository.branch())
        
            # Set the LCS in the resource branch to UPDATE and the object to the update
            self.ResourceLifeCycleState = self.UPDATE
        
            # Copy the update object into resource as the current state object.
            self.Resource.resource_object = update
        
            self.Repository.commit(comment=str(mode))
            
            yield self.Repository.checkout(branchname=current_branchname)
        
        # Set up the merge in the repository
        for b_name in merge_branches:
            yield self.Repository.merge(branchname=b_name)
        
            # Remove the merge branch - it is only a local concern
            self.Repository.remove_branch(b_name)
        # on the next commit - when put_instance is called - the merge will be complete!
        
        
        
    
    
    def CreateObject(self, type_id):
        """
        @brief CreateObject is used to make new locally create objects which can
        be added to the resource's data structure.
        @param type_id is the type_id of the object to be created
        @retval the new object which can now be attached to the resource
        """
        return self.Repository.create_object(type_id)
        
        
    
    def ListSetFields(self):
        """
        Return a list of the names of the fields which have been set.
        """
        return self.ResourceObject.ListSetFields()

    def HasField(self, field):
        log.warn('HasField is depricated because the name is confusing. Use IsFieldSet')
        return self.IsFieldSet(field)
        
    def IsFieldSet(self, field):
        return self.ResourceObject.IsFieldSet(field)
        
    def ClearField(self, field):
        return self.ResourceObject.ClearField(field)
        
    @property
    def ResourceIdentity(self):
        """
        @brief Return the resource identity as a string
        """
        return str(self.Resource.identity)
    
    @property
    def ResourceType(self):
        """
        @brief Returns the resource type - A type identifier object - not the wrapped object.
        """
        return self.Resource.type.GPBMessage
    
    def _set_life_cycle_state(self, state):
        """
        @brief Set the Life Cycel State of the resource
        @param state is a resource life cycle state class variable defined in
        the ResourceInstance class.
        """
        # Using IS for comparison - I think this is better than the usual ==
        # Want to force the use of the self.XXXX as the argument!
                
        if state == self.NEW:        
            self.Resource.lcs = self.Resource.LifeCycleState.NEW
        elif state == self.ACTIVE:
            self.Resource.lcs = self.Resource.LifeCycleState.ACTIVE
        elif state == self.INACTIVE:
            self.Resource.lcs = self.Resource.LifeCycleState.INACTIVE
        elif state == self.COMMISSIONED:
            self.Resource.lcs = self.Resource.LifeCycleState.COMMISSIONED
        elif state == self.DECOMMISSIONED:
            self.Resource.lcs = self.Resource.LifeCycleState.DECOMMISSIONED
        elif state == self.RETIRED:
            self.Resource.lcs = self.Resource.LifeCycleState.RETIRED
        elif state == self.DEVELOPED:
            self.Resource.lcs = self.Resource.LifeCycleState.DEVELOPED
        elif state == self.UPDATE:
            self.Resource.lcs = self.Resource.LifeCycleState.UPDATE
        else:
            raise Exception('''Invalid argument value state: %s. State must be 
                one of the class variables defined in Resource Instance''' % str(state))
        
    def _get_life_cycle_state(self):
        """
        @brief Get the life cycle state of the resource
        """
        state = None
        if self.Resource.lcs == self.Resource.LifeCycleState.NEW:
            state = self.NEW    
        
        elif self.Resource.lcs == self.Resource.LifeCycleState.ACTIVE:
            state = self.ACTIVE
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.INACTIVE:
            state = self.INACTIVE
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.COMMISSIONED:
            state = self.COMMISSIONED
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.DECOMMISSIONED:
            state = self.DECOMMISSIONED
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.RETIRED:
            state = self.RETIRED
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.DEVELOPED:
            state = self.DEVELOPED
        
        elif self.Resource.lcs == self.Resource.LifeCycleState.UPDATE:
            state = self.UPDATE
        
        return state
        
    ResourceLifeCycleState = property(_get_life_cycle_state, _set_life_cycle_state)
    """
    @var ResourceLifeCycleState is a getter setter property for the life cycle state of the resource
    """
        
    def _set_resource_name(self, name):
        """
        Set the name of the resource object
        """
        self.Resource.name = name
        
    def _get_resource_name(self):
        """
        """
        return str(self.Resource.name)
    
    ResourceName = property(_get_resource_name, _set_resource_name)
    """
    @var ResourceName is a getter setter property for the name of the resource
    """
    
    def _set_resource_description(self, description):
        """
        """
        self.Resource.description = description
        
    def _get_resource_description(self):
        """
        """
        return str(self.Resource.description)
        
    
    ResourceDescription = property(_get_resource_description, _set_resource_description)
    """
    @var ResourceDescription is a getter setter property for the description of the resource
    """
