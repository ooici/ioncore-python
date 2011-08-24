#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry/resource_client.py
@author David Stuebe
@brief Resource Client and and Resource Instance classes are used to manage
resource objects in services and processes. They provide a simple interface to
create, get, put and update resources.

@ TODO
Add methods to access the state of updates which are merging...
"""
import re

from twisted.internet import defer
from ion.core.object.object_utils import sha1_to_hex

import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit

from ion.core.process import process
from ion.core.object import workbench
from ion.core.object import repository
from ion.core.object import association_manager

from ion.services.coi.resource_registry.resource_registry import ResourceRegistryClient
from ion.services.coi.datastore_bootstrap import ion_preload_config

from ion.services.coi.datastore_bootstrap.ion_preload_config import OWNED_BY_ID

from ion.core.exception import ApplicationError, ReceivedError

from google.protobuf import message
from google.protobuf.internal import containers
from ion.core.object import object_utils

import weakref

RESOURCE_DESCRIPTION_TYPE = object_utils.create_type_identifier(object_id=1101, version=1)
RESOURCE_TYPE = object_utils.create_type_identifier(object_id=1102, version=1)
IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)

CONF = ioninit.config(__name__)

class ResourceClientError(ApplicationError):
    """
    A class for resource client exceptions
    """


class ResourceClient(object):
    """
    @brief This is the base class for a resource client. It is a factory for resource
    instances. The resource instance provides the interface for working with resources.
    The client helps create and manage resource instances.
    """

    # The type_map is a map from object type to resource type built from the ion_preload_configs
    # this is a temporary device until the resource registry is fully architecturally operational.
    type_map = ion_preload_config.TypeMap()



    def __init__(self, proc=None, datastore_service='datastore'):
        """
        Initializes a process client
        @param proc a IProcess instance as originator of messages
        @param datastore the name of the datastore service with which you wish to
        interact with the OOICI.
        """
        if not proc:
            proc = process.Process()

        if not hasattr(proc, 'op_fetch_blobs'):
            setattr(proc, 'op_fetch_blobs', proc.workbench.op_fetch_blobs)

        self.proc = proc

        self.datastore_service = datastore_service

        # The resource client is backed by a process workbench.
        self.workbench = self.proc.workbench

        #self.asc = AssociationServiceClient(proc=proc)

        # What about the name of the index services to use?

        self.registry_client = ResourceRegistryClient(proc=self.proc)

        # Make a weak value dictionary to hold the resource instance - make sure there is only one wrapper for each repository
        self.myresources=weakref.WeakValueDictionary()


    @defer.inlineCallbacks
    def _check_init(self):
        """
        Called in client methods to ensure that there exists a spawned process
        to send and receive messages
        """
        if not self.proc.is_spawned():
            yield self.proc.spawn()

        assert isinstance(self.workbench, workbench.WorkBench),\
        'Process workbench is not initialized'


    @defer.inlineCallbacks
    def create_instance(self, type_id, ResourceName, ResourceDescription=''):
        """
        @brief Ask the resource registry to create the instance!
        @param type_id is a type identifier object
        @param name is a string, a name for the new resource
        @param description is a string describing the resource
        @retval resource is a ResourceInstance object

        @TODO Change the create method to take a Resource Type ID - change RR to look up object type!
        @TODO pull the associations that go with this resource

        """
        yield self._check_init()

        # Create a sendable resource object
        resource_description = yield self.proc.message_client.create_instance(RESOURCE_DESCRIPTION_TYPE)

        # Set the description
        resource_description.name = ResourceName
        resource_description.description = ResourceDescription

        # This is breaking some abstractions - using the GPB directly...
        resource_description.object_type.GPBMessage.CopyFrom(type_id)

        # Set the resource type - keep the object type above for now...
        res_type = resource_description.CreateObject(IDREF_TYPE)

        # Get the resource type if it exists - otherwise a default will be set!
        res_type.key = self.type_map.get(type_id.object_id)

        resource_description.resource_type = res_type

        # Use the registry client to make a new resource
        try:
            result = yield self.registry_client.register_resource_instance(resource_description)
            res_id = str(result.MessageResponseBody)
        except ReceivedError, ex:
            raise ResourceClientError('Pull from datastore failed in resource client! Requested Resource Type Not Found!\nInner Exception:\n%s' % str(ex))

        try:
            yield self.workbench.pull(self.datastore_service, res_id)
        except workbench.WorkBenchError, ex:
            raise ResourceClientError('Pull from datastore failed in resource client! Resource Not Found!\nInner Exception:\n%s' % str(ex))

        repo = self.workbench.get_repository(res_id)

        self.workbench.set_repository_nickname(res_id, ResourceName)

        yield repo.checkout('master')
        resource = ResourceInstance(repo)

        self.myresources[res_id] = resource

        defer.returnValue(resource)


    @defer.inlineCallbacks
    def get_instance(self, resource_id, excluded_types=None):
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
        treeish = None
        has_treeish = False

        # Get the type of the argument and act accordingly
        if hasattr(resource_id, 'ObjectType') and resource_id.ObjectType == IDREF_TYPE:
            # If it is a resource reference, unpack it.
            if resource_id.branch:
                branch = resource_id.branch

            reference = resource_id.key

            if resource_id.IsFieldSet('commit'):
                commit = resource_id.commit

            if resource_id.IsFieldSet('treeish'):
                treeish = resource_id.treeish

        elif isinstance(resource_id, (str, unicode)):
            # if it is a string, us it as an identity
            reference = resource_id

            rtreeish = re.compile('([~^].*)$')
            m = rtreeish.search(reference)
            if m:
                reference = reference[0:m.start()]
                treeish = m.groups()[0]
                has_treeish = True

            # @TODO Some reasonable test to make sure it is valid?

        else:
            raise ResourceClientError('''Illegal argument type in get_instance:
                                      \n type: %s \nvalue: %s''' % (type(resource_id), str(resource_id)))

            # Pull the repository
        try:
            result = yield self.workbench.pull(self.datastore_service, reference, get_head_content=not has_treeish, excluded_types=excluded_types)
        except workbench.WorkBenchError, ex:
            log.error('Resource client error during pull operation: Resource ID "%s" \nException - %s' % (reference, str(ex)))
            raise ResourceClientError(
                'Could not pull the requested resource from the datastore. Workbench exception: \n %s' % ex)

        # Get the repository
        repo = self.workbench.get_repository(reference)

        # do we have a treeish to resolve?
        if has_treeish and treeish is not None and len(treeish)>0:
            commitref = repo.resolve_treeish(treeish, branch)
            commit = commitref.MyId

        try:
            yield repo.checkout(branch, commit_id=commit, excluded_types=excluded_types)
        except repository.RepositoryError, ex:
            log.exception('Could not check out branch "%s":\n Current repo state:\n %s' % (branch, str(repo)))
            raise ResourceClientError('Could not checkout branch during get_instance.')

        # Create a resource instance to return
        # @TODO - Check and see if there is already one - what to do?

        if reference not in self.myresources:
            resource = ResourceInstance(repo)
            self.myresources[reference] = resource

        else:
            # Use the existing one - it is basically stateless...
            resource = self.myresources.get(reference)
            resource._repository = repo
            resource._merge = None


        self.workbench.set_repository_nickname(reference, resource.ResourceName)
        # Is this a good use of the resource name? Is it safe?

        # Get owner and ownership association:
        #owner_associations = yield self.get_associations(subject=resource, predicate_or_predicates=OWNED_BY_ID)

        defer.returnValue(resource)

    @defer.inlineCallbacks
    def put_instance(self, instance, comment=None):
        """
        @Brief Write the current state of the resource and any associations to the data store
        @param instance is a ResourceInstance object to be written
        @param comment is a comment to add about the current state of the resource

        @TODO push the associations that go with this resource
        """
        yield self._check_init()

        if not comment:
            comment = 'Resource client default commit message'

        # Get the repository
        repository = instance.Repository

        if repository.status == repository.MODIFIED:
            repository.commit(comment=comment)

        try:
            yield self.workbench.push(self.datastore_service, repository)
        except workbench.WorkBenchError, ex:
            raise ResourceClientError('Push to datastore failed during put_instance, inner exception:\n%s' % str(ex))

    @defer.inlineCallbacks
    def put_resource_transaction(self, instances=None, comment=None):
        """
        @Brief Write the current state of a list of resources to the data store
        @param instance is a ResourceInstance object. All associations and all associated objects will be pushed.
        @param comment is a comment to add about the current state of the resource

        @TODO push the associations that go with this resource
        """
        yield self._check_init()

        if comment is None or comment == '':
            comment = 'Resource client default commit message'

        if instances is None:
            raise ResourceClientError('Must pass at least one resource instance to put_resource_transaction')
        elif hasattr(instances, '__iter__'):
            instances = instances

            # Check to make sure they are valid resource instances
            for instance in instances:
                if not hasattr(instance, 'Repository'):
                    raise ResourceClientError(
                        'Invalid object in list of instances argument to put_resource_transaction. Must be an Instance, received: "%s"' % str(
                            instance))
        else:
            raise ResourceClientError(
                'Invalid argument to put_resource_transaction: instances must be a resource instance or a list of them')

        transaction_repos = []

        for instance in instances:
            repo = instance.Repository
            if repo.status != repo.UPTODATE:
                repo.commit(comment=comment)

            transaction_repos.append(repo)

        try:
            yield self.workbench.push(self.datastore_service, transaction_repos)
        except workbench.WorkBenchError, ex:
            raise ResourceClientError('Push to datastore failed during put_resource_transaction, inner exception:\n %s' % str(ex))

    @defer.inlineCallbacks
    def get_associated_resource_object(self, association):
        """
        @Brief Get the Resource Instance which is the object of the association
        @param association is an association instance
        """
        if not isinstance(association, association_manager.AssociationInstance):
            raise ResourceClientError(
                'Invalid argument to get_associated_resource_object: argument must be an association instance')

        obj_ref = association.ObjectReference

        # Get the latest of that resource
        resource_instance = yield self.get_instance(obj_ref.key)

        resource_instance.ResourceAssociationsAsObject.add(association)

        defer.returnValue(resource_instance)


    @defer.inlineCallbacks
    def get_associated_resource_subject(self, association):
        """
        @Brief Get the Resource Instance which is the object of the association
        @param association is an association instance
        """
        if not isinstance(association, association_manager.AssociationInstance):
            raise ResourceClientError(
                'Invalid argument to get_associated_resource_subject: argument must be an association instance')

        subject_ref = association.SubjectReference

        # Get the latest of that resource
        resource_instance = yield self.get_instance(subject_ref.key)

        resource_instance.ResourceAssociationsAsSubject.add(association)

        defer.returnValue(resource_instance)


    def reference_instance(self, instance, current_state=False):
        """
        @brief Reference Resource creates a data object which can be used as a
        message or part of a message or added to another data object or resource.
        @param instance is a ResourceInstance object
        @param current_state is a boolen argument which determines whether you
        intend to reference exactly the current state of the resource.
        @retval an Identity Reference object to the resource
        """

        # @TODO  yield self._check_init()
        return self.workbench.reference_repository(instance.ResourceIdentity, current_state)


class ResourceInstanceError(ApplicationError):
    """
    Exception class for Resource Instance Object
    """


class ResourceFieldProperty(object):
    def __init__(self, name, res_prop, doc=None):
        self.name = name
        if doc: self.__doc__ = doc
        self.field_type = res_prop.field_type
        self.field_enum = res_prop.field_enum

    def __get__(self, resource_instance, objtype=None):
        return getattr(resource_instance._repository.root_object.resource_object, self.name)

    def __set__(self, resource_instance, value):
        return setattr(resource_instance._repository.root_object.resource_object, self.name, value)

    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Resource Instance property')


class MergeResourceFieldProperty(object):
    def __init__(self, name, res_prop, doc=None):
        self.name = name
        if doc: self.__doc__ = doc
        self.field_type = res_prop.field_type
        self.field_enum = res_prop.field_enum

    def __get__(self, resource_instance, objtype=None):
        return getattr(resource_instance._merge_repository.root_object.resource_object, self.name)

    def __set__(self, resource_instance, value):
        raise AttributeError('Can not set a Merge Resource Instance property')

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


class MergeResourceInstanceType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.

    This approach is generally applicable to wrap data structures. It is extremely powerful!
    """

    _type_cache = {}

    def __call__(cls, merge_repository, *args, **kwargs):
        # Cache the custom-built classes

        # Check that the object we are wrapping is a Google Message object
        if not isinstance(merge_repository, repository.MergeRepository):
            raise ResourceInstanceError('MergeResourceInstance init argument must be an instance of a MergeRepository')

        if merge_repository.root_object.ObjectType != RESOURCE_TYPE:
            raise ResourceInstanceError('MergeResourceInstance init Repository is not a MergeRepository!')

        resource_obj = merge_repository.root_object.resource_object

        msgType, clsType = type(resource_obj), None

        if msgType in MergeResourceInstanceType._type_cache:
            clsType = MergeResourceInstanceType._type_cache[msgType]
        else:
            # Get the class name
            clsName = '%s_%s' % (cls.__name__, msgType.__name__)
            clsDict = {}

            for propName, msgProp in msgType._Properties.items():
                #print 'Key: %s; Type: %s' % (fieldName, type(message_field))
                prop = MergeResourceFieldProperty(propName, msgProp)
                clsDict[propName] = prop

            for enumName, enumProp in msgType._Enums.items():
                enum = ResourceEnumProperty(enumName)
                clsDict[enumName] = enum

            clsDict['_Properties'] = msgType._Properties
            clsDict['_Enums'] = msgType._Enums

            # Try rewriting using slots - would be more efficient...
            def obj_setter(self, k, v):
                # For the resource instance, there is only one class propety to set,
                # so just create it using object.__setattr__
                if not hasattr(self, k):
                    raise AttributeError(\
                        '''Cant add properties to the ION Resource Instance.\n'''
                        '''Unknown property name - "%s"; value - "%s"''' % (k, v))
                super(MergeResourceInstance, self).__setattr__(k, v)

            clsDict['__setattr__'] = obj_setter

            clsType = MergeResourceInstanceType.__new__(MergeResourceInstanceType, clsName, (cls,), clsDict)

            MergeResourceInstanceType._type_cache[msgType] = clsType

        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(MergeResourceInstanceType, clsType).__call__(merge_repository, *args, **kwargs)
        return obj


class MergeResourceInstance(object):
    """
    @brief The resource instance is the vehicle through which a process
    interacts with a resource instance. It hides the git semantics of the data
    store and deals with resource specific properties.

    @TODO how do we make sure that there is only one resource instance per resource? If the process creates multiple
    resource instances to wrap the same resource we are in trouble. Need to find a way to keep a cache of the instances
    """
    __metaclass__ = MergeResourceInstanceType

    def __init__(self, merge_repository):
        """
        Merge Resource Instance objects are created by the resource client
        """
        object.__setattr__(self, '_merge_repository', None)

        self._merge_repository = merge_repository

    @property
    def MergeRepository(self):
        return self._merge_repository

    @property
    def Resource(self):
        repo = self._merge_repository
        return repo.root_object

    @property
    def ResourceObject(self):
        repo = self._merge_repository
        return repo.root_object.resource_object

    def ListSetFields(self):
        """
        Return a list of the names of the fields which have been set.
        """
        return self.ResourceObject.ListSetFields()

    def IsFieldSet(self, field):
        return self.ResourceObject.IsFieldSet(field)

    @property
    def ResourceIdentity(self):
        """
        @brief Return the resource identity as a string
        """
        return str(self.Resource.identity)

    @property
    def ResourceObjectType(self):
        """
        @brief Returns the resource type - A type identifier object - not the wrapped object.
        """
        # Resource type should be a Resource Identifier - UUID defined in preload_config and stored in the Resource Registry

        return self.Resource.object_type.GPBMessage

    @property
    def ResourceTypeID(self):
        """
        @brief Returns the resource type identifier - the idref for the resource type instance.
        """
        # Resource type should be a Resource Identifier - UUID defined in preload_config and stored in the Resource Registry

        return self.Resource.resource_type

    @property
    def ResourceLifeCycleState(self):
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

    @property
    def ResourceName(self):
        return self.Resource.name

    @property
    def ResourceDescription(self):
        return self.Resource.description


class MergeResourceContainer(object):
    def __init__(self):
        self.parent = None

        self._merge_commits = {}
        self.merge_repos = []

    def _append(self, item):
        mri = MergeResourceInstance(item)

        self.merge_repos.append(mri)

        self._merge_commits[item.commit] = mri

    def __iter__(self):
        return self.merge_repos.__iter__()


    def __len__(self):
        return len(self.merge_repos)


    def __getitem__(self, index):
        return self.merge_repos[index]


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
            raise ResourceInstanceError(
                'ResourceInstance init Repository argument is in an invalid state - checkout first!')

        if resource_repository.root_object.ObjectType != RESOURCE_TYPE:
            raise ResourceInstanceError('ResourceInstance init Repository is not a resource object!')

        resource_obj = resource_repository.root_object.resource_object

        msgType, clsType = type(resource_obj), None

        if msgType in ResourceInstanceType._type_cache:
            clsType = ResourceInstanceType._type_cache[msgType]
        else:
            # Get the class name
            clsName = '%s_%s' % (cls.__name__, msgType.__name__)
            clsDict = {}

            for propName, msgProp in msgType._Properties.items():
                #print 'Key: %s; Type: %s' % (fieldName, type(message_field))
                prop = ResourceFieldProperty(propName, msgProp)
                clsDict[propName] = prop

            for enumName, enumProp in msgType._Enums.items():
                enum = ResourceEnumProperty(enumName)
                clsDict[enumName] = enum

            clsDict['_Properties'] = msgType._Properties
            clsDict['_Enums'] = msgType._Enums

            # Try rewriting using slots - would be more efficient...
            def obj_setter(self, k, v):
                # For the resource instance, there is only one class propety to set,
                # so just create it using object.__setattr__
                if not hasattr(self, k):
                    raise AttributeError(\
                        '''Cant add properties to the ION Resource Instance.\n'''
                        '''Unknown property name - "%s"; value - "%s"''' % (k, v))
                super(ResourceInstance, self).__setattr__(k, v)

            clsDict['__setattr__'] = obj_setter

            clsType = ResourceInstanceType.__new__(ResourceInstanceType, clsName, (cls,), clsDict)

            ResourceInstanceType._type_cache[msgType] = clsType

        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(ResourceInstanceType, clsType).__call__(resource_repository, *args, **kwargs)
        return obj


class ResourceInstance(object):
    """
    @brief The resource instance is the vehicle through which a process
    interacts with a resource instance. It hides the git semantics of the data
    store and deals with resource specific properties.

    @TODO how do we make sure that there is only one resource instance per resource? If the process creates multiple
    resource instances to wrap the same resource we are in trouble. Need to find a way to keep a cache of the instances
    """
    __metaclass__ = ResourceInstanceType

    predicate_map = ion_preload_config.PredicateMap()

    # Life Cycle States
    NEW = 'New'
    ACTIVE = 'Active'
    INACTIVE = 'Inactive'
    COMMISSIONED = 'Commissioned'
    DECOMMISSIONED = 'Decommissioned'
    RETIRED = 'Retired'
    DEVELOPED = 'Developed'
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
        object.__setattr__(self, '_repository', None)

        self._repository = resource_repository

        self.ResourceAssociationsAsObject.update_predicate_map(self.predicate_map)
        self.ResourceAssociationsAsSubject.update_predicate_map(self.predicate_map)

        object.__setattr__(self, '_merge', None)

    @property
    def Repository(self):
        return self._repository

    @property
    def ResourceAssociationsAsSubject(self):
        return self._repository.associations_as_subject

    @property
    def ResourceAssociationsAsObject(self):
        return self._repository.associations_as_object

    @property
    def Resource(self):
        repo = self._repository
        return repo.root_object


    def _get_resource_object(self):
        repo = self._repository
        return repo._workspace_root.resource_object

    def _set_resource_object(self, value):
        repo = self._repository
        if value.ObjectType != self.ResourceObjectType:
            raise ResourceInstanceError('Can not change the type of a resource object!')
        repo._workspace_root.resource_object = value

    ResourceObject = property(_get_resource_object, _set_resource_object)

    def __str__(self):
        output = '============== Resource ==============\n'
        output += 'Resource Repository State:\n'
        output += str(self.Repository) + '\n'
        output += '============== Object ==============\n'
        try:
            output += str(self.ResourceObject) + '\n'
        except AttributeError, ae:
            output += 'Resource Object in an invalid state!'
        output += '============ End Resource ============\n'
        return output

    @property
    def Merge(self):
        """
        @ Brief This method provides access to the committed resource states that
        are bing merged into the current version of the Resource.
        """

        # Do some synchronization with the repository:

        if self.Repository.merge is None:
            # Clear the Resource clients list of merge stuff
            self._merge = None
            return None

        if self._merge is None:
            self._merge = MergeResourceContainer()

        for commit, mr in self.Repository.merge._merge_commits.iteritems():
            if commit not in self._merge._merge_commits:
                self._merge._append(mr)

        return self._merge


    def VersionResource(self):
        """
        @brief Create a new version of this resource - creates a new branch in
        the objects repository. This is purely local until the next push!
        @retval the key for the new version
        """

        return self.Repository.branch()


    def CreateUpdateBranch(self, update=None):
        branch_key = self.Repository.branch()

        # Set the LCS in the resource branch to UPDATE and the object to the update
        self.ResourceLifeCycleState = self.UPDATE

        if update is not None:
            if update.ObjectType != self.ResourceObjectType:
                log.error('Resource Type does not match update Type!')
                log.error('Update type %s; Resource type %s' % (str(update.ObjectType), str(self.ResourceObjectType)))
                raise ResourceInstanceError(
                    'CreateUpdateBranch argument "update" must be of the same type as the resource to be updated!')

            # Copy the update object into resource as the current state object.
            self.Resource.resource_object = update

        return branch_key


    def CurrentBranchKey(self):
        return self.Repository.current_branch_key()

    @defer.inlineCallbacks
    def MergeWith(self, branchname, parent_branch=None):
        if parent_branch is not None:
            yield self.Repository.checkout(branchname=parent_branch)

        yield self.Repository.merge_with(branchname=branchname)


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
            if update.ObjectType != self.ResourceObjectType:
                log.debug('Resource Type does not match update Type')
                log.debug('Update type %s; Resource type %s' % (str(update.ObjectType), str(self.ResourceObjectType)))
                raise ResourceInstanceError(
                    'update_instance argument "update" must be of the same type as the resource')

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
            yield self.Repository.merge_with(branchname=b_name)

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
    def ResourceObjectType(self):
        """
        @brief Returns the resource type - A type identifier object - not the wrapped object.
        """
        # Resource type should be a Resource Identifier - UUID defined in preload_config and stored in the Resource Registry

        return self.Resource.object_type.GPBMessage

    @property
    def ResourceTypeID(self):
        """
        @brief Returns the resource type identifier - the idref for the resource type instance.
        """
        # Resource type should be a Resource Identifier - UUID defined in preload_config and stored in the Resource Registry

        return self.Resource.resource_type


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
