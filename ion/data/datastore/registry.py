"""
@file ion/data/datastore/registry.py
"""

import logging
logging = logging.getLogger(__name__)

from zope import interface

from twisted.internet import defer

from ion.data import dataobject
from ion.data.datastore import objstore

import uuid

def create_unique_identity():
    return str(uuid.uuid4())

LCStateNames = ['new',
                'active',
                'inactive',
                'decomm',
                'retired',
                'developed',
                'commissioned',
                ]



class LCState(object):

    def __init__(self, state):
        assert state in LCStateNames
        self._state = state

    def __repr__(self):
        return self._state

    def __eq__(self, other):
        assert isinstance(other, LCState)
        return str(self) == str(other)

LCStates = dict([('LCState', LCState)] + [(name, LCState(name)) for name in LCStateNames])

class states(dict):

    def __init__(self, d):
        dict.__init__(self, d)
        for k, v in d.items():
            setattr(self, k, v)

LCStates = states(LCStates)


class ResourceReference(dataobject.DataObject):
    _identity = dataobject.TypedAttribute(str,None)
    #@TODO Make the commit ref a list so that an object can be a merge
    _parent_commit = dataobject.TypedAttribute(str,None)
    _resource_type = dataobject.TypedAttribute(str,None)
    _branch = dataobject.TypedAttribute(str,'master')

    def __init__(self,branch=None,id=None,parent=None,type=None):
        if id:
            self._identity = id
        if parent:
            self._parent_commit = parent
        if type:
            self._resource_type = type
        if branch:
            self._branch = branch


    @classmethod
    def create_new_resource(cls):
        inst = cls()
        inst._identity = create_unique_identity()
        inst._resource_type = cls.__class__.__name__
        inst._branch = 'master'
        return inst
    
    def reference(self):
        inst = ResourceReference()
        if self._identity:
            inst._identity = self._identity
        if self._parent_commit:
            inst._parent_commit = self._parent_commit
        inst._resource_type = self._resource_type
        inst._branch = self._branch
        return inst
    """
    def get_identity(self):
        return self._identity
    
    def set_identity(self, id):
        self._identity = id
    
    def get_parent_commit(self):
        return self._parent_commit

    def set_parent_commit(self, cref):
        self._parent_comiit = cref
    """

class ResourceDescription(ResourceReference):
    """
    @brief Base for all OOI resource objects
    @note OOIResource or OOIRegistryObject or OOIObject???
    @note could build in explicit link back to ResourceRegistryClient so
    user can make changes through this object.
    """
    _types = LCStates
    _types['ResourceReference']=ResourceReference

    name = dataobject.TypedAttribute(str)
    lifecycle = dataobject.TypedAttribute(LCState, default=LCStates.new)

    def set_lifecyclestate(self, state):
        assert(isinstance(state, LCState))
        self.lifecycle = state

    def get_lifecyclestate(self):
        return self.lifecycle


class Generic(ResourceDescription):
    """
    """


class IResourceRegistry(interface.Interface):
    """
    @brief General API of any registry
    """

    def register(resource):
        """
        @brief Register resource description.
        @param uuid unique name of resource instance.
        @param resource instance of OOIResource.
        @note Does the resource instance define its own name/uuid?
        """

    def get_description(resource_reference):
        """
        @param uuid name of resource.
        """

    def set_resource_lcstate(resource_reference, state):
        """
        """

class ResourceRegistryClient(objstore.ObjectChassis):
    """
    """
    objectClass = ResourceDescription

class ResourceRegistry(objstore.ObjectStore):
    """
    """

    objectChassis = ResourceRegistryClient

    @defer.inlineCallbacks
    def register(self, resource_description):
        """
        @brief Add a new resource description to the registry. Implemented
        by creating a new (unique) resource object to the store.
        @note Is the way objectClass is referenced awkward?
        """
        assert isinstance(resource_description, self.objectChassis.objectClass)
        
        id = resource_description._identity
        if not id:
            raise RuntimeError('Can not register a resource which does not have an identity.')
        
        try:
            res_client = yield self.create(id, self.objectChassis.objectClass)
        except objstore.ObjectStoreError:
            res_client = yield self.clone(id)
            
        yield res_client.checkout()
        res_client.index = resource_description
        resource_description._parent_commit = yield res_client.commit()
        #print 'pcommit',resource_description._parent_commit
        defer.returnValue(resource_description)

    @defer.inlineCallbacks
    def get_description(self, resource_reference):
        """
        @brief Get resource description object
        """
        branch = resource_reference._branch
        assert isinstance(resource_reference, ResourceReference)
        resource_client = yield self.clone(resource_reference._identity)
        if resource_client:
            if not resource_reference._parent_commit:
                resource_reference._parent_commit = yield resource_client.get_head(branch)

            pc = resource_reference._parent_commit
            resource_description = yield resource_client.checkout(commit_id=pc)
            resource_description._branch = branch
            resource_description._parent_commit = pc
        else:
            resource_description=None
        defer.returnValue(resource_description)

    @defer.inlineCallbacks
    def list(self):
        """
        @brief list of resource description uuids(names)
        """
        idlist = yield self.refs.query('([-\w]*$)')
        defer.returnValue([ResourceReference(id=id) for id in idlist])


    @defer.inlineCallbacks
    def list_descriptions(self):
        refs = yield self.list()
        #logging.info('ID List:'+str(ids))
        # Should this return a dictionary with UUID:Resource?
        # Should the UUID be stored as part of the resource?
        defer.returnValue([(yield self.get_description(ref)) for ref in refs])
            
#    @defer.inlineCallbacks
#    def get_history(self,parent=None):
        


@defer.inlineCallbacks
def test(ns):
    from ion.data import store
    s = yield store.Store.create_store()
    ns.update(locals())
    reg = yield ResourceRegistry.new(s, 'registry')
    res1 = ResourceDescription.create_new_resource()
    ns.update(locals())
    res1.name = 'foo'
    commit_id = yield reg.register(res1)
    res2 = ResourceDescription.create_new_resource()
    res2.name = 'doo'
    commit_id = yield reg.register(res2)
    ns.update(locals())


