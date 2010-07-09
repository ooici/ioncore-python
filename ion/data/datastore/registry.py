"""
@file ion/data/datastore/registry.py
"""

import logging
logging = logging.getLogger(__name__)

from zope import interface

from twisted.internet import defer

from ion.data import dataobject
from ion.data.datastore import objstore



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
    objectClass = dataobject.ResourceDescription

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
        assert isinstance(resource_reference, dataobject.ResourceReference)
        
        branch = resource_reference._branch
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
        defer.returnValue([dataobject.ResourceReference(id=id) for id in idlist])


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
    res1 = dataobject.ResourceDescription.create_new_resource()
    ns.update(locals())
    res1.name = 'foo'
    commit_id = yield reg.register(res1)
    res2 = dataobject.ResourceDescription.create_new_resource()
    res2.name = 'doo'
    commit_id = yield reg.register(res2)
    ns.update(locals())


