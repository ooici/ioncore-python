"""
@file ion/data/datastore/registry.py
"""

from zope import interface

from twisted.internet import defer

from ion.data.datastore import objstore

class LCState(object):

    def __repr__(self):
        return "%s" % self.__class__.__name__

LCStateNames = ['new',
                'active',
                'inactive',
                'decomm',
                'retired',
                'developed',
                'commissioned',
                ]

LCStates = dict([(name, type(name, (LCState,), {})()) for name in LCStateNames])

class StateAttribute(objstore.TypedAttribute):
    """
    """
    states = LCStates


class ResourceDescription(objstore.DataObject):
    """
    @brief Base for all OOI resource objects
    @note OOIResource or OOIRegistryObject or OOIObject???
    @note could build in explicit link back to ResourceRegistryClient so
    user can make changes through this object.
    """
    name = TypedAttribute(str)
    lifecycle = StateAttribute(LCState, default=LCStates['new'])

class Generic(ResourceDescription):
    """
    """
    name = TypedAttribute(str)
    lifecycle = StateAttribute(LCState, default=LCStates['new'])


class IResourceRegistry(interface.Interface):
    """
    @brief General API of any registry
    """

    def register(uuid, resource):
        """
        @brief Register resource description.
        @param uuid unique name of resource instance.
        @param resource instance of OOIResource.
        @note Does the resource instance define its own name/uuid?
        """

    def get_description(uuid):
        """
        @param uuid name of resource.
        """

    def set_resource_lcstate(uuid, state):
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
    def register(self, uuid, resource):
        """
        @brief Add a new resource description to the registry. Implemented
        by creating a new (unique) resource object to the store.
        @note Is the way objectClass is referenced awkward?
        """
        assert isinstance(resource, self.objectChassis.objectClass)
        
        try:
            res_client = yield self.create(uuid, self.objectChassis.objectClass)
        except objstore.ObjectStoreError:
            res_client = yield self.clone(uuid)
            
        yield res_client.checkout()
        res_client.index = resource

        c_id = yield res_client.commit()

        defer.returnValue(c_id)

        # now what?

    @defer.inlineCallbacks
    def get_description(self, uuid):
        """
        @brief Get resource description object
        """
        resource_client = yield self.clone(uuid)
        resource_description = yield resource_client.checkout()
        defer.returnValue(resource_description)


