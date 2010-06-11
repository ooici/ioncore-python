#!/usr/bin/env python
"""
@file ion/data/datastore/objstore.py
@author Dorian Raymer
@author David Stuebe
@author Michael Meisinger
"""

import logging

from twisted.internet import defer
from twisted.python import reflect

from ion.data.datastore import cas

class ObjectStoreError(Exception):
    """
    Base ObjectStore Exception
    """

class Entity(cas.Entity):

    def load(self, backend):
        """
        @brief experimental way to dynamically load objects from id's.
        """
        if not self.obj:
            def cb(obj):
                self.obj = obj
                return obj.load(backend)

            d = backend.get(self.obj_id)
            d.addCallback(cb)
            return d
        return defer.succeed(None)

class UUID(cas.Blob):
    """
    Names of objects are UUIDs and are stored as content objects.
    """

    type = 'uuid'

class Blob(cas.Blob):

    def load(self, backend):
        """
        @brief leaf of load recursion. Do nothing.
        """
        return defer.succeed(self.content)

class Tree(cas.Tree):

    entityFactory = Entity

    def load(self, backend):
        """
        @brief Call load on all entities.
        """
        return defer.DeferredList([child.load(backend) for child in self.children])

    def __getitem__(self, name):
        """
        lazy loading
        """

class Commit(cas.Commit):

    def load(self, backend, recursive=False):
        """
        """
        if not self.tree_obj:
            d = backend.get(self.tree)
            def cb(obj):
                self.tree_obj = obj
                if recursive:
                    return obj.load(backend)
            d.addCallback(cb)
            return d
        return defer.succeed(None)

class ActiveObject(object):
    """
    Container/proxy for storable/retrievable object.
    """

    def __init__(self, backend, id=None, obj=None):
        """
        @param backend Live sa store instance. 
        @param id object hash.
        @param obj instance of decoded object.
        """
        self.backend = backend
        self.id = id
        self.obj = obj
    
    def load():
        """
        """

class ActiveTree(ActiveObject):
    """
    The tree concept is the thing mediating the mapping between the pure
    data storage model and the higher level application data model.
    A tree [or, in general, a graph] is an environment, the child element
    of a tree node is the entity of the environment. The entity can be a
    tree itself, so internally, it is another environment hosting other
    entities. The elements of a tree node are a 2-tuple (key/value pair
    where the key is a name and the value is an entity object). The name is
    an external identifier that has no intrinsic meaning to the tree model.
    The entity is a fundamental object of the tree model.

    The node entity is a container/collection of name:entity pairs.

    Content:
        Pure data content - leaf nodes

    """

    def __init__(self, backend, tree=None):
        self.backend = backend
        self.tree = tree

    @classmethod
    def load(cls, backend, tree):
        """
        @brief Create ActiveTree from existing
        """
        if not isinstance(tree, cas.BaseObject):
            # Assume it's an id
            d = backend.get(tree)
            d.addCallback()

    @classmethod
    def new(cls, backend):
        """
        @brief Create empty ActiveTree.
        """

    def __call__(self):
        """
        snap shot?
        """

    def __getitem__(self, name):
        """
        @brief Retrieve an object by the name associated with it in this
        tree.
        @param name of object
        @retval object associated with name.
        """

class WorkingTree(object):
    """
    Tree of objects that may or may not be written to backend store
    One way this object is created is by loading a commit.
    """

    def __init__(self, backend, tree=None, commit=None):
        """
        @param backend Instance of CAStore.
        @param name of working tree; could have more than one.
        """
        self.backend = backend
        self._init_tree = tree
        self._init_commit = commit

    def load_objects(self):
        """
        """
        return self._init_tree.load(self.backend)

    @classmethod
    def load_commit(cls, backend, commit):
        """
        """
        tree_id = commit.tree
        tree = backend.get(tree_id)
        return cls(backend, tree, commit)

    def add(self, entity):
        """
        @param entity Instance of Entity or EntityProxy.
        """

    def update(self, entity):
        """
        """

    def remove(self, entity):
        """
        """

class BaseObjectChassis(object):
    """
    """

    def __init__(self, objstore, refs, meta, commit=None):
        """
        @note
        objstore vs. objs
        objstore implements ICAStore; objs is just the raw key/value
        (namespaced) interface to the content objects.
        refs is not a refstore (yet); it is just the namespaced key/value
        store for accessing the refs. Same for meta.
        objstore expects/returns cas.BaseObject types
        refs & meta don't necessarily have types (yet).
        """
        self.objstore = objstore
        self.refs = refs
        self.meta = meta

    @defer.inlineCallbacks
    def _init_metadata(self):
        """
        @brief write basic information about this store
        @note self.objs.put(namespace, id) is used so that the uuid
        representing an object is the key to the uuid object.
        """
        uuid_obj = UUID(self.namespace)
        id = yield self.put(uuid_obj)
        yield self.objs.put(self.namespace, id)
        defer.returnValue(self) # Return this instance

    def _get_object_meta(self):
        """
        @brief Common meta info on this object.
        It's like the misc mutable stuff in .git
        """
        d = self.meta.get('')

        def _raise_if_none(res):
            if not bool(res):
                raise ObjectStoreError("Store meta not found")
            return res
        d.addCallback(_raise_if_none)
        return d

class ObjectChassis(BaseObjectChassis):
    """
    This establishes a context for accessing/modifying/committing an
    "Object" or "Resource Entity" structure...thing.

    """

    def update_head(self, commit_id, head='master'):
        """
        @param commit_id Id of CAStore commit object.
        @brief Update association of reference name to commit id.
        In general, head is a ref, but other refs are not yet implemented.
        @retval Deferred
        """
        self.refs(head, commit_id)

    def get_head(self, name='master'):
        """
        @brief Get reference named 'name'. The head references represent
        commit ancestry chains. Master is the name of the main commit
        linage. A branch is represents a divergent commit linage. Any head
        reference besides the one named 'master' is a branch head.
        @retval commit object id
        @todo get('heads.master') instead of just get('head')
        @note if head is not there, *IStore says return None*
        """
        return self.refs.get(name)

    @defer.inlineCallbacks
    def checkout(self, head='master'):
        """
        """
        ref = yield self.get_head(head)
        if ref:
            commit = yield self.get(ref)
            tree = yield self.get(commit.tree)
            wt = WorkingTree(self, tree, commit)
        else:
            wt = WorkingTree(self)
        defer.returnValue(wt)

    def add(self, element):
        """
        @param entity Instance of Entity or EntityProxy.
        """

    def update(self, element):
        """
        """

    def remove(self, element):
        """
        """

    def commit(self, working_tree=None):
        """
        """



class BaseObjectStore(cas.CAStore):
    """
    @brief Interactive interface to an Object Store instance (who's raw
    data lives on some 'backend'. An instance of this object sets the
    context for creating/modifying/retrieving "Object" or "Resource
    Entity" structures from a general key/value store. Each structure is
    accessed by a UUID. 
     - Each structure is comparable to a git repository
    Each structure is a model composed of references to content objects
    (blobs, etc.).
    """
    TYPES = {
            UUID.type:UUID,
            Blob.type:Blob,
            Tree.type:Tree,
            Commit.type:Commit,
            }

    def __init__(self, backend, partition=''):
        """
        @param backend instance providing ion.data.store.IStore interface.
        @param partition Logical name spacing in an otherwise flat
        key/value space.
        @note Design decision on qualifying/naming a store name space (like
        a git repository tree)
        """
        cas.CAStore.__init__(self, backend, partition)
        self.partition = partition
        self.storemeta = cas.StoreContextWrapper(backend, partition + '.meta.')
        self.type = reflect.fullyQualifiedName(self.__class__)

    @classmethod
    def new(cls, backend, name):
        """
        @brief Initialize an Object Store in the backend.  
        This is a major operation, like formating a blank hard drive; In
        general, this only needs to be done once to a backend.
        @retval A Deferred that succeeds with a new instance.
        """
        inst = cls(backend, name)
        d = inst._store_exists()

        def _succeed(result):
            if not result:
                d = inst._init_store_metadata()
                return d 
            return _fail(result)

        def _fail(result):
            return defer.fail(ObjectStoreError('Partition name already exists'))

        d.addCallback(_succeed)
        d.addErrback(lambda r: _fail(r))
        return d

    @classmethod
    def load(cls, backend, name):
        """
        @brief Connect to an existing object store namespace that lives
        inthe given backend.
        @retval A Deferred that succeeds with an instance of ObjectStore
        """
        inst = cls(backend, name)
        d = inst._store_exists()

        def _succeed(result):
            if result:
                return inst
            return defer.fail(ObjectStoreError("Partition name does not exist"))

        def _fail(result):
            return defer.fail(ObjectStoreError("Error checking partition named %s" % name))

        d.addCallback(_succeed)
        d.addErrback(lambda r: _fail(r))

    @defer.inlineCallbacks
    def _init_store_metadata(self):
        """
        @brief write basic information about this store
        """
        yield self.storemeta.put('type', self.type)
        defer.returnValue(self) # Return this instance

    def _get_store_meta(self):
        d = self.storemeta.get('type')

        def _raise_if_none(res):
            if not bool(res):
                raise ObjectStoreError("Store meta not found")
            return res
        d.addCallback(_raise_if_none)
        return d

    def _store_exists(self):
        """
        @brief Verify the backend IStore contains the keys that represent
        this stores namespace.
        @retval A Deferred 
        """
        d = self._get_store_meta()
        d.addCallback(lambda res: True)
        d.addErrback(lambda res: False)
        return d


class ObjectStore(BaseObjectStore):

    def create(self, name):
        """
        @param name of object store object to create...
        @brief Create a new object named 'name'. This name should not exist
        yet in the object store.
        @retval A Deferred that succeeds with a new instance.
        @note Could just use an idempotent declare/get thing instead of
        error-ing on create
        """
        d = self._object_exists(name)

        def _succeed(result):
            if not result:
                return self._create_object(name)
            return _fail(result)

        def _fail(result):
            return defer.fail(ObjectStoreError('Object name already exists'))

        d.addCallback(_succeed)
        d.addErrback(lambda r: _fail(r))
        return d

    def _create_object(self, name):
        """
        @note Not Using Object Store Partition Namespace Yet.
        Might not need to
        """
        refs = cas.StoreContextWrapper(self.backend, name + '.refs.')
        meta = cas.StoreContextWrapper(self.backend, name + '.meta.')
        return ObjectChassis(self, refs, meta)

    @defer.inlineCallbacks
    def _object_exists(self, name):
        """
        @brief does object store object exist?
        @param name in this context is the id of the cas uuid object.
        @retval A Deferred
        """
        try:
            obj = yield self.get(cas.sha1(name))
            exists = True
        except cas.CAStoreError:
            exists = False
        defer.returnValue(exists)

    def clone(self, name):
        """
        @param name uuid of object store object.
        @brief Clone an object from the object store. The semantic of clone
        as opposed to get is very important. Objects in the object store
        represent mutable models of immutable constituents. Getting is not
        the appropriate verb because you are not getting something
        canonical; this makes much sense when you consider the compliment put
        idea. There is no put, there is commit, and update ref,
        """
        uuid = self.get(name)


@defer.inlineCallbacks
def _test(ns):
    from ion.data import store
    s = yield store.Store.create_store()
    obs = yield ObjectStore.new(s, 'test_partition')
    obj = yield obs.create('thing')
    ns.update(locals())


"""
@note Two organizational strategies:
    1 BaseObjectStore/ObjectStore class methods as factories for creating
      and loading ObjectStore instances.
    2 ObjectStore Factory for setting up an ObjsectStore instance.

With 1, ObjectStore would have method names for creating/checking Stores
and also the actual Objects in the ObjectStore.
"""


class ObjectStoreFactory(object):
    """
    Sets up an object store
    """

    def __init__(self, backend):
        """
        """
        self.backend = backend






