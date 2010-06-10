#!/usr/bin/env python
"""
@file ion/data/frontend.py
@author Dorian Raymer
"""

import logging

from twisted.internet import defer
from twisted.python import reflect

from ion.data.datastore import cas
from ion.data import dataobject 

class ObjectStoreError(Exception):
    """
    Base ObjectStore Exception
    """

class UUID(cas.Blob):

    type = 'uuid'



class Tree(cas.Tree):

    def load(self, backend):
        """
        load the entities/children of this tree
        """

class EntityProxy(cas.Entity):
    """
    @brief Used for reading from the store
    """

    def __init__(self, name, hash, mode=None):
        cas.Entity.__init__(self, name, hash, mode)
        self._obj = None

    def get_obj(self, backend):
        """
        @brief Get object from backend store, cache result.
        @retval Deferred that fires with obj
        """
        if not self._obj:
            d = backend.get(self[1])
            def set_obj(obj):
                self._obj = obj
                return obj
            d.addCallback(set_obj)
            return d
        return defer.succeed(self._obj)


class BlobProxy(object):
    """
    Inverse of regular Blob object.
    Start with the object id (hash), fetching the content when needed
    """

    def __init__(self, backend, id):
        """
        @param backend (or cas) active backend to read from
        @param id the object hash
        """
        self.objstore = backend
        self.id = id
        self._content = None

    def get_content(self):
        """
        @retval a Deferred that will fire with the content
        """
        if not self._content:
            d = self.objstore.get(self.id)
            def store_result(content):
                self._content = content
                return content
            d.addCallback(store_result)
        else:
            d = defer.succeed(self._content)
        return d

class TreeProxy(cas.Tree):
    """
    Live tree of real objects
    """

    entityFactory = EntityProxy

    def __init__(self, backend, *children):
        """
        @param child An element of the tree (a child entity).
        """
        self.backend = backend
        self.children = children

    @defer.inlineCallbacks
    def load_children(self):
        for child in self.children:
            child_obj = yield child.get_obj(self.backend)

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

class Frontend(cas.CAStore):
    """
    """

    def __init__(self, backend, namespace=''):
        """
        @note Design decision on qualifying/naming a store name space (like
        a git repository tree)
        """
        cas.CAStore.__init__(self, backend, namespace)
        self.working_tree = None
        #self.TYPES['tree'] = TreeProxy

    @classmethod
    def new(cls, backend, name):
        """
        @brief Create a new store named 'name'. This name should not exist
        yet in the backend.
        """
        new = cls(backend, name)
        d = new._store_exists()

        def init(result):
            if result:
                return defer.fail(ContentStoreError('Name already Exists'))

            d = new._init_store_metadata()
            return d

        d.addCallback(init)
        d.addCallback(lambda r: new)
        return d

    @defer.inlineCallbacks
    def _init_store_metadata(self):
        """
        @brief write basic information about this store
        """
        yield self.infostore.put('name', self.root_namespace)
        yield self.infostore.put('type', reflect.fullyQualifiedName(self.__class__))
        yield self.infostore.put('head', 'master')

    def _store_exists(self):
        """
        @brief check to see if name corresponds to a store in the backend.
        @retval Deferred that returns True if the name exists; False
        otherwise.
        """
        d = self.infostore.get('name')
        d.addCallback(lambda r: bool(r))
        return d

    def _get_symbolic_ref(self, name='HEAD'):
        """
        """

    def _set_symbolic_ref(self, name='HEAD', ref=''):
        """
        """


    def update_ref(self, commit_id, ref='master'):
        """
        @brief Update association of reference name to commit id.
        @retval Deferred
        """
        return self.refstore.put(ref, commit_id)

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
        return self.refstore.get(name)

    @defer.inlineCallbacks
    def checkout(self, head=None):
        """
        @retval Instance of WorkingTree
        """
        ref = yield self.get_head()
        if ref:
            commit = yield self.get(ref)
            tree = yield self.get(commit.tree)
            wt = WorkingTree(self, tree, commit)
        else:
            wt = WorkingTree(self)
        defer.returnValue(wt)

    def commit(self, working_tree=None):
        """
        """

    def _get_named_entity(self, name):
        """
        @brief Get object by name.
        @param name represents an object in a tree
        """

    def _put_raw_data_value(self, name, value):
        """
        @brief Write a piece of raw data
        """
        b = Blob(value)
        t = Tree(Entity(name, b.hash))
        d = self.put(t)
        #d.addCallback(

    def get_info(self, id):
        """
        @brief get an objects type
        """

class BaseObjectChassis(object):
    """
    """

    def __init__(self, objstore, refs, meta, commit=None):
        """
        """
        self.objstore = objstore
        self.refs = cas.StoreContextWrapper(objstore.backend, namespace + '.refs.')
        self.meta = cas.StoreContextWrapper(objstore.backend, namespace + '.meta.')

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

    def update_ref(self, commit_id, ref='master'):
        """
        @brief Update association of reference name to commit id.
        @retval Deferred
        """

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

    @defer.inlineCallbacks
    def checkout(self, head=None):
        """
        @retval Instance of WorkingTree
        """
        ref = yield self.get_head()
        if ref:
            commit = yield self.get(ref)
            tree = yield self.get(commit.tree)
            wt = WorkingTree(self, tree, commit)
        else:
            wt = WorkingTree(self)
        defer.returnValue(wt)

    def load():
        """
        """

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
        self.TYPES[UUID.type] = UUID

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
        d = self._object_exists()

        def _succeed(result):
            if not result:
                # create object chassis
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
        obj = yield self.get(name)
        if obj:
            exists = True
        else:
            exists = False
        defer.returnValue(exists)

    def clone(self, name):
        """
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
    obs = yield BaseObjectStore.new(s, 'test_partition')
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






