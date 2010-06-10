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

class ContentStoreError(Exception):
    """
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



