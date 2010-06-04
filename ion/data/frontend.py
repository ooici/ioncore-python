#!/usr/bin/env python
"""
@file ion/data/frontend.py
@author Dorian Raymer
"""

import logging

from twisted.internet import defer
from twisted.python import reflect

from ion.data import objstore
from ion.data import dataobject 

class ContentStoreError(Exception):
    """
    """


class EntityProxy(objstore.Entity):
    """
    @brief Used for reading from the store
    """

    def __init__(self, name, hash, mode=None):
        objstore.Entity.__init__(self, name, hash, mode)
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
        @param backend (or objstore) active backend to read from
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

class TreeProxy(objstore.Tree):
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
        self.entitys = {}

    @defer.inlineCallbacks
    def load_objects(self):
        """
        """

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

class Frontend(objstore.CAStore):
    """
    """

    def __init__(self, backend, namespace=''):
        """
        @note Design decision on qualifying/naming a store name space (like
        a git repository tree)
        """
        objstore.CAStore.__init__(self, backend, namespace)
        self.working_tree = None
        self.TYPES['tree'] = TreeProxy

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
        @retval Deferred
        """
        return self.refstore.put(ref, commit_id)

    def get_head(self, head='master'):
        """
        @retval commit object id
        @todo get('heads.master') instead of just get('head')
        @note if head is not there, *IStore says return None*
        """
        return self.refstore.get(head)

    @defer.inlineCallbacks
    def checkout(self, head=None):
        """
        @retval Instance of WorkingTree
        """
        head = yield self.get_head()
        if head:
            commit = yield self.get(head)
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



