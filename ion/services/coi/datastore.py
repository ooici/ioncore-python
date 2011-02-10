#!/usr/bin/env python

"""
@file ion/services/coi/datastore.py
@author David Stuebe

@TODO
use persistent key:value store in work bench to persist push and get pull!
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.object import object_utils
from ion.core.object import gpb_wrapper
from ion.core.data import store
from ion.core.data import cassandra

import binascii

from ion.core import ioninit
CONF = ioninit.config(__name__)

link_type = object_utils.create_type_identifier(object_id=3, version=1)
commit_type = object_utils.create_type_identifier(object_id=8, version=1)
mutable_type = object_utils.create_type_identifier(object_id=6, version=1)
structure_element_type = object_utils.create_type_identifier(object_id=1, version=1)

association_type = object_utils.create_type_identifier(object_id=13, version=1)
terminology_type = object_utils.create_type_identifier(object_id=14, version=1)


class DataStoreError(Exception):
    """
    An exception class for the data store
    """


class DataStoreService(ServiceProcess):
    """
    The data store is not yet persistent. At the moment all its stored objects
    are kept in a python dictionary, part of the work bench. This service will
    be modified to use a persistent store - a set of cache instances to which
    it will dump data from push ops and retrieve data for pull and fetch ops.
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='datastore',
                                             version='0.1.0',
                                             dependencies=[])

    LinkClassType = object_utils.create_type_identifier(object_id=3, version=1)

    MUTABLE_STORE = 'mutable_store_class'
    COMMIT_STORE = 'commit_store_class'
    BLOB_STORE = 'blob_store_class'
    
    CommitIndexName = 'repository'
    
    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        #assert isinstance(backend, store.IStore)
        #self.backend = backend
        ServiceProcess.__init__(self, *args, **kwargs)        
            
        self._backend_cls_names = {}
        #self.spawn_args['_class'] = self.spawn_args.get('_class', CONF.getValue('_class', default='ion.data.store.Store'))
        self._backend_cls_names[self.MUTABLE_STORE] = self.spawn_args.get(self.MUTABLE_STORE, CONF.getValue(self.MUTABLE_STORE, default='ion.core.data.store.Store'))
        self._backend_cls_names[self.COMMIT_STORE] = self.spawn_args.get(self.COMMIT_STORE, CONF.getValue(self.COMMIT_STORE, default='ion.core.data.store.IndexStore'))
        self._backend_cls_names[self.BLOB_STORE] = self.spawn_args.get(self.BLOB_STORE, CONF.getValue(self.BLOB_STORE, default='ion.core.data.store.Store'))
            
        self._backend_classes={}
            
        self._backend_classes[self.MUTABLE_STORE] = pu.get_class(self._backend_cls_names[self.MUTABLE_STORE])
        assert store.IStore.implementedBy(self._backend_classes[self.MUTABLE_STORE]), \
            'The back end class to store mutable objects passed to the data store does not implement the required ISTORE interface.'
            
        self._backend_classes[self.COMMIT_STORE] = pu.get_class(self._backend_cls_names[self.COMMIT_STORE])
        assert store.IIndexStore.implementedBy(self._backend_classes[self.COMMIT_STORE]), \
            'The back end class to store commit objects passed to the data store does not implement the required IIndexSTORE interface.'
            
        self._backend_classes[self.BLOB_STORE] = pu.get_class(self._backend_cls_names[self.BLOB_STORE])
        assert store.IStore.implementedBy(self._backend_classes[self.BLOB_STORE]), \
            'The back end class to store blob objects passed to the data store does not implement the required ISTORE interface.'
            
        # Declare some variables to hold the store instances
        self.m_store = None
        self.c_store = None
        self.b_store = None
            

        log.info('DataStoreService.__init__()')
        

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass
        

    @defer.inlineCallbacks
    def slc_activate(self):
        
        if issubclass(self._backend_classes[self.MUTABLE_STORE], cassandra.CassandraStore):
            raise NotImplementedError('Startup for cassandra store is not yet complete')
        else:
            self.m_store = yield defer.maybeDeferred(self._backend_classes[self.MUTABLE_STORE])
        
        if issubclass(self._backend_classes[self.COMMIT_STORE], cassandra.CassandraStore):
            raise NotImplementedError('Startup for cassandra store is not yet complete')
        else:
            self.c_store = yield defer.maybeDeferred(self._backend_classes[self.COMMIT_STORE])
        
        if issubclass(self._backend_classes[self.BLOB_STORE], cassandra.CassandraStore):
            raise NotImplementedError('Startup for cassandra store is not yet complete')
        else:
            self.b_store = yield defer.maybeDeferred(self._backend_classes[self.BLOB_STORE])
        

    
    @defer.inlineCallbacks
    def op_push(self, heads, headers, msg):
        
        pushed_repos = {}
        
        for head in heads:
            
            # Extract the repository key from the mutable
            raw_mutable = object_utils.get_gpb_class_from_type_id(mutable_type)()
            raw_mutable.ParseFromString(head.value)
            repo_key = str(raw_mutable.repositorykey)
            
            # Get the mutable
            store_head = None
            store_commits = {}

            pushed_repos[repo_key] = store_commits

            blob = yield self.m_store.get(repo_key)
            # if the store has a version of the repo - then load it
            if blob:
                store_head = gpb_wrapper.StructureElement.parse_structure_element(blob)
                self.workbench._hashed_elements[store_head.key]=store_head
                
                # Get the commits using the query interface
                blobs = yield self.c_store.query({self.CommitIndexName:repo_key})
                    
                for key, blob in blobs.items():
                    #print 'BLOB key: "%s"; value: "%s"' % (binascii.b2a_hex(key), binascii.b2a_hex(blob))
                    wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
                    assert key == wse.key, 'Calculated key does not match the stored key!'
                    store_commits[wse.key] = wse
                    
                    
                # Load these commits into the workbench
                self.workbench._hashed_elements.update(store_commits)
                    
                repo = self.workbench._load_repo_from_mutable(store_head)
                    
                # Check to make sure the mutable is upto date with the commits...
                for commit_key in store_commits.keys():
                    if not commit_key in repo._commit_index:
                        raise DataStoreError('Can not handle divergence yet...')
            
            
        yield self.workbench.op_push(heads, headers, msg)
        
        def_list = []
        # First put the updated commits
        for repo_key, store_commits in pushed_repos.items():
            # Get the updated repository
            repo = self.workbench.get_repository(repo_key)
            
            # any objects in the data structure that were transmitted have already
            # been updated during fetch linked objects.
            
            
            for key in repo._commit_index.keys():
                if not key in store_commits:
                    
                    attributes = {self.CommitIndexName : str(repo_key)}
                    
                    wse = self.workbench._hashed_elements.get(key)
                    
                    cref = repo._commit_index.get(key)
                    if cref.objectroot.ObjectType == association_type:
                        attributes['subject'] = cref.objectroot.subject
                        attributes['predicate'] = cref.objectroot.predicate
                        attributes['object'] = cref.objectroot.object
                        
                    elif  cref.objectroot.ObjectType == terminology_type:
                        attributes['word'] = cref.objectroot.word
                    
                    # Should replace this with one put slice command
                    defd = self.c_store.put(key = key,
                                           value = wse.serialize(),
                                           index_attributes = attributes)
                    def_list.append(defd)
            
        yield defer.DeferredList(def_list)
            
        # Pretty useless to try and debug by reading, but its a start...   
        #print 'KVS: \n', self.c_store.kvs, '\n\n'
        
        #print 'Index: \n', self.c_store.indices, '\n\n'
            
            
        def_list = []
        # Now put the mutable heads
        for repo_key in pushed_repos.keys():
            repo = self.workbench.get_repository(repo_key)
            wse = self.workbench.serialize_mutable(repo._dotgit)
            defd = self.m_store.put(repo_key, wse.serialize())
            
            def_list.append(defd)
            
        yield defer.DeferredList(def_list)
        
            
                
        
    @defer.inlineCallbacks
    def op_pull(self, content, headers, msg):
        """
        Content is a string - the name of a mutable head for a repository
        """
        repo_key = str(content)
        
        store_commits ={}
        
        blob = yield self.m_store.get(repo_key)
        # if the store has a version of the repo - then load it
        if blob:
            store_head = gpb_wrapper.StructureElement.parse_structure_element(blob)
            self.workbench._hashed_elements[store_head.key]=store_head
                
            # Get the commits using the query interface
            blobs = yield self.c_store.query({self.CommitIndexName:repo_key})
                
            for key, blob in blobs.items():
                #print 'BLOB key: "%s"; value: "%s"' % (binascii.b2a_hex(key), binascii.b2a_hex(blob))
                wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
                assert key == wse.key, 'Calculated key does not match the stored key!'
                store_commits[wse.key] = wse
                    
                    
                # Load these commits into the workbench
                self.workbench._hashed_elements.update(store_commits)
                    
                repo = self.workbench._load_repo_from_mutable(store_head)
                    
                # Check to make sure the mutable is upto date with the commits...
                for commit_key in store_commits.keys():
                    if not commit_key in repo._commit_index:
                        raise DataStoreError('Can not handle divergence yet...')
        
        yield self.workbench.op_pull(content, headers, msg)
        
    @defer.inlineCallbacks
    def op_fetch_linked_objects(self, elements, headers, message):
        """
        The data store is getting objects for another process...
        """
        def_list=[]
        # Elements is a dictionary of wrapped structure elements
        for se in elements.values():
            
            assert se.type == link_type, 'This is not a link element!'
            link = object_utils.get_gpb_class_from_type_id(link_type)()
            link.ParseFromString(se.value)
                
            # if it is already in memory, don't worry about it...
            if not link.key in self.workbench._hashed_elements:            
                if link.type == commit_type:
                    # Can get commits for a service in a fetch
                    def_list.append(self.c_store.get(link.key))
                else:
                    def_list.append(self.b_store.get(link.key))
            
        obj_list = yield defer.DeferredList(def_list)
        #print 'OBJECT LIST:', obj_list
            
        # Load this list of objects from the store into memory for use in the datastores workbench
        for result, blob in obj_list:
            wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
            self._hashed_elements[wse.key]=wse
            
        yield self.workbench.op_fetch_linked_objects(elements, headers, message)
        
    @defer.inlineCallbacks
    def push(self, *args):
        """
        This method is only for testing purposes. There is not need architecturally
        for the data store to push to other services...
        """
        
        ret = yield self.workbench.push(*args)
        
        defer.returnValue(ret)
        
    @defer.inlineCallbacks
    def pull(self, *args):

        ret = yield self.workbench.pull(*args)

        defer.returnValue(ret)
        
    @defer.inlineCallbacks
    def fetch_linked_objects(self, address, links):
        """
        The datastore is getting any objects it does not already have... 
        """
        
        #Check and make sure it is not in the datastore
        def_list = []
        for link in links:
            if not link.key in self.workbench._hashed_elements:            
                # Can request to get commits in a fetch...
                if link.type == commit_type:
                    def_list.append(self.c_store.get(link.key))
                else:
                    def_list.append(self.b_store.get(link.key))
        
        #for defd in def_list:
        #    print 'Defd type: %s; value: %s' % (type(defd), defd)
        
        # The list of requested objects that are in the store
        obj_list = yield defer.DeferredList(def_list)
        
        #for obj in obj_list:
        #    print 'obj type: %s; value: %s' % (type(obj), obj)
        
        
        # If we have the object, put it in the work space, if not request it.
        need_list = []
        obj_dict = {}
        
        # For some reason the obj_list is a tuple not just the value of the result
        for link, (result, blob)  in zip(links, obj_list):
            if blob is None:
                need_list.append(link)
                obj_dict[link.key] = None
            else:
                #print 'BLOB type: %s; value: %s' % (type(blob), blob)
                wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
                self._hashed_elements[wse.key]=wse
                obj_dict[link.key] = wse


        # Get these from the other service
        if need_list:
            got_objs = yield self.workbench.fetch_linked_objects(address, need_list)
        
        def_list = []
        for key, wse in got_objs.items():
            #if wse.type == commit_type:
            #    raise DataStoreError('Can not get commits in a fetch!')
            #    def_list.append(self.c_store.put(key, wse.serialize()))
            #else:
            #    def_list.append(self.b_store.put(key, wse.serialize()))
            
            # Don't ever put commits out of context. This is done by push!
            if wse.type != commit_type:
                def_list.append(self.b_store.put(key, wse.serialize()))
                
            # Add it to the dictionary of objects 
        
        obj_dict.update(got_objs)
        
        yield defer.DeferredList(def_list)
        
        defer.returnValue(obj_dict.values())
        
        
        


# Spawn of the process using the module name
factory = ProcessFactory(DataStoreService)


