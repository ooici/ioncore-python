#!/usr/bin/env python

"""
@file ion/services/coi/datastore.py
@author David Stuebe
@author Matt Rodriguez
@TODO
Deal with a commit that is the head of more than one branch!

"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess
from ion.core.exception import ReceivedError, ApplicationError

from ion.core.object import object_utils
from ion.core.object import gpb_wrapper, repository
from ion.core.object.workbench import WorkBench, WorkBenchError, PUSH_MESSAGE_TYPE, PULL_MESSAGE_TYPE, PULL_RESPONSE_MESSAGE_TYPE, BLOBS_REQUSET_MESSAGE_TYPE
from ion.core.data import store
from ion.core.data import cassandra
from ion.core.data.store import Query


from ion.core.data.storage_configuration_utility import BLOB_CACHE, COMMIT_CACHE
from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS
from ion.core.data.storage_configuration_utility import REPOSITORY_KEY, BRANCH_NAME

from ion.core.data.storage_configuration_utility import SUBJECT_KEY, SUBJECT_BRANCH, SUBJECT_COMMIT
from ion.core.data.storage_configuration_utility import PREDICATE_KEY, PREDICATE_BRANCH, PREDICATE_COMMIT
from ion.core.data.storage_configuration_utility import OBJECT_KEY, OBJECT_BRANCH, OBJECT_COMMIT

from ion.core.data.storage_configuration_utility import KEYWORD, VALUE

from ion.core import ioninit
CONF = ioninit.config(__name__)


LINK_TYPE = object_utils.create_type_identifier(object_id=3, version=1)
COMMIT_TYPE = object_utils.create_type_identifier(object_id=8, version=1)
MUTABLE_TYPE = object_utils.create_type_identifier(object_id=6, version=1)
STRUCTURE_ELEMENT_TYPE = object_utils.create_type_identifier(object_id=1, version=1)

ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
TERMINOLOGY_TYPE = object_utils.create_type_identifier(object_id=14, version=1)


# Set some constants based on the config file:


class DataStoreWorkbench(WorkBench):


    def __init__(self, process, blob_store, commit_store):

        workbench.WorkBench.__init__(self, process)

        self._blob_store = blob_store
        self._commit_store = commit_store


    def pull(self, *args, **kwargs):

        raise NotImplementedError("The Datastore Service can not Pull")

    def push(self, *args, **kwargs):

        raise NotImplementedError("The Datastore Service can not Push")

    @defer.inlineCallbacks
    def op_pull(self,request, headers, msg):
        """
        The operation which responds to a pull request

        The pull is much higher heat that I would like - it requires decoding the serialized blobs.
        We should consider storing the child links of each element external to the element - but then the put is
        high heat... Not sure what to do.
        """


        if not hasattr(request, 'MessageType') or request.MessageType != PULL_MESSAGE_TYPE:
            raise WorkBenchError('Invalid pull request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)


        repo = self.get_repository(request.repository_key)
        if repo is None:
            #if it does not exist make a new one
            repo = repository.Repository(repository_key=repo_name)
            self.put_repository(repo)

        # Must reconstitute the head and merge with existing
        mutable_cls = object_utils.get_gpb_class_from_type_id(MUTABLE_TYPE)
        new_head = repo._wrap_message_object(mutable_cls(), addtoworkspace=False)
        new_head.repository_key = request.repository_key


        q = Query()
        q.add_predicate_eq(REPOSITORY_KEY, repo_key)

        rows = yield self._commit_store.query(q)

        for key, columns in rows.items():
            blob = columns[VALUE]
            wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
            assert key == wse.key, 'Calculated key does not match the stored key!'
            repo.index_hash[key] = wse

            if columns[BRANCH_NAME]:
                # If this appears to be a head commit

                for branch in new_head.branches:
                    # if the branch already exists in the new_head just add a commitref
                    if branch.branchkey == columns[BRANCH_NAME]:
                        link = branch.commitrefs.add()
                        break
                else:
                    # If not add a new branch
                    branch = new_head.branches.add()
                    branch.branchkey = columns[BRANCH_NAME]
                    link = branch.commitrefs.add()

                cref = repo._load_element(wse)
                repo._commit_index[cref.MyId]=cref
                obj.ReadOnly = True

                link.SetLink(cref)

                # Check to make sure the mutable is upto date with the commits...

        # Do the update!
        self._update_repo_to_head(repo, new_head)


        ####
        # Back to boiler plate op_pull
        ####

        my_commits = self.list_repository_commits(repo)

        puller_has = request.commit_keys

        puller_needs = set(my_commits).difference(puller_has)

        response = yield self._process.message_client.create_instance(PULL_RESPONSE_MESSAGE_TYPE)

        # Create a structure element and put the serialized content in the response
        head_element = self.serialize_mutable(repo._dotgit)
        # Pull out the structure element and use it as the linked object in the message.
        obj = response.Repository._wrap_message_object(head_element._element)

        response.repo_head_element = obj


        for commit_key in puller_needs:
            commit_element = repo.index_hash.get(commit_key)
            if commit_element is None:
                raise WorkBenchError('Repository commit object not found in op_pull', request.ResponseCodes.NOT_FOUND)
            link = response.commit_elements.add()
            obj = response.Repository._wrap_message_object(commit_element._element)
            link.SetLink(obj)


        if request.get_head_content:
            # Slightly different machinary here than in the workbench - Could be made more similar?
            blobs={}
            links_to_get=set()
            for commit in repo.current_heads():

                links_to_get.add(commit.GetLink('objectroot'))


            while len(links_to_get) > 0:

                new_links_to_get = set()


                def_list = []
                #@TODO - put some error checking here so that we don't overflow due to a stupid request!
                for link in links_to_get:

                    # Short cut if we have already got it!
                    wse = repo.index_hash.get(link.key)
                    if wse:
                        blobs[wse.key]=wse
                        # get the object
                        obj = repo.get_linked_object(link)

                        new_links_to_get.update(obj.ChildLinks)
                    else:
                        def_list.append(self._blob_store.get(link.key))


                result_list = []
                if def_list:
                    result_list = yield defer.DeferredList(def_list)

                for result, blob in result_list:
                    assert result==True, 'Error getting link from blob store!'
                    wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
                    blobs[wse.key]=wse

                    # Add it to the repository index
                    self.index_hash[wse.key] = wse

                    # load the object so we can find its children
                    obj = repo.get_linked_object(link)

                    new_links_to_get.update(obj.ChildLinks)

                links_to_get =[]
                for link in new_links_to_get:
                    if not blobs.has_key(link.key):
                        links_to_get.append(link)


            for element in blobs.values():
                link = response.blob_elements.add()
                obj = response.Repository._wrap_message_object(element._element)

                link.SetLink(obj)

        yield self._process.reply_ok(msg, content=response)




    @defer.inlineCallbacks
    def op_push(self, pushmsg, headers, msg):
        """
        The Operation which responds to a push.

        Operation does not complete until transfer is complete!
        """
        log.info('op_push!')

        if not hasattr(pushmsg, 'MessageType') or pushmsg.MessageType != PUSH_MESSAGE_TYPE:
            raise WorkBenchError('Invalid push request. Bad Message Type!', pushmsg.ResponseCodes.BAD_REQUEST)

        # A dictionary of the new commits received in the push - sorted by repository
        new_commits={}

        # A list of the blobs received - does not matter what repo they are in - just jam them into the store
        new_blob_keys =[]


        for repostate in pushmsg.repositories:

            repo = self.get_repository(repostate.repository_key)
            if repo is None:
                #if it does not exist make a new one
                repo = repository.Repository(repository_key=repostate.repository_key)
                self.put_repository(repo)
                repo_keys=set()
            else:

                if repo.status == repo.MODIFIED:
                    raise WorkBenchError('Requested push to a repository is in an invalid state: MODIFIED.', request.ResponseCodes.BAD_REQUEST)
                repo_keys = set(self.list_repository_blobs(repo))

            # add a new entry in the new_commits dictionary to store the commits of the push for this repo
            new_commits[repo.repository_key] = []

            # Get the set of keys in repostate that are not in repo_keys
            need_keys = set(repostate.blob_keys).difference(repo_keys)

            workbench_keys = set(self._workbench_cache.keys())

            local_keys = workbench_keys.intersection(need_keys)


            def_commit_list = []
            def_blob_list = []
            key_list = []
            for key in local_keys:
                try:
                    repo.index_hash.get(key)
                    need_keys.remove(key)
                    continue
                except KeyError, ke:
                    log.info('Key disappeared - get it from the remote after all')

                # @TODO Assumption is that this check is less costly than getting it from the remote service
                key_list.append(key)
                def_commit_list.append(self._commit_store.has_key(key))
                def_blob_list.append(self._blob_store.has_key(key))

            if key_list:
                result_commit_list = yield defer.DeferredList(def_commit_list)
                result_blob_list = yield defer.DeferredList(def_blob_list)

                # Remove
                for key, res1, have_blob, res2, have_commit in zip(key_list, result_blob_list, result_commit_list):

                    if have_blob or have_commit:
                        need_keys.remove(key)

            if len(need_keys) > 0:
                blobs_request = yield self._process.message_client.create_instance(BLOBS_REQUSET_MESSAGE_TYPE)
                blobs_request.blob_keys.extend(need_keys)

                try:
                    blobs_msg = yield self.fetch_blobs(headers.get('reply-to'), blobs_request)
                except ReceivedError, re:

                   log.debug('ReceivedError', str(re))
                   raise WorkBenchError('Fetch Objects returned an exception! "%s"' % re.msg_content)


                for se in blobs_msg.blob_elements:
                    # Put the new objects in the repository
                    element = gpb_wrapper.StructureElement(se.GPBMessage)
                    repo.index_hash[element.key] = element

                    if element.type == COMMIT_TYPE:
                        new_commits[repo.repository_key].append(element.key)
                    else:
                        new_blob_keys.append(element.key)


            # Move over the new head object
            head_element = gpb_wrapper.StructureElement(repostate.repo_head_element.GPBMessage)
            new_head = repo._load_element(head_element)
            new_head.Modified = True
            new_head.MyId = repo.new_id()

            # Now merge the state!
            self._update_repo_to_head(repo,new_head)

        # Put any new blobs
        def_list = []
        for key in new_blob_keys:

            element = self._workbench_cache.get(key)

            def_list.append(self._blob_store.put(key, element.serialize()))
        yield defer.DeferredList(def_list)
        # @TODO - check the results - for what?


        # now put any new commits that are not at the head
        def_list = []

        # list of the keys which are no longer heads
        clear_head_list=[]

        # list of the new heads to push at the same time
        new_head_list=[]
        for repo_key, commit_keys in new_commits.items():
            # Get the updated repository
            repo = self.workbench.get_repository(repo_key)

            # any objects in the data structure that were transmitted have already
            # been updated during fetch linked objects.

            #
            head_keys = []
            for cref in repo.current_heads():
                head_keys.append( cref.MyId )

            for key in commit_keys:

                # Set the repository name for the commit
                attributes = {REPOSITORY_KEY : str(repo_key)}

                cref = repo._commit_index.get(key)



                if cref.objectroot.ObjectType == ASSOCIATION_TYPE:
                    attributes[SUBJECT_KEY] = cref.objectroot.subject.key
                    attributes[SUBJECT_BRANCH] = cref.objectroot.subject.branch
                    attributes[SUBJECT_COMMIT] = cref.objectroot.subject.commit

                    attributes[PREDICATE_KEY] = cref.objectroot.predicate.key
                    attributes[PREDICATE_BRANCH] = cref.objectroot.predicate.branch
                    attributes[PREDICATE_COMMIT] = cref.objectroot.predicate.commit

                    attributes[OBJECT_KEY] = cref.objectroot.object.key
                    attributes[OBJECT_BRANCH] = cref.objectroot.object.branch
                    attributes[OBJECT_COMMIT] = cref.objectroot.object.commit

                elif  cref.objectroot.ObjectType == TERMINOLOGY_TYPE:
                    attributes[KEYWORD] = cref.objectroot.word

                # get the wrapped structure element to put in...
                wse = self._workbench_cache.get(key)


                if key not in head_keys:

                    defd = self._commit_store.put(key = key,
                                       value = wse.serialize(),
                                       index_attributes = attributes)
                    def_list.append(defd)

                else:

                    # We know it is a head - but we need to get the branch name again
                    for branch in  repo.branches:
                        # If this is currently the head commit - set the branch name attribute
                        if cref in branch.commitrefs:
                            # If this is currently the head commit - set
                            attributes[BRANCH_NAME] = branch.branchkey
                            break


                    new_head_list.append({'key':key, 'value':wse.serialize(), 'index_attributes':attributes})



            # Get the current head list
            q = Query()
            q.add_predicate_eq(REPOSITORY_KEY, repo_key)
            q.add_predicate_gt(BRANCH_NAME, '')

            rows = yield self._commit_store.query(q)

            for key in rows.keys():
                if key not in head_keys:
                    clear_head_list.append(key)


        yield defer.DeferredList(def_list)
        #@TODO - check the return vals?

        def_list = []
        for new_head in new_head_list:

            def_list.append(self._commit_store.put(**new_head))

        yield defer.DeferredList(def_list)
        #@TODO - check the return vals?

        def_list = []
        for key in clear_head_list:

            def_list.append(self._commit_store.update_index(key=key, index_attributes={BRANCH_NAME:''}))

        yield defer.DeferredList(def_list)
        
        response = yield self._process.message_client.create_instance(MessageContentTypeID=None)
        response.MessageResponseCode = response.ResponseCodes.OK

        # The following line shows how to reply to a message
        yield self._process.reply_ok(msg, response)
        log.info('op_push: Complete!')



    @defer.inlineCallbacks
    def op_fetch_blobs(self, request, headers, message):
        """
        Send the object back to a requester if you have it!
        @TODO Update to new message pattern!

        """
        log.info('op_fetch_blobs')

        if not hasattr(request, 'MessageType') or request.MessageType != BLOBS_REQUSET_MESSAGE_TYPE:
            raise WorkBenchError('Invalid fetch objects request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)

        response = yield self._process.message_client.create_instance(BLOBS_MESSAGE_TYPE)

        for key in request.blob_keys:
            element = self._workbench_cache.get(key)
            if element is None:
                raise WorkBenchError('Invalid fetch objects request. Key Not Found!', request.ResponseCodes.NOT_FOUND)

            link = response.blob_elements.add()
            obj = response.Repository._wrap_message_object(element._element)

            link.SetLink(obj)

        yield self._process.reply_ok(message, response)

        log.info('op_fetch_blobs: Complete!')



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


    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        ServiceProcess.__init__(self, *args, **kwargs)
            
        self._backend_cls_names = {}
        self._backend_cls_names[COMMIT_CACHE] = self.spawn_args.get(COMMIT_CACHE, CONF.getValue(COMMIT_CACHE, default='ion.core.data.store.IndexStore'))
        self._backend_cls_names[BLOB_CACHE] = self.spawn_args.get(BLOB_CACHE, CONF.getValue(BLOB_CACHE, default='ion.core.data.store.Store'))
            
        self._backend_classes={}

        self._backend_classes[COMMIT_CACHE] = pu.get_class(self._backend_cls_names[COMMIT_CACHE])
        assert store.IIndexStore.implementedBy(self._backend_classes[COMMIT_CACHE]), \
            'The back end class to store commit objects passed to the data store does not implement the required IIndexSTORE interface.'
            
        self._backend_classes[BLOB_CACHE] = pu.get_class(self._backend_cls_names[BLOB_CACHE])
        assert store.IStore.implementedBy(self._backend_classes[BLOB_CACHE]), \
            'The back end class to store blob objects passed to the data store does not implement the required ISTORE interface.'
            
        # Declare some variables to hold the store instances
        self.c_store = None
        self.b_store = None
            

        log.info('DataStoreService.__init__()')
        

    @defer.inlineCallbacks
    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        if issubclass(self._backend_classes[COMMIT_CACHE], cassandra.CassandraStore):
            raise NotImplementedError('Startup for cassandra store is not yet complete')
        else:

            self.c_store = yield defer.maybeDeferred(self._backend_classes[COMMIT_CACHE], self, **{'indices':COMMIT_INDEXED_COLUMNS})

        if issubclass(self._backend_classes[BLOB_CACHE], cassandra.CassandraStore):
            raise NotImplementedError('Startup for cassandra store is not yet complete')
        else:
            self.b_store = yield defer.maybeDeferred(self._backend_classes[BLOB_CACHE])
        


        self.workbench = DataStoreWorkbench()


    #@defer.inlineCallbacks
    def slc_activate(self):
       pass


        

    '''
    @defer.inlineCallbacks
    def op_push(self, heads, headers, msg):
        
        pushed_repos = {}
        
        for head in heads:
            
            # Extract the repository key from the mutable
            raw_mutable = object_utils.get_gpb_class_from_type_id(MUTABLE_TYPE)()
            raw_mutable.ParseFromString(head.value)
            repo_key = str(raw_mutable.repositorykey)
            
            # Get the mutable
            store_commits = {}

            # keep track of all the commits read during this push...
            pushed_repos[repo_key] = store_commits

            # Get the commits using the query interface
            q = Query()
            q.add_predicate_eq(REPOSITORY_KEY, repo_key)
            rows = yield self.c_store.query(q)

            if rows:

                stored_repo = self.workbench.create_repository(repository_key=repo_key)


            for key, columns in rows.items():
                blob = columns["value"]
                wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
                assert key == wse.key, 'Calculated key does not match the stored key!'
                store_commits[wse.key] = wse
                    
                    
                # Load these commits into the workbench
                self.workbench._hashed_elements.update(store_commits)


                # Check to make sure the mutable is upto date with the commits...
                #for commit_key in store_commits.keys():
                #    if not commit_key in repo._commit_index:
                #        raise DataStoreError('Can not handle divergence yet...')
            
            
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

                    # Set the repository name for the commit
                    attributes = {REPOSITORY_KEY : str(repo_key)}

                    cref = repo._commit_index.get(key)
                    
                    for branch in  repo.branches:
                        # If this is currently the head commit - set
                        if cref in branch.commitrefs:
                            attributes[BRANCH_NAME] = branch.branchkey
                                        
                    if cref.objectroot.ObjectType == ASSOCIATION_TYPE:
                        attributes[SUBJECT_KEY] = cref.objectroot.subject.key
                        attributes[SUBJECT_BRANCH] = cref.objectroot.subject.branch
                        attributes[SUBJECT_COMMIT] = cref.objectroot.subject.commit
                        
                        attributes[PREDICATE_KEY] = cref.objectroot.predicate.key
                        attributes[PREDICATE_BRANCH] = cref.objectroot.predicate.branch
                        attributes[PREDICATE_COMMIT] = cref.objectroot.predicate.commit

                        attributes[OBJECT_KEY] = cref.objectroot.object.key
                        attributes[OBJECT_BRANCH] = cref.objectroot.object.branch
                        attributes[OBJECT_COMMIT] = cref.objectroot.object.commit
                        
                    elif  cref.objectroot.ObjectType == TERMINOLOGY_TYPE:
                        attributes[KEYWORD] = cref.objectroot.word
                    
                    # get the wrapped structure element to put in...
                    wse = self.workbench._hashed_elements.get(key)
                    
                    # Should replace this with one put slice command
                    defd = self.c_store.put(key = key,
                                           value = wse.serialize(),
                                           index_attributes = attributes)
                    def_list.append(defd)
            
        yield defer.DeferredList(def_list)
            
        # Pretty useless to try and debug by reading, but its a start...   
        #print 'KVS: \n', self.c_store.kvs, '\n\n'
        
        #print 'Index: \n', self.c_store.indices, '\n\n'
            
        '''



            
    '''
        
    @defer.inlineCallbacks
    def op_pull(self, content, headers, msg):
        """
        Content is a string - the name of a mutable head for a repository
        """
        repo_key = str(content)
        
        store_commits ={}
        


        q = Query()
        q.add_predicate_eq(REPOSITORY_KEY, repo_key)
        q.add_predicate_gt(BRANCH_NAME, '')

        rows = yield self.c_store.query(q)

        for key, columns in rows.items():
            blob = columns["value"]
            wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
            assert key == wse.key, 'Calculated key does not match the stored key!'
            store_commits[wse.key] = wse
                    
                    
            # Load these commits into the workbench
            self.workbench._hashed_elements.update(store_commits)


            # Check to make sure the mutable is upto date with the commits...
            for commit_key in store_commits.keys():
                if not commit_key in repo._commit_index:
                    raise DataStoreError('Can not handle divergence yet...')
        
        yield self.workbench.op_pull(content, headers, msg)
    '''


    '''
    @defer.inlineCallbacks
    def op_fetch_linked_objects(self, elements, headers, message):
        """
        The data store is getting objects for another process...
        """
        def_list=[]
        # Elements is a dictionary of wrapped structure elements
        for se in elements.values():
            
            assert se.type == LINK_TYPE, 'This is not a link element!'
            link = object_utils.get_gpb_class_from_type_id(LINK_TYPE)()
            link.ParseFromString(se.value)
                
            # if it is already in memory, don't worry about it...
            if not link.key in self.workbench._hashed_elements:            
                if link.type == COMMIT_TYPE:
                    # Can get commits for a service in a fetch
                    def_list.append(self.c_store.get(link.key))
                else:
                    def_list.append(self.b_store.get(link.key))

        obj_list = yield defer.DeferredList(def_list)
        #print 'OBJECT LIST:', obj_list
            
        # Load this list of objects from the store into memory for use in the datastores workbench
        for result, blob in obj_list:
            wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
            self.workbench._hashed_elements[wse.key]=wse
            
        yield self.workbench.op_fetch_linked_objects(elements, headers, message)

    '''



    '''
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
                if link.type == COMMIT_TYPE:
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
            #if wse.type == COMMIT_TYPE:
            #    raise DataStoreError('Can not get commits in a fetch!')
            #    def_list.append(self.c_store.put(key, wse.serialize()))
            #else:
            #    def_list.append(self.b_store.put(key, wse.serialize()))
            
            # Don't ever put commits out of context. This is done by push!
            if wse.type != COMMIT_TYPE:
                def_list.append(self.b_store.put(key, wse.serialize()))
                
            # Add it to the dictionary of objects 
        
        obj_dict.update(got_objs)
        
        yield defer.DeferredList(def_list)
        
        defer.returnValue(obj_dict.values())
        
        '''
        


# Spawn of the process using the module name
factory = ProcessFactory(DataStoreService)


