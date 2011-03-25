#!/usr/bin/env python

"""
@file ion/services/coi/datastore.py
@author David Stuebe
@author Matt Rodriguez
@TODO

"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess
from ion.core.exception import ReceivedError, ApplicationError

from ion.services.coi.resource_registry_beta import resource_client

from types import FunctionType

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

from ion.core.data.storage_configuration_utility import KEYWORD, VALUE, RESOURCE_OBJECT_TYPE, RESOURCE_LIFE_CYCLE_STATE


from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_DATASETS, ION_PREDICATES, ION_RESOURCE_TYPES, ION_IDENTITIES
from ion.services.coi.datastore_bootstrap.ion_preload_config import ID_CFG, TYPE_CFG, PREDICATE_CFG, PRELOAD_CFG, NAME_CFG, DESCRIPTION_CFG, CONTENT_CFG
from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_PREDICATES_CFG, ION_DATASETS_CFG, ION_RESOURCE_TYPES_CFG, ION_IDENTITIES_CFG


from ion.core import ioninit
CONF = ioninit.config(__name__)


LINK_TYPE = object_utils.create_type_identifier(object_id=3, version=1)
COMMIT_TYPE = object_utils.create_type_identifier(object_id=8, version=1)
MUTABLE_TYPE = object_utils.create_type_identifier(object_id=6, version=1)
STRUCTURE_ELEMENT_TYPE = object_utils.create_type_identifier(object_id=1, version=1)

ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
TERMINOLOGY_TYPE = object_utils.create_type_identifier(object_id=14, version=1)

RESOURCE_TYPE = object_utils.create_type_identifier(object_id=1102, version=1)

class DataStoreWorkBenchError(WorkBenchError):
    """
    An Exception class for errors in the data store workbench
    """


class DataStoreWorkbench(WorkBench):


    def __init__(self, process, blob_store, commit_store):

        WorkBench.__init__(self, process)

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

        log.info('op_pull!')

        if not hasattr(request, 'MessageType') or request.MessageType != PULL_MESSAGE_TYPE:
            raise DataStoreWorkBenchError('Invalid pull request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)


        repo = self.get_repository(request.repository_key)
        if repo is None:
            #if it does not exist make a new one
            repo = repository.Repository(repository_key=request.repository_key)
            self.put_repository(repo)

        # Must reconstitute the head and merge with existing
        mutable_cls = object_utils.get_gpb_class_from_type_id(MUTABLE_TYPE)
        new_head = repo._wrap_message_object(mutable_cls(), addtoworkspace=False)
        new_head.repositorykey = request.repository_key


        q = Query()
        q.add_predicate_eq(REPOSITORY_KEY, request.repository_key)

        rows = yield self._commit_store.query(q)

        if len(rows) == 0:
            raise DataStoreWorkBenchError('Repository Key "%s" not found in Datastore' % request.repository_key, request.ResponseCodes.NOT_FOUND)


        for key, columns in rows.items():

            blob = columns[VALUE]
            wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
            repo.index_hash[key] = wse

            if columns[BRANCH_NAME]:
                # If this appears to be a head commit

                # Deal with the possiblity that more than one branch points to the same commit
                branch_names = columns[BRANCH_NAME].split(',')


                for name in branch_names:

                    for branch in new_head.branches:
                        # if the branch already exists in the new_head just add a commitref
                        if branch.branchkey == name:
                            link = branch.commitrefs.add()
                            break
                    else:
                        # If not add a new branch
                        branch = new_head.branches.add()
                        branch.branchkey = name
                        link = branch.commitrefs.add()

                    cref = repo._load_element(wse)
                    repo._commit_index[cref.MyId]=cref
                    cref.ReadOnly = True

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
                raise DataStoreWorkBenchError('Repository commit object not found in op_pull', request.ResponseCodes.NOT_FOUND)
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
                    repo.index_hash[wse.key] = wse

                    # load the object so we can find its children
                    obj = repo._load_element(wse)

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

        log.info('op_pull: Complete!')



    @defer.inlineCallbacks
    def op_push(self, pushmsg, headers, msg):
        """
        The Operation which responds to a push.

        Operation does not complete until transfer is complete!
        """
        log.info('op_push!')

        if not hasattr(pushmsg, 'MessageType') or pushmsg.MessageType != PUSH_MESSAGE_TYPE:
            raise DataStoreWorkBenchError('Invalid push request. Bad Message Type!', pushmsg.ResponseCodes.BAD_REQUEST)

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
                    raise DataStoreWorkBenchError('Requested push to a repository is in an invalid state: MODIFIED.', request.ResponseCodes.BAD_REQUEST)
                repo_keys = set(self.list_repository_blobs(repo))

            # Get the latest commits in the repository
            q = Query()
            q.add_predicate_eq(REPOSITORY_KEY, repostate.repository_key)

            rows = yield self._commit_store.query(q)
    
            for key, columns in rows.items():

                blob = columns[VALUE]
                wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
                if wse.key in repo._commit_index.keys():
                    # No thanks, he's already got one!
                    continue

                repo.index_hash[key] = wse

                if columns[BRANCH_NAME]:
                    # If this appears to be a head commit

                    # Deal with the possibility that more than one branch points to the same commit
                    branch_names = columns[BRANCH_NAME].split(',')

                    for name in branch_names:

                        for branch in repo.branches:
                            # if the branch already exists in the new_head just add a commitref
                            if branch.branchkey == name:
                                link = branch.commitrefs.add()
                                #  Link is set below...
                                break
                        else:
                            # If not add a new branch
                            branch = repo._dotgit.branches.add()
                            branch.branchkey = name
                            link = branch.commitrefs.add()
                            # Link is set below...

                        cref = repo._load_element(wse)
                        repo._commit_index[cref.MyId]=cref
                        cref.ReadOnly = True

                        link.SetLink(cref)

            # Now the repo is up to date on the data store side...



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
                   raise DataStoreWorkBenchError('Fetch Objects returned an exception! "%s"' % re.msg_content)


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
            repo = self.get_repository(repo_key)

            # any objects in the data structure that were transmitted have already
            # been updated now it is time to set update the commits
            #

            branch_names = []
            for branch in repo.branches:
                branch_names.append(branch.branchkey)

            head_keys = []
            for cref in repo.current_heads():
                head_keys.append( cref.MyId )

            for key in commit_keys:

                # Set the repository name for the commit
                attributes = {REPOSITORY_KEY : str(repo_key)}
                # Set a default branch name to empty
                attributes[BRANCH_NAME] = ''

                cref = repo._commit_index.get(key)

                # it may not have been loaded during the update process - if not load it now.
                if not cref:
                    element = repo.index_hash.get(key)
                    cref = repo._load_element(element)

                link = cref.GetLink('objectroot')
                # Extract the GPB Message for comparison with type objects!
                root_type = link.type.GPBMessage


                if root_type == ASSOCIATION_TYPE:

                    attributes[SUBJECT_KEY] = cref.objectroot.subject.key
                    attributes[SUBJECT_BRANCH] = cref.objectroot.subject.branch
                    attributes[SUBJECT_COMMIT] = cref.objectroot.subject.commit

                    attributes[PREDICATE_KEY] = cref.objectroot.predicate.key
                    attributes[PREDICATE_BRANCH] = cref.objectroot.predicate.branch
                    attributes[PREDICATE_COMMIT] = cref.objectroot.predicate.commit

                    attributes[OBJECT_KEY] = cref.objectroot.object.key
                    attributes[OBJECT_BRANCH] = cref.objectroot.object.branch
                    attributes[OBJECT_COMMIT] = cref.objectroot.object.commit

                elif root_type == RESOURCE_TYPE:


                    attributes[RESOURCE_OBJECT_TYPE] = cref.objectroot.type.object_id
                    attributes[RESOURCE_LIFE_CYCLE_STATE] = cref.objectroot.lcs


                elif  root_type == TERMINOLOGY_TYPE:
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
                            # If this is currently the head commit - set the branch name
                            if attributes[BRANCH_NAME] == '':
                                attributes[BRANCH_NAME] = branch.branchkey
                            else:
                                attributes[BRANCH_NAME] = ','.join([attributes[BRANCH_NAME],branch.branchkey])



                    new_head_list.append({'key':key, 'value':wse.serialize(), 'index_attributes':attributes})

            # Get the current head list
            q = Query()
            q.add_predicate_eq(REPOSITORY_KEY, repo_key)
            q.add_predicate_gt(BRANCH_NAME, '')

            rows = yield self._commit_store.query(q)

            for key, columns in rows.items():
                if key not in head_keys:
                    clear_head_list.append(key)

                    # Any commit which is currently a head will have the correct branch names set.
                    # Just delete the branch names for the ones that are no longer heads.

        yield defer.DeferredList(def_list)
        #@TODO - check the return vals?

        def_list = []
        for new_head in new_head_list:

            #print 'Setting New Head:', new_head
            def_list.append(self._commit_store.put(**new_head))

        yield defer.DeferredList(def_list)
        #@TODO - check the return vals?

        def_list = []
        for key in clear_head_list:

            def_list.append(self._commit_store.update_index(key=key, index_attributes={BRANCH_NAME:''}))

        yield defer.DeferredList(def_list)

        #import pprint
        #print 'After update to heads'
        #pprint.pprint(self._commit_store.kvs)


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
            raise DataStoreWorkBenchError('Invalid fetch objects request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)

        response = yield self._process.message_client.create_instance(BLOBS_MESSAGE_TYPE)

        def_list = []
        for key in request.blob_keys:
            element = self._workbench_cache.get(key)

            if element is not None:
                link = response.blob_elements.add()
                obj = response.Repository._wrap_message_object(element._element)

                link.SetLink(obj)

                continue

            def_list.append(self._blob_store.get(key))

        res_list = yield defer.DeferredList(def_list)

        for result, blob in res_list:

            if blob is None:
                raise DataStoreWorkBenchError('Invalid fetch objects request. Key Not Found!', request.ResponseCodes.NOT_FOUND)

            element = gpb_wrapper.StructureElement.parse_structure_element(blob)
            link = response.blob_elements.add()
            obj = response.Repository._wrap_message_object(element._element)

            link.SetLink(obj)
            


        yield self._process.reply_ok(message, response)

        log.info('op_fetch_blobs: Complete!')

    @defer.inlineCallbacks
    def flush_initialization_to_backend(self):
        """
        Flush any repositories in the backend to the the workbench backend storage
        """

        # Put any blobs from the workbench
        #@TODO - only put the blobs - not the commits or the heads...
        def_list = []
        for key, element in self._workbench_cache.items():

            def_list.append(self._blob_store.put(key, element.serialize()))
        yield defer.DeferredList(def_list)
        # @TODO - check the results - for what?


        # This is simpler than a push - all of these are guaranteed to be new objects!
        # now put any new commits that are not at the head
        def_list = []


        for repo_key, repo in self._repos.items():

            # any objects in the data structure that were transmitted have already
            # been updated now it is time to set update the commits
            #

            commit_keys = repo._commit_index.keys()


            branch_names = []
            for branch in repo.branches:
                branch_names.append(branch.branchkey)

            head_keys = []
            for cref in repo.current_heads():
                head_keys.append( cref.MyId )

            for key in commit_keys:

                # Set the repository name for the commit
                attributes = {REPOSITORY_KEY : str(repo_key)}
                # Set a default branch name to empty
                attributes[BRANCH_NAME] = ''

                cref = repo._commit_index.get(key)

                link = cref.GetLink('objectroot')
                root_type = link.type


                if root_type == ASSOCIATION_TYPE:
                    attributes[SUBJECT_KEY] = cref.objectroot.subject.key
                    attributes[SUBJECT_BRANCH] = cref.objectroot.subject.branch
                    attributes[SUBJECT_COMMIT] = cref.objectroot.subject.commit

                    attributes[PREDICATE_KEY] = cref.objectroot.predicate.key
                    attributes[PREDICATE_BRANCH] = cref.objectroot.predicate.branch
                    attributes[PREDICATE_COMMIT] = cref.objectroot.predicate.commit

                    attributes[OBJECT_KEY] = cref.objectroot.object.key
                    attributes[OBJECT_BRANCH] = cref.objectroot.object.branch
                    attributes[OBJECT_COMMIT] = cref.objectroot.object.commit

                elif root_type == RESOURCE_TYPE:


                    attributes[RESOURCE_OBJECT_TYPE] = cref.objectroot.type.object_id
                    attributes[RESOURCE_LIFE_CYCLE_STATE] = cref.objectroot.lcs


                elif  root_type == TERMINOLOGY_TYPE:
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
                            # If this is currently the head commit - set the branch name
                            if attributes[BRANCH_NAME] == '':
                                attributes[BRANCH_NAME] = branch.branchkey
                            else:
                                attributes[BRANCH_NAME] = ','.join([attributes[BRANCH_NAME],branch.branchkey])


                    # Now commit it!
                    defd = self._commit_store.put(key = key,
                                       value = wse.serialize(),
                                       index_attributes = attributes)
                    def_list.append(defd)

        yield defer.DeferredList(def_list)

        # Now clear the in memory workbench
        self.clear_non_persistent()


    @defer.inlineCallbacks
    def test_existence(self,repo_key):
        """
        For use in initialization - test to see if the repository already exists in the backend
        """

        q = Query()
        q.add_predicate_eq(REPOSITORY_KEY, repo_key)

        rows = yield self._commit_store.query(q)

        defer.returnValue(len(rows)>0)





class DataStoreError(ApplicationError):
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


        # Get the arguments for preloading the datastore
        self.preload = {ION_PREDICATES_CFG:True,
                        ION_RESOURCE_TYPES_CFG:True,
                        ION_IDENTITIES_CFG:True,
                        ION_DATASETS_CFG:False,}

        self.preload.update(CONF.getValue(PRELOAD_CFG, default={}))
        self.preload.update(self.spawn_args.get(PRELOAD_CFG, {}))



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
            self.b_store = yield defer.maybeDeferred(self._backend_classes[BLOB_CACHE], self)


        self.workbench = DataStoreWorkbench(self, self.b_store, self.c_store)


        yield self.initialize_datastore()


    #@defer.inlineCallbacks
    def slc_activate(self):


        self.op_fetch_blobs = self.workbench.op_fetch_blobs
        self.op_pull = self.workbench.op_pull
        self.op_push = self.workbench.op_push



    @defer.inlineCallbacks
    def initialize_datastore(self):
        """
        This method is used to preload required content into the datastore
        """


        if self.preload[ION_PREDICATES_CFG]:

            log.info('Preloading Predicates')
            for key, value in ION_PREDICATES.items():

                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    self._create_predicate(value)


        if self.preload[ION_RESOURCE_TYPES_CFG]:
            log.info('Preloading Resource Types')

            for key, value in ION_RESOURCE_TYPES.items():

                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    self._create_resource(value)


        if self.preload[ION_IDENTITIES_CFG]:
            log.info('Preloading Identities')

            for key, value in ION_IDENTITIES.items():
                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    self._create_resource(value)

        if self.preload[ION_DATASETS_CFG]:
            log.info('Preloading Data')

            for key, value in ION_DATASETS.items():
                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    self._create_resource(value)

        yield self.workbench.flush_initialization_to_backend()



    def _create_predicate(self,description):

        predicate_type = description[TYPE_CFG]
        predicate_key = description[ID_CFG]
        predicate_word = description[PREDICATE_CFG]


        predicate_repository = self.workbench.create_repository(root_type=predicate_type, repository_key=predicate_key)
        predicate = predicate_repository.root_object
        predicate.word = predicate_word

        predicate_repository.commit('Predicate instantiated by datastore bootstrap')



    def _create_resource(self, description):
        """
        Helper method to create resource objects during initialization
        """

        resource_key = description[ID_CFG]
        # Create this resource with a constant ID from the config file
        resource_repository = self.workbench.create_repository(root_type=RESOURCE_TYPE, repository_key=resource_key)
        resource = resource_repository.root_object

        assert resource_repository.repository_key == resource_key, 'Failure in repository creation!'
        # Set the identity of the resource
        resource.identity = resource_repository.repository_key

        # Create the new resource object
        res_obj = resource_repository.create_object(description[TYPE_CFG])

        # Set the object as the child of the resource
        resource.resource_object = res_obj

        # Name and Description is set by the resource client
        resource.name = description[NAME_CFG]
        resource.description = description[DESCRIPTION_CFG]

        object_utils.set_type_from_obj(res_obj, description[TYPE_CFG])

        # State is set to new by default
        resource.lcs = resource.LifeCycleState.NEW

        resource_instance = resource_client.ResourceInstance(resource_repository)

        # Set the content
        content = description[CONTENT_CFG]
        if isinstance(content, dict):
            # If it is a dictionary, set the content of the resource
            for k,v in content.items():
                setattr(resource_instance,k,v)

        elif isinstance(content, FunctionType):
            #execute the function on the resource_instance!
            content(resource_instance)


        resource_instance.Repository.commit('Resource instantiated by datastore bootstrap')

        return resource_instance




# Spawn of the process using the module name
factory = ProcessFactory(DataStoreService)


