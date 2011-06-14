#!/usr/bin/env python

"""
@file ion/services/coi/datastore.py
@author David Stuebe
@author Matt Rodriguez
@author Dave Foster <dfoster@asascience.com>
@TODO Deal with preload issue - must be possible to run preload with partial datastore. IE if predicates are already
there but resource types are not...

"""
import math
from ion.core.object.object_utils import CDM_ARRAY_INT32_TYPE, CDM_ARRAY_INT64_TYPE, CDM_ARRAY_UINT64_TYPE, CDM_ARRAY_FLOAT32_TYPE, CDM_ARRAY_FLOAT64_TYPE, CDM_ARRAY_STRING_TYPE, CDM_ARRAY_OPAQUE_TYPE, CDM_ARRAY_UINT32_TYPE, ARRAY_STRUCTURE_TYPE

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.exception import ReceivedError, ApplicationError

from ion.services.coi.resource_registry import resource_client

from types import FunctionType

from ion.core.object import object_utils
from ion.core.object import gpb_wrapper, repository
from ion.core.object.workbench import WorkBench, WorkBenchError, PUSH_MESSAGE_TYPE, PULL_MESSAGE_TYPE, PULL_RESPONSE_MESSAGE_TYPE, BLOBS_REQUSET_MESSAGE_TYPE, REQUEST_COMMIT_BLOBS_MESSAGE_TYPE, BLOBS_MESSAGE_TYPE, GET_OBJECT_REQUEST_MESSAGE_TYPE, GET_OBJECT_REPLY_MESSAGE_TYPE, GPBTYPE_TYPE, DATA_REQUEST_MESSAGE_TYPE, DATA_REPLY_MESSAGE_TYPE, DATA_CHUNK_MESSAGE_TYPE
from ion.core.data import store
from ion.core.data import cassandra
#from ion.core.data import cassandra_bootstrap
from ion.core.data.store import Query


from ion.core.data.storage_configuration_utility import BLOB_CACHE, COMMIT_CACHE
from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS
from ion.core.data.storage_configuration_utility import REPOSITORY_KEY, BRANCH_NAME

from ion.core.data.storage_configuration_utility import SUBJECT_KEY, SUBJECT_BRANCH, SUBJECT_COMMIT
from ion.core.data.storage_configuration_utility import PREDICATE_KEY, PREDICATE_BRANCH, PREDICATE_COMMIT
from ion.core.data.storage_configuration_utility import OBJECT_KEY, OBJECT_BRANCH, OBJECT_COMMIT, STORAGE_PROVIDER, PERSISTENT_ARCHIVE

from ion.core.data.storage_configuration_utility import KEYWORD, VALUE, RESOURCE_OBJECT_TYPE, RESOURCE_LIFE_CYCLE_STATE, get_cassandra_configuration


from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_DATASETS, ION_PREDICATES, ION_RESOURCE_TYPES, ION_IDENTITIES, ION_DATA_SOURCES
from ion.services.coi.datastore_bootstrap.ion_preload_config import ID_CFG, TYPE_CFG, PREDICATE_CFG, PRELOAD_CFG, NAME_CFG, DESCRIPTION_CFG, CONTENT_CFG, CONTENT_ARGS_CFG, LCS_CFG, COMMISSIONED
from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_PREDICATES_CFG, ION_DATASETS_CFG, ION_RESOURCE_TYPES_CFG, ION_IDENTITIES_CFG, root_name, HAS_A_ID

from ion.services.coi.datastore_bootstrap.ion_preload_config import TypeMap, ANONYMOUS_USER_ID, ROOT_USER_ID, OWNED_BY_ID, ION_AIS_RESOURCES, ION_AIS_RESOURCES_CFG, OWNER_ID

from ion.core import ioninit
CONF = ioninit.config(__name__)


LINK_TYPE = object_utils.create_type_identifier(object_id=3, version=1)
COMMIT_TYPE = object_utils.create_type_identifier(object_id=8, version=1)
MUTABLE_TYPE = object_utils.create_type_identifier(object_id=6, version=1)
STRUCTURE_ELEMENT_TYPE = object_utils.create_type_identifier(object_id=1, version=1)

ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
TERMINOLOGY_TYPE = object_utils.create_type_identifier(object_id=14, version=1)
IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)

RESOURCE_TYPE = object_utils.create_type_identifier(object_id=1102, version=1)

CDM_BOUNDED_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10021, version=1)


class DataStoreWorkBenchError(WorkBenchError):
    """
    An Exception class for errors in the data store workbench
    """

class DataStoreWorkbench(WorkBench):


    def __init__(self, process, blob_store, commit_store, cache_size=10**8):

        WorkBench.__init__(self, process, cache_size)

        self._blob_store = blob_store
        self._commit_store = commit_store


    def pull(self, *args, **kwargs):

        raise NotImplementedError("The Datastore Service can not Pull")

    def push(self, *args, **kwargs):

        raise NotImplementedError("The Datastore Service can not Push")

    @defer.inlineCallbacks
    def _get_blobs(self, repo, startkeys, filtermethod=None):
        """
        Common blob fetching helper method.
        Used by checkout and pull.

        @param  repo            Repository for the response.
        @param  startkeys       The keys that should start the fetching process.
        @param  filtermethod    A callable to be applied to all children of fetched items. If the callable returns true,
                                the item is included.

        @returns                A dictionary of keys => blobs.
        """
        # Slightly different machinary here than in the workbench - Could be made more similar?
        blobs={}
        keys_to_get=set(startkeys)
        def_filter = lambda x: True
        filtermethod = filtermethod or def_filter

        while len(keys_to_get) > 0:
            new_links_to_get = set()

            def_list = []
            #@TODO - put some error checking here so that we don't overflow due to a stupid request!
            for key in keys_to_get:
                # Short cut if we have already got it!
                wse = repo.index_hash.get(key)

                if wse:
                    blobs[wse.key]=wse
                    # get the object
                    obj = repo._load_element(wse)

                    # only add new items to get if they meet our criteria, meaning they are not in the excluded type list
                    new_links_to_get.update(obj.ChildLinks)
                else:
                    def_list.append(self._blob_store.get(key))


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

            keys_to_get.clear()
            for link in new_links_to_get:
                if not blobs.has_key(link.key) and filtermethod(link):
                    keys_to_get.add(link.key)

        defer.returnValue(blobs)

    @defer.inlineCallbacks
    def _resolve_repo_state(self, repository_key, fail_if_not_found=True):
        """
        @returns Repo.
        """

        log.info('_resolve_repo_state: start')

        repo = self.get_repository(repository_key)
        if repo is None:
            #if it does not exist make a new one
            log.debug('Repository is not loaded - get it from the persistent store')

            repo = repository.Repository(repository_key=repository_key)
            self.put_repository(repo)
        else:
            log.debug('Repository is loaded - merge it with the state in the persistent store')


        # Must reconstitute the head and merge with existing
        mutable_cls = object_utils.get_gpb_class_from_type_id(MUTABLE_TYPE)
        new_head = repo._wrap_message_object(mutable_cls(), addtoworkspace=False)
        new_head.repositorykey = repository_key

        q = Query()
        q.add_predicate_eq(REPOSITORY_KEY, repository_key)

        rows = yield self._commit_store.query(q)

        if fail_if_not_found and len(rows) == 0:
            self.clear_repository(repo)
            raise DataStoreWorkBenchError('Repository Key "%s" not found in Datastore' % repository_key, 404)   # @TODO: constant

        log.debug('Found %d commits in the store' % len(rows))

        for key, columns in rows.items():

            if key not in repo.index_hash:
                blob = columns[VALUE]
                wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
                repo.index_hash[key] = wse
            else:
                wse = repo.index_hash.get(key)


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

                    if key not in repo._commit_index:
                        cref = repo._load_element(wse)

                        ### DO NOT ADD IT TO THE COMMIT INDEX - THE STATE OF THE COMMIT INDEX IS USED IN UPDATING TO THE HEAD!
                        #repo._commit_index[cref.MyId]=cref
                        cref.ReadOnly = True
                    else:
                        cref = repo._commit_index.get(key)
                        
                    link.SetLink(cref)
                    link.isleaf=False

                # Check to make sure the mutable is upto date with the commits...

        # Do the update!
        self._update_repo_to_head(repo, new_head)


        log.info('_resolve_repo_state: complete')

        # return repository
        defer.returnValue(repo)

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

        repo = yield self._resolve_repo_state(request.repository_key)
        repo.cached = True

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

            keys = [x.GetLink('objectroot').key for x in repo.current_heads()]


            def filtermethod(x):
                """
                Returns true if the passed in link's type is not in the excluded_types list of the passed in message.
                """
                return (x.type not in request.excluded_types)

            blobs = yield self._get_blobs(response.Repository, keys, filtermethod)

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


            repo = yield self._resolve_repo_state(repostate.repository_key, fail_if_not_found=False)
            repo.cached = True

            """
            repo = self.get_repository(repostate.repository_key)
            if repo is None:
                #if it does not exist make a new one
                repo = repository.Repository(repository_key=repostate.repository_key, cached=True)
                self.put_repository(repo)
                repo_keys=set()
            else:

                if repo.status == repo.MODIFIED:
                    raise DataStoreWorkBenchError('Requested push to a repository is in an invalid state: MODIFIED.', pushmsg.ResponseCodes.BAD_REQUEST)
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
            """


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


                    attributes[RESOURCE_OBJECT_TYPE] = cref.objectroot.resource_type.key
                    attributes[RESOURCE_LIFE_CYCLE_STATE] = str(cref.objectroot.lcs)


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
    def op_put_blobs(self, request, headers, message):
        log.info("op_put_blobs")
        if not hasattr(request, 'MessageType') or request.MessageType != BLOBS_MESSAGE_TYPE:
            raise DataStoreWorkBenchError('Invalid put blobs request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)

        def_list = []
        for blob in request.blob_elements:
            def_list.append(self._blob_store.put(blob.key, blob.SerializeToString() ))

        yield defer.DeferredList(def_list)

        yield self._process.reply_ok(message)
        log.info("op_put_blobs: Complete!")

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
        def_list=[]
        for repo in self._repos.itervalues():

            def_list.append(self.flush_repo_to_backend(repo))

        yield defer.DeferredList(def_list)

        #import pprint
        #print 'After update to heads'
        #pprint.pprint(self._commit_store.kvs)
        log.info("Number of repositories:  %s" % len(self._repos))
        log.info("Number of blobs: %s " % len(self._workbench_cache))
        
        num_commit_keys = map(lambda repo: len(repo._commit_index.keys()), self._repos.values())
        log.info("Number of commits: %s " % sum(num_commit_keys))

        # Now clear the in memory workbench
        self.clear()



    def flush_repo_to_backend(self, repo):
        """
        Flush any repositories in the backend to the the workbench backend storage
        """

        # This is simpler than a push - all of these are guaranteed to be new objects!
        def_list = []
        for key, element in repo.index_hash.items():

            def_list.append(self._blob_store.put(key, element.serialize()))


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
            attributes = {REPOSITORY_KEY : str(repo.repository_key)}
            # Set a default branch name to empty
            attributes[BRANCH_NAME] = ''

            cref = repo._commit_index.get(key)

            link = cref.GetLink('objectroot')
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

                attributes[RESOURCE_OBJECT_TYPE] = cref.objectroot.resource_type.key
                attributes[RESOURCE_LIFE_CYCLE_STATE] = str(cref.objectroot.lcs)


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
        return defer.DeferredList(def_list)




    @defer.inlineCallbacks
    def test_existence(self,repo_key):
        """
        For use in initialization - test to see if the repository already exists in the backend
        """

        q = Query()
        q.add_predicate_eq(REPOSITORY_KEY, repo_key)

        rows = yield self._commit_store.query(q)

        defer.returnValue(len(rows)>0)

    @defer.inlineCallbacks
    def op_extract_data(self, request, headers, message):
        """
        DataRequestMessage / DataReplyMessage
        """
        log.info("op_extract_data")

        if not hasattr(request, 'MessageType') or request.MessageType != DATA_REQUEST_MESSAGE_TYPE:
            raise DataStoreWorkBenchError('Invalid extract_data request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)

        # verify there are no 0 strides in the request
        if 0 in [x.stride for x in request.request_bounds if x.IsFieldSet('stride')]:   # the if makes it so that unset strides don't even get in the list
            raise DataStoreWorkBenchError('Stride of 0 specified in request_bounds!')

        response = yield self._process.message_client.create_instance(DATA_REPLY_MESSAGE_TYPE)

        log.debug("Extract data request bounds: %s", ["%d+%d,%d" % (x.origin, x.size, x.stride) for x in request.request_bounds])

        CHUNK_FACTOR = 10000

        # create an anonymous repo to load things into
        repo = self.create_repository(root_type=ARRAY_STRUCTURE_TYPE)

        filterlist = [CDM_ARRAY_INT32_TYPE,
                      CDM_ARRAY_UINT32_TYPE,
                      CDM_ARRAY_INT64_TYPE,
                      CDM_ARRAY_UINT64_TYPE,
                      CDM_ARRAY_FLOAT32_TYPE,
                      CDM_ARRAY_FLOAT64_TYPE,
                      CDM_ARRAY_STRING_TYPE,
                      CDM_ARRAY_OPAQUE_TYPE]

        # get some blobs into the repo
        blobs = yield self._get_blobs(repo, [request.structure_array_ref], lambda x: x not in filterlist)
        repo.index_hash.update(blobs)

        # get element pointed to by key
        se = repo.index_hash[request.structure_array_ref]
        assert se
        obj = repo._load_element(se)

        repo.load_links(obj, filterlist)

        # update repository's excluded_types
        for extype in filterlist:
            if extype not in repo.excluded_types:
                repo.excluded_types.append(extype)

        # now onto the fun.  let's traverse all the bounded arrays we find!

        log.debug("op_extract_data: obj has %d bounded arrays" % len(obj.bounded_arrays))

        # get the type of bounded array we have here
        assert len(obj.bounded_arrays) > 0

        # a list of matching bounded arrays
        bounded_includes_list = []
        targetshape = [x.size for x in request.request_bounds]

        # ===================================================================
        # STEP 1: Match bounded arrays
        # ===================================================================

        # iterate bounded arrays in this object
        for ba in obj.bounded_arrays:

            # need to be the same rank
            if not len(ba.bounds) == len(request.request_bounds):
                raise DataStoreWorkBenchError("Bounds dimensionality mismatch: this ba has %d dims, our request has %d" % (len(ba.bounds), len(request.request_bounds)))

            target_range = []
            src_range = []

            # this for loop is doing a few things:
            # - checking to see if the ranges intersect for each dimension.  If one does not intersect, the whole array
            #   is rejected.
            # - computing the intersection slices for each of those dimensions if they do intersect.
            # - if we make it through the for without a rejection on a dimension, the else: clause is run, which marks
            #   an array as being a required to copy array along with the ranges in both target and source.
            for reqbounds, babounds in zip(request.request_bounds, ba.bounds):

                #log.debug("Cur bounds: %d+%d, Req bounds: %d+%d" % (babounds.origin, babounds.size, reqbounds.origin, reqbounds.size))

                # this bounds is below our target range
                # requested end is lower than this bounds start (exclusive)
                if reqbounds.origin + reqbounds.size <= babounds.origin:
                    break

                # this bounds is above our target range
                # requested start is higher than this bounds end (exclusive)
                if reqbounds.origin >= babounds.origin + babounds.size:
                    break

                # compute intersections and offsets into request and src bounded arrays
                isec_start = max(babounds.origin, reqbounds.origin)
                isec_end = min(babounds.origin + babounds.size, reqbounds.origin + reqbounds.size)

                effective_start = isec_start - reqbounds.origin
                effective_end = isec_end - reqbounds.origin

                ba_start = isec_start - babounds.origin
                ba_end = isec_end - babounds.origin

                # append those intersections - each entry represents a range into a dimension
                target_range.append((effective_start, effective_end))
                src_range.append((ba_start, ba_end))

            else:
                # all bounds are included, this bounded array is good to go
                # format: (bounded array, target range of data (multidim), source bounded array range (multidim))
                bounded_includes_list.append((ba, target_range, src_range))

        # ===================================================================
        # STEP 2: Compress/Optimize bounded_includes_list for overlap
        # ===================================================================
        # @TODO: not needed for R1

        # ===================================================================
        # STEP 3: Generate a list of matching strips from each BA
        # ===================================================================
        striplist = []
        strides = [x.stride or 1 for x in request.request_bounds]

        for batuple in bounded_includes_list:
            ba, targetranges, srcranges = batuple

            # is this a scalar? is there anything to slice?
            if len(targetshape) == 0:
                striplist.append((ba, (0, 1), (0, 1), 1, 1))
            else:
                # get slices out of it
                # get dims of this bounded array
                ba_shape = [x.size for x in ba.bounds]

                for targetslice, srcslice, laststridelen in self._get_slices(targetshape, ba_shape, targetranges, srcranges, strides):
                    striplist.append((ba, targetslice, srcslice, targetslice[1]-targetslice[0], laststridelen))

        def debug_print_strip_list(lst):
            import base64
            for x in lst:
                ba, targetslice, srcslice, leng, laststridelen = x
                print "ba: %s  [%d:%d] -> [%d:%d], stride %d, len %d/%d=%d" % (base64.encodestring(ba.MyId)[0:6],
                                                              srcslice[0],
                                                              srcslice[1],
                                                              targetslice[0],
                                                              targetslice[1],
                                                              laststridelen,
                                                              leng,
                                                              laststridelen,
                                                              leng/laststridelen)

        log.debug("Number of uncompressed strips: %d" % len(striplist))

        # ===================================================================
        # STEP 4: Sort that list of matching strips by start index in target array to start index + length
        # ===================================================================

        sorted_striplist = sorted(striplist, lambda x, y: x[1][1] < y[1][0])

        # ===================================================================
        # STEP 5: Compress any contiguous strips from the same BAs
        # ===================================================================
        compressed_striplist = []
        accumstrip = None
        for stripitem in sorted_striplist:
            ba, targetslice, srcslice, leng, laststridelen = stripitem

            # check for end condition
            if accumstrip and ba != accumstrip[0]:
                # push current strip into compressed striplist
                compressed_striplist.append(accumstrip)
                accumstrip = None

            # brand new strip, either after we just pushed or starting this loop
            if accumstrip is None:
                accumstrip = stripitem[:]
                continue

            # see if we have a contiguous strip
            # - current item's source slice start index is the same as the accumulated strip's source end index
            # - strides are the same (should always be, this is more sanity check)
            # - the possible accumulated strip's total length is over the max size for chunking messages
            if srcslice[0] == accumstrip[2][1] and \
               laststridelen == accumstrip[4] and \
               accumstrip[3] + leng < CHUNK_FACTOR:
                # update accumstrip
                accumstrip = (accumstrip[0], (accumstrip[1][0], targetslice[1]), (accumstrip[2][0], srcslice[1]), srcslice[1] - accumstrip[2][0], accumstrip[4])
                #log.debug("accumstrip now [%d,%d] -> [%d,%d]" % (accumstrip[1][0], accumstrip[1][1], accumstrip[2][0], accumstrip[2][1]))
            else:
                # not contiguous?  push into list and forget it
                compressed_striplist.append(accumstrip)
                accumstrip = stripitem[:]

        # catch the last one
        if accumstrip is not None:
            compressed_striplist.append(accumstrip)

        log.debug("Number of compressed strips: %d" % len(compressed_striplist))
        #debug_print_strip_list(compressed_striplist)

        # ===================================================================
        # STEP 6: Generate a list of extractions using heuristics, an "extraction plan"
        # ===================================================================

        # list of [list of indicies into compressed_striplist]
        extraction_plan = []

        # simple: relying on the LRU cache to free up BAs when done, and that datasets will be laid out in a sane
        # variety fastest varying dimension will never be broken up over multiple BAs unless dimensionality is one,
        # we can just assemble each step of the plan to be the maximum chunk size we can fit.
        curstep = []
        curlen = 0
        for csidx, cstrip in enumerate(compressed_striplist):

            # always need at least one strip
            if len(curstep) == 0:
                curstep.append(csidx)
                curlen += cstrip[3]
                continue

            # can we fit the new strip?
            if curlen + cstrip[3] <= CHUNK_FACTOR:
                curstep.append(csidx)
            else:
                extraction_plan.append(curstep)
                curstep = [csidx]
                curlen = cstrip[3]

        # catch any leftovers
        if len(curstep) > 0:
            extraction_plan.append(curstep)

        log.debug("EXPLAN LENGTH: %d" % len(extraction_plan))

        # ===================================================================
        # STEP 7: Perform extractions
        # ===================================================================

        loaded_ba_ndarrays = set()
        loaded_ba_ndarray_objs = {}     # key => actual ndarray object
        for exidx, exstep in enumerate(extraction_plan):
            curstrips = []
            for sidx in exstep:
                curstrips.append(compressed_striplist[sidx])

            # get the start index.. should be in the first item
            targetstartidx = curstrips[0][1][0]

            # just to make sure..
            tsi = min([x[1][0] for x in curstrips])
            assert tsi==targetstartidx

            log.debug("extraction step %d, # strips: %d" % (exidx, len(curstrips)))

            # calculate number of elements we are going to output in this chunk, create temp storage for it
            #elemcount = reduce(lambda x,y: x+y, [int(math.floor(x[3] / float(x[4]))) for x in curstrips])   # divide stored length by stride to get true number of elems
            elemcount = reduce(lambda x, y: x+y, [x[3] for x in curstrips])
            targetndarray = [None] * elemcount

            log.debug("created target array of length %d" % elemcount)

            # needed BAs
            needed_ba_ndarrays = [x[0].GetLink('ndarray').key for x in curstrips]

            # get a list of things to load
            toload_ba_ndarrays = set([x for x in needed_ba_ndarrays if x not in loaded_ba_ndarrays])

            if len(toload_ba_ndarrays) > 0:
                import base64
                log.debug("need to load %s" % ",".join([base64.encodestring(x)[0:6] for x in toload_ba_ndarrays]))
    
                # physically load them!
                ndblobs = yield self._get_blobs(repo, toload_ba_ndarrays, lambda x: True)
                repo.index_hash.update(ndblobs)

                # keep track of what we've loaded
                loaded_ba_ndarrays.update(toload_ba_ndarrays)

                # load them as objects now
                for ndkey in toload_ba_ndarrays:
                    loaded_ba_ndarray_objs[ndkey] = repo._load_element(repo.index_hash[ndkey])

            # ok, now we can perform the extractions on this step
            for csidx, curstrip in enumerate(curstrips):
                ba, targetidxs, srcidxs, leng, stride = curstrip

                # index into the current chunk data
                striplen = leng #int(leng / float(stride))
                targetoffset = csidx * striplen

                ndobj = loaded_ba_ndarray_objs[ba.GetLink('ndarray').key]

                srcslice = ndobj.value[srcidxs[0]:srcidxs[1]]
                if stride == 1:
                    targetslice = srcslice
                else:
                    targetslice = [d for i, d in enumerate(srcslice) if i % stride == 0]

                log.debug("targetslice len: %d [%d, %d]" % (len(targetslice), targetoffset, targetoffset+striplen))

                targetndarray[targetoffset:targetoffset+striplen] = targetslice

            # ensure we filled this chunk
            nonelist = [i for i,d in enumerate(targetndarray) if d is None]
            assert len(nonelist) == 0

            # SEND THIS CHUNK

            # create new message to send
            chunkmsg = yield self._process.message_client.create_instance(DATA_CHUNK_MESSAGE_TYPE)
            chunkmsg.seq_number = exidx
            chunkmsg.seq_max = len(extraction_plan)

            log.debug("Chunk #%d: starter index %d, length %d" % (exidx, targetstartidx, elemcount))

            # set info in this chunk
            chunkmsg.start_index = targetstartidx
            chunkmsg.done = exidx == len(extraction_plan) - 1 #(i==totalchunks-1)      # last chunk message?  set the done flag

            # create the ndarray in this chunk
            chunkndarray = chunkmsg.CreateObject(curstrips[0][0].GetLink('ndarray').type)

            # these lines blow up with a TypeError if we screwed up the bounds and didn't fill in the targetarray fully,
            # aka it contains Nones
            chunkndarray.value[0:elemcount] = targetndarray[:]
            chunkmsg.ndarray = chunkndarray

            # send this message to the passed in routing key
            yield self._send_data_chunk(request.data_routing_key, chunkmsg)

            #  unload arrays we no longer need

            # collect upcoming keys from extraction plan
            # all csis
            csis = sum(extraction_plan[exidx+1:], [])
            upcoming_keys = set([compressed_striplist[csi][0].GetLink('ndarray').key for csi in csis])
            unneeded_keys = loaded_ba_ndarrays.difference(upcoming_keys)

            # remove everything in unneeded_keys from repo, our local cache
            for unload_key in unneeded_keys:
                log.debug('Removing unneeded key %s' % base64.encodestring(unload_key)[0:6])
                # _load_element does not load into repo._workspace, so don't need to unload that ourselves
                del repo.index_hash[unload_key]

                # remove from our local as well
                loaded_ba_ndarrays.discard(unload_key)
                del loaded_ba_ndarray_objs[unload_key]
            
        self._process.reply_ok(message, response)
        log.info("/op_extract_data")
        
    @defer.inlineCallbacks
    def _send_data_chunk(self, data_routing_key, chunkmsg):
        """
        Sends a data chunk message (from op_extract_data).  This is split out to facilitate
        testing via monkeypatching this method.
        """
        log.debug("_send_data_chunk to %s" % data_routing_key)
        yield self._process.send(data_routing_key, 'noop', chunkmsg)


    def _double_xrange(self, start1, end1, start2, end2):
        """
        This is a method just like xrange, but it operates on two diff ranges at once.
        Used by extract_data when mapping slice ranges for data extraction.

        @NOTE: This is a generator method, not to be confused with one that needs defer.inlineCallbacks decoration!
        """
        counter1 = start1
        counter2 = start2
        while counter1 < end1 and counter2 < end2:
            yield (counter1, counter2)

            counter1 += 1
            counter2 += 1

    def _get_slices(self, targetdimextents, srcdimextents, targetranges, srcranges, strides, targetidx=0, srcidx=0):
        """
        Returns a tuple of slice ranges (as tuples) that you can use to extract data from slices
        inside an ndarray. This is used by extract_data.

        @NOTE: This is a generator method, not to be confused with one that needs defer.inlineCallbacks decoration!

        @param  targetdimextents    The dimensional extents of the target bounded array.
        @param  srcdimextents       The dimensional extents of the source bounded array.
        @param  targetranges        A list of tuples (one tuple per dimension), specifying a range in each
                                    dimension that we are storing into.
        @param  srcranges           A list of tuples (one tuple per dimension), specifying a range in each
                                    dimension that we are pulling data out of.
        @param  strides             A list of stride amounts for each dimension. Should be the same length
                                    as targetranges/srcranges. The last dimension is not factored in here, but is
                                    returned as the last member of the return tuple.
        @param  targetidx           An accumulated index value into the target bounded array which gives the
                                    current index calculated on each recursive call.
        @param  srcidx              An accumulated index value into the source bounded array which gives the
                                    current index calculated on each recursive call.

        @returns                    On each yield, a tuple containing two tuples and an integer: a range to the target WITH
                                    striding applied to all but the last dimension, a range to copy from the source WITHOUT
                                    striding applied, and the stride length of the last dimension. 
        """

        # make sure we have sane things here
        assert len(targetdimextents) == len(srcdimextents)
        assert len(strides) == len(targetdimextents)

        # build index extent arrays
        targetidxextents = [None] * len(targetdimextents)
        srcidxextents = [None] * len(srcdimextents)

        targetidxextents[-1] = 1
        srcidxextents[-1] = 1

        # reduce target dim extents to extents after applying stride
        striddentargetextents = [targetdimextents[i]/strides[i] for i in xrange(len(targetdimextents))]

        # calculate index extents into target array, using stridden extents
        for x in range(len(targetidxextents)-2, -1, -1):
            targetidxextents[x] = reduce(lambda x,y: x*y, striddentargetextents[x+1:])

        # calculate index extents into source array
        for x in range(len(srcidxextents)-2, -1, -1):
            srcidxextents[x] = reduce(lambda x, y: x*y, srcdimextents[x+1:])

        def recslice(trs, srs, ts, ss, tstrides, cts=0, css=0, rc=0):
            """
            Recursive slice finder.
            @param  trs     Target ranges.
            @param  srs     Source ranges.
            @param  ts      Target slice (last dimension).
            @param  ss      Source slice (last dimension).
            @param  tstrides List of strides in all dimensions.
            @param  cts     Current target sum, aka index into target array.
            @param  css     Current source sum, aka index into source array.
            @param  rc      Recursion count, used to index into targetidxextents/srcidxextents.
            """
            if len(trs) == 0:
                # exit case: traversed all dimensions, we're on the last dimension, extract our slices
                yield ((cts+ts[0]/tstrides[0], cts+ts[1]/tstrides[0]),
                       (css+ss[0], css+ss[1]),
                       tstrides[0])
            else:
                # iterative case: look at current dimension, co-iterate over the ranges in target/src,
                #                 recurse into recslice again one dimension up until we run out.
                ctr = trs[0]
                csr = srs[0]
                cstride = tstrides[0]

                for tv, sv in self._double_xrange(ctr[0], ctr[1], csr[0], csr[1]):
                    if tv % cstride == 0:
                        for xx in recslice(trs[1:],
                                           srs[1:],
                                           ts,
                                           ss,
                                           tstrides[1:],
                                           cts+(tv * targetidxextents[rc]),     # calculate actual index offset here and pass it
                                           css+(sv * srcidxextents[rc]),        # calculate actual index offset here and pass it
                                           rc+1):
                            yield xx
                    else:
                        log.debug("IGNOREING STRIDED OUT DIM tv/sv %d,%d len dims %d" % (tv, sv, len(trs)))

        # iterate through all recslice generated slicepairs
        for x in recslice(targetranges[:-1],
                          srcranges[:-1],
                          targetranges[-1],
                          srcranges[-1],
                          strides[:]):
            yield x

    @defer.inlineCallbacks
    def op_get_object(self, request, headers, message):
        log.info('op_get_object')

        if not hasattr(request, 'MessageType') or request.MessageType != GET_OBJECT_REQUEST_MESSAGE_TYPE:
            raise DataStoreWorkBenchError('Invalid get_object request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)

        response = yield self._process.message_client.create_instance(GET_OBJECT_REPLY_MESSAGE_TYPE)

        key = request.object_id.key
        repo = yield self._resolve_repo_state(key)    # gets latest repo state from cassandra
        assert repo
        repo.cached = True

        # @TODO: use first head for now
        comms = repo.current_heads()
        commit = repo.current_heads()[0]

        link = commit.GetLink('objectroot')

        def filtermethod(x):
            """
            Returns true if the passed in link's type is not in the excluded_object_types list of the passed in message.
            """
            return x.type not in request.excluded_object_types

        # get blobs, update into response repository so we don't have to copy
        blobs = yield self._get_blobs(response.Repository, [link.key], filtermethod)
        response.Repository.index_hash.update(blobs)
        repo.index_hash.update(blobs)

        # load root object + links
        element = response.Repository.index_hash[link.key]
        root_obj = response.Repository._load_element(element)

        excluded_types = [x.GPBMessage for x in request.excluded_object_types]
        response.Repository.load_links(root_obj, excluded_types)

        # update repository's excluded_types
        for extype in excluded_types:
            if extype not in response.Repository.excluded_types:
                response.Repository.excluded_types.append(extype)

        # fill in response, respond
        response.retrieved_object = root_obj
        for ex_type in request.excluded_object_types:
            link = response.excluded_object_types.add()
            newex = response.CreateObject(GPBTYPE_TYPE)
            newex.object_id = ex_type.object_id
            newex.version = ex_type.version
            link.SetLink(newex)

        yield self._process.reply_ok(message, response)

        log.info("/op_get_object")

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

    # The type_map is a map from object type to resource type built from the ion_preload_configs
    # this is a temporary device until the resource registry is fully architecturally operational.
    type_map = TypeMap()



    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        ServiceProcess.__init__(self, *args, **kwargs)
            
        self._backend_cls_names = {}
        self._backend_cls_names[COMMIT_CACHE] = self.spawn_args.get(COMMIT_CACHE, CONF.getValue(COMMIT_CACHE, default='ion.core.data.store.IndexStore'))
        self._backend_cls_names[BLOB_CACHE] = self.spawn_args.get(BLOB_CACHE, CONF.getValue(BLOB_CACHE, default='ion.core.data.store.Store'))

        self._cache_size = self.spawn_args.get('cache_size', CONF.getValue('cache_size', default=10**8))

        self._backend_classes={}

        self._username = self.spawn_args.get("username", CONF.getValue("username", None))
        self._password = self.spawn_args.get("password", CONF.getValue("password",None))

        self._backend_classes[COMMIT_CACHE] = pu.get_class(self._backend_cls_names[COMMIT_CACHE])
        assert store.IIndexStore.implementedBy(self._backend_classes[COMMIT_CACHE]), \
            'The back end class to store commit objects passed to the data store does not implement the required IIndexSTORE interface.'
            
        self._backend_classes[BLOB_CACHE] = pu.get_class(self._backend_cls_names[BLOB_CACHE])
        assert store.IStore.implementedBy(self._backend_classes[BLOB_CACHE]), \
            'The back end class to store blob objects passed to the data store does not implement the required ISTORE interface.'
            
        # Declare some variables to hold the store instances
        
        self.c_store = None
        self.b_store = None

        # Get the configuration for cassandra - may or may not be used depending on the backend class
        self._storage_conf = get_cassandra_configuration()
        
        # Get the arguments for preloading the datastore
        self.preload = {ION_PREDICATES_CFG:True,
                        ION_RESOURCE_TYPES_CFG:True,
                        ION_IDENTITIES_CFG:True,
                        ION_DATASETS_CFG:False,
                        ION_AIS_RESOURCES_CFG:False}

        self.preload.update(CONF.getValue(PRELOAD_CFG, {}))
        self.preload.update(self.spawn_args.get(PRELOAD_CFG, {}))



        log.info('DataStoreService.__init__()')
        

    @defer.inlineCallbacks
    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        if issubclass(self._backend_classes[COMMIT_CACHE], cassandra.CassandraStore):
            #raise NotImplementedError('Startup for cassandra store is not yet complete')
            log.info("Instantiating Cassandra Index Store: %s" % self._backend_classes[COMMIT_CACHE])

            storage_provider = self._storage_conf[STORAGE_PROVIDER]
            keyspace = self._storage_conf[PERSISTENT_ARCHIVE]['name']

            self.c_store = self._backend_classes[COMMIT_CACHE](self._username, self._password, storage_provider, keyspace, COMMIT_CACHE)

            yield self.c_store.initialize()
            yield self.c_store.activate()

            # Can not afford to lazy initialize this value - the first call is a deferred list!
            query_attributes =  yield self.c_store.get_query_attributes()
            self.c_store._query_attribute_names = set(query_attributes)

            yield self.register_life_cycle_object(self.c_store)
            
        else:

            log.info("Clearing The In Memeory Index Store")

            self._backend_classes[COMMIT_CACHE].indices.clear()
            self._backend_classes[COMMIT_CACHE].kvs.clear()

            log.info("Instantiating In Memeory Index Store")
            # Pass self for index store service implementation
            self.c_store = self._backend_classes[COMMIT_CACHE](self, indices=COMMIT_INDEXED_COLUMNS )

        if issubclass(self._backend_classes[BLOB_CACHE], cassandra.CassandraStore):
            #raise NotImplementedError('Startup for cassandra store is not yet complete')
            log.info("Instantiating Cassandra Store: %s" % self._backend_classes[BLOB_CACHE])

            storage_provider = self._storage_conf[STORAGE_PROVIDER]
            keyspace = self._storage_conf[PERSISTENT_ARCHIVE]['name']
            
            self.b_store = self._backend_classes[BLOB_CACHE](self._username, self._password, storage_provider, keyspace, BLOB_CACHE)

            yield self.b_store.initialize()
            yield self.b_store.activate()

            yield self.register_life_cycle_object(self.b_store)
        else:

            log.info("Clearing The In Memeory Store")

            self._backend_classes[BLOB_CACHE].kvs.clear()

            log.info("Instantiating In Memory Store")
            # Pass self for store service implementation
            self.b_store = self._backend_classes[BLOB_CACHE](self)

        
        log.info("Created stores")
        self.workbench = DataStoreWorkbench(self, self.b_store, self.c_store, cache_size=self._cache_size)

        yield self.initialize_datastore()


    def slc_activate(self):


        self.op_fetch_blobs = self.workbench.op_fetch_blobs
        self.op_pull = self.workbench.op_pull
        self.op_push = self.workbench.op_push
        self.op_checkout = self.workbench.op_checkout
        self.op_put_blobs = self.workbench.op_put_blobs
        self.op_get_object = self.workbench.op_get_object
        self.op_extract_data = self.workbench.op_extract_data


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
                    log.info('Preloading Predicate:' + str(value.get(PREDICATE_CFG)))
                    predicate_repo = self._create_predicate(value)
                    if predicate_repo is None:
                        raise DataStoreError('Failed to create predicate: %s' % str(value))
                    #@TODO make associations to predicates!



        # Load the Root User!
        if self.preload[ION_IDENTITIES_CFG]:
            log.info('Preloading Identities')

            root_description = ION_IDENTITIES.get(root_name)
            exists = yield self.workbench.test_existence(ROOT_USER_ID)
            if not exists:
                log.info('Preloading ROOT USER')

                resource_instance = self._create_resource(root_description)
                if resource_instance is None:
                    raise DataStoreError('Failed to create Identity Resource: %s' % str(root_description))
                self._create_ownership_association(resource_instance.Repository, ROOT_USER_ID)

        if self.preload[ION_RESOURCE_TYPES_CFG]:
            log.info('Preloading Resource Types')

            for key, value in ION_RESOURCE_TYPES.items():

                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    log.info('Preloading Resource Type:' + str(value.get(NAME_CFG)))

                    resource_instance = self._create_resource(value)
                    if resource_instance is None:
                        raise DataStoreError('Failed to create Resource Type Resource: %s' % str(value))
                    self._create_ownership_association(resource_instance.Repository, ROOT_USER_ID)


        if self.preload[ION_IDENTITIES_CFG]:
            log.info('Preloading Identities')

            for key, value in ION_IDENTITIES.items():
                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    log.info('Preloading Identity:' + str(value.get(NAME_CFG)))

                    resource_instance = self._create_resource(value)
                    if resource_instance is None:
                        raise DataStoreError('Failed to create Identity Resource: %s' % str(value))
                    self._create_ownership_association(resource_instance.Repository, value[ID_CFG])
                    
        if self.preload[ION_DATASETS_CFG]:
            log.info('Preloading Data Sets: %d' % len(ION_DATASETS))

            for key, value in ION_DATASETS.items():
                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    log.info('Preloading DataSet:' + str(value.get(NAME_CFG)))

                    resource_instance = self._create_resource(value)
                    # Do not fail if returning none - may or may not load data from disk
                    if resource_instance is not None:

                        owner = value.get(OWNER_ID) or ANONYMOUS_USER_ID
                        log.info('Dataset Owner ID: %s' % owner)

                        self._create_ownership_association(resource_instance.Repository, owner)

                    else:
                        # Delete this entry from the CONFIG!
                        del ION_DATASETS[key]


            log.info('Preloading Data Sources: %d' % len(ION_DATA_SOURCES))
            for key, value in ION_DATA_SOURCES.items():
                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    log.info('Preloading DataSource:' + str(value.get(NAME_CFG)))

                    resource_instance = self._create_resource(value)
                    # Do not fail if returning none - may or may not load data from disk
                    if resource_instance is not None:

                        owner = value.get(OWNER_ID) or ANONYMOUS_USER_ID
                        log.info('Datasource Owner ID: %s' % owner)

                        self._create_ownership_association(resource_instance.Repository, owner)
                    else:
                        # Delete this entry from the CONFIG!
                        del ION_DATA_SOURCES[key]

        if self.preload[ION_AIS_RESOURCES_CFG]:
            log.info('Preloading AIS Resources')

            for key, value in ION_AIS_RESOURCES.items():
                exists = yield self.workbench.test_existence(value[ID_CFG])
                if not exists:
                    log.info('Preloading AIS Resource:' + str(value.get(NAME_CFG)))

                    resource_instance = self._create_resource(value)
                    if resource_instance is None:
                        raise DataStoreError('Failed to create AIS Resource: %s' % str(value))
                    self._create_ownership_association(resource_instance.Repository, ANONYMOUS_USER_ID)



        yield self.workbench.flush_initialization_to_backend()



    def _create_predicate(self,description):

        try:
            predicate_type = description[TYPE_CFG]
            predicate_key = description[ID_CFG]
            predicate_word = description[PREDICATE_CFG]
        except KeyError, ke:
            log.info(ke)
            return None

        try:
            predicate_repository = self.workbench.create_repository(root_type=predicate_type, repository_key=predicate_key)
            predicate = predicate_repository.root_object
            predicate.word = predicate_word

            predicate_repository.commit('Predicate instantiated by datastore bootstrap')
        except WorkBenchError, ex:
            log.info(ex)
            return None
        except repository.RepositoryError, ex:
            log.info(ex)
            return None
        
        return predicate_repository



    def _create_resource(self, description):
        """
        Helper method to create resource objects during initialization
        """
        try:
            resource_key = description[ID_CFG]
            resource_description = description[DESCRIPTION_CFG]
            resource_name = description[NAME_CFG]
            resource_type = description[TYPE_CFG]
            content = description[CONTENT_CFG]

            # LCS is optional!
            res_lcs = description.get(LCS_CFG)
        except KeyError, ke:
            log.info(ke)
            return None

        # Create this resource with a constant ID from the config file
        try:
            resource_repository = self.workbench.create_repository(root_type=RESOURCE_TYPE, repository_key=resource_key)
        except WorkBenchError, we:
            log.info(we)
            return None
        except repository.RepositoryError, re:
            log.info(re)
            return None

        resource = resource_repository.root_object

        # Set the identity of the resource
        resource.identity = resource_repository.repository_key

        # Create the new resource object
        res_obj = resource_repository.create_object(resource_type)

        # Set the object as the child of the resource
        resource.resource_object = res_obj

        # Name and Description is set by the resource client
        resource.name = resource_name
        resource.description = resource_description

        # Set the type...
        object_utils.set_type_from_obj(res_obj, resource.object_type)

        res_type = resource_repository.create_object(IDREF_TYPE)
        # Get the resource type if it exists - otherwise a default will be set!
        res_type.key = self.type_map.get(resource_type.object_id)
        resource.resource_type = res_type

        # State is set to new by default
        if res_lcs == COMMISSIONED:
            resource.lcs = resource.LifeCycleState.COMMISSIONED
        else:
            resource.lcs = resource.LifeCycleState.ACTIVE



        resource_instance = resource_client.ResourceInstance(resource_repository)

        # Set the content
        set_content_ok = True
        if isinstance(content, dict):
            # If it is a dictionary, set the content of the resource
            for k,v in content.items():
                setattr(resource_instance,k,v)

        elif isinstance(content, FunctionType):
            #execute the function on the resource_instance!
            kwargs = {'has_a_id':HAS_A_ID}
            if description.has_key(CONTENT_ARGS_CFG):
                kwargs.update(description[CONTENT_ARGS_CFG])

            set_content_ok = content(resource_instance, self, **kwargs)


        if set_content_ok:
            resource_instance.Repository.commit('Resource instantiated by datastore bootstrap')

            ### EXTREMELY VERBOSE LOGGING!
            #log.warn(description)
            #log.warn(resource_instance)
            #log.warn(resource_instance.ResourceObject.PPrint())
            
            return resource_instance

        else:
            self.workbench.clear_repository_key(resource_key)
            log.info('Retrieving content for resource "%s" failed.  This resource instance will not be added to the repository!' % resource_name)
            return None



    def _create_ownership_association(self, repo_object, user_id):

        association_repo = self.workbench.create_repository(ASSOCIATION_TYPE)

        # Set the subject
        id_ref = association_repo.create_object(IDREF_TYPE)
        repo_object.set_repository_reference(id_ref, current_state=True)
        association_repo.root_object.subject = id_ref

        # Set the predicate
        id_ref = association_repo.create_object(IDREF_TYPE)
        owned_by_repo = self.workbench.get_repository(OWNED_BY_ID)
        if owned_by_repo is None:
            raise DataStoreError('Owned_By predicate not found during preload.')
        owned_by_repo.set_repository_reference(id_ref, current_state=True)

        association_repo.root_object.predicate = id_ref

        # Set teh Object
        id_ref = association_repo.create_object(IDREF_TYPE)
        owner_repo = self.workbench.get_repository(user_id)
        if owner_repo is None:
            raise DataStoreError('Owner resource not found during preload.')
        owner_repo.set_repository_reference(id_ref, current_state=True)

        association_repo.root_object.object = id_ref

        association_repo.commit('Ownership association created for preloaded object.')


        return association_repo

class DataStoreClient(ServiceClient):
    """
    Client for retrieving datastore resources -- currently for retrieving the IDs of preloaded datasets
    """
    
    def __init__(self, *args, **kwargs):
        kwargs['targetname'] = 'datastore'
        ServiceClient.__init__(self, *args, **kwargs)

    @defer.inlineCallbacks
    def push(self, content):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('push', content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def pull(self, content):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('pull', content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def checkout(self, content):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('checkout', content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def fetch_blobs(self, content):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('fetch_blobs', content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def put_blobs(self, content):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('put_blobs', content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_object(self, content):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_object', content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def extract_data(self, content):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('extract_data', content)
        defer.returnValue(content)

#    @defer.inlineCallbacks
#    def get_preloaded_datasets_dict(self):
#        """
#        Retrieve the dictionary of preloaded dataset IDs
#        """
##        yield self._check_init()
#
#        log.info("@@@--->>> DataStoreClient: Sending RPC message.  OP = 'get_preloaded_datasets_dict'")
##        (content, headers, msg) = yield self.rpc_send('get_preloaded_datasets_dict', None)
##
##        log.info("<<<---@@@ DataStoreClient: Incoming rpc reply to op: 'get_preloaded_datasets_dict'")
##        log.debug("... Content\t" + str(content))
##        log.debug("... Headers\t" + str(headers))
##        log.debug("... Message\t" + str(msg))
##
##        defer.returnValue(content)
#        yield
#        defer.returnValue({})

# Spawn of the process using the module name
factory = ProcessFactory(DataStoreService)
