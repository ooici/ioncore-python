#!/usr/bin/env python
"""
@file ion/core/object/workbench.py
@author David Stuebe
@author Matt Rodriguez
@brief Workbench for operating on GPB backed object structures

@TODO
Caching mechanisms are now in place. Consider changing the cache size test to look at the size of the _workbench_cache
but throw out repositories from the _repo_cache to clear it - that would be better!
"""
from twisted.internet import defer

from ion.core.object.object_utils import sha1_to_hex

from ion.core.object import object_utils
from ion.core.object import repository
from ion.core.object import gpb_wrapper
from ion.core.object import association_manager
from ion.util import procutils as pu

from ion.core.exception import ReceivedError
from ion.core.object.gpb_wrapper import OOIObjectError

from ion.core.exception import ReceivedApplicationError, ApplicationError

import weakref

# Static entry point for "thread local" context storage during request
# processing, eg. to retaining user-id from request message
#from net.ooici.core.container import container_pb2


from ion.util.cache import LRUDict
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


STRUCTURE_ELEMENT_TYPE = object_utils.create_type_identifier(object_id=1, version=1)
STRUCTURE_TYPE = object_utils.create_type_identifier(object_id=2, version=1)

IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)
GPBTYPE_TYPE = object_utils.create_type_identifier(object_id=9, version=1)
ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)

MUTABLE_TYPE = object_utils.create_type_identifier(object_id=6, version=1)
LINK_TYPE = object_utils.create_type_identifier(object_id=3, version=1)
COMMIT_TYPE = object_utils.create_type_identifier(object_id=8, version=1)



PULL_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=46, version=1)
PULL_RESPONSE_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=47, version=1)

# used by op_checkout, response is BLOBS_MESSAGE_TYPE
REQUEST_COMMIT_BLOBS_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=48, version=1)


PUSH_MESSAGE_TYPE  = object_utils.create_type_identifier(object_id=41, version=1)
#REPOSITORY_STATE_TYPE= object_utils.create_type_identifier(object_id=42, version=1)
#PUSH_CONTENT_REQUEST_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=43, version=1)
#HASHED_CONTENT_KEYS = object_utils.create_type_identifier(object_id=44, version=1)
#REQUESTED_PUSH_CONTENT_MESSAGE = object_utils.create_type_identifier(object_id=45, version=1)
#PUSHCONTENTELEMENTS = object_utils.create_type_identifier(object_id=53, version=1)

BLOBS_REQUSET_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=51, version=1)
BLOBS_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=52, version=1)
GET_LCS_REQUEST_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=58, version=1)
GET_LCS_RESPONSE_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=59, version=1)

DATA_REQUEST_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=53, version=1)
DATA_REPLY_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=54, version=1)
DATA_CHUNK_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=57, version=1)
GET_OBJECT_REQUEST_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=55, version=1)
GET_OBJECT_REPLY_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=56, version=1)

class WorkBenchError(ApplicationError):
    """
    An exception class for errors that occur in the Object WorkBench class
    """

class WorkBench(object):
    
    def __init__(self, process, cache_size=10**7):
    
        self._process = process

        # For storage of repositories which are deterministically held in memory
        self._repos = {}
        
        self._repository_nicknames = {}


        # A Cache of repositories that holds upto a certain size between op message calls.
        self._repo_cache = LRUDict(cache_size, use_size=True)


        """
        A cache - shared between repositories for hashed objects
        """  
        self._workbench_cache = weakref.WeakValueDictionary()

        #@TODO Consider using an index store in the Workbench to keep a cache of associations and keep track of objects

    def __str__(self):
        '''
        Debugging string method.
        '''
        try:
            proc_name = self._process.proc_name
        except AttributeError:
            proc_name = 'No Process Name'

        retstr = "/ ==== Workbench info (id:%s) (ProcName: %s) ==========\n" % (id(self), proc_name)
        retstr += "++ Workbench Blob Cache, (len:%d)\n" % len(self._workbench_cache)
        #for k,v in self._workbench_cache.iteritems():
        #    retstr += "\t%s: %s\n" % (base64.encodestring(k)[0:-1], '')

        retstr += "++ Persistent Repositories, (len:%d)\n" % len(self._repos)
        for k, v in self._repos.iteritems():
            retstr += "\t%s: ih %d, cached %s, persistent %s, conv %s\n" %(k, len(v.index_hash), v.cached, v.persistent, v.convid_context)

        retstr += "++ LRU RepoCache, (len:%d)\n" % len(self._repo_cache.keys())
        for k, v in self._repo_cache.iteritems():
            retstr += "\t%s: ih %d, cached %s, persistent %s,conv %s\n" %(k, len(v.index_hash), v.cached, v.persistent, v.convid_context)


        retstr += "/ ==== End  Workbench info ===========\n"

        return retstr

    def cache_info(self):

        trouble = False
        convid=None
        convids = set()
        for repo in self._repos.itervalues():

            if convid is None and repo.persistent is False:
                convid = repo.convid_context
                convids.add(repo.convid_context)
            elif convid != repo.convid_context and repo.persistent is False:
                trouble = True
                convids.add(repo.convid_context)

        if trouble:
            return str('Workbench Cache is holding %d repositories in %d conversations' % (len(self._repos), len(convids)) )
        else:
            return 'Workbench Cache is clear!'

    def count_persistent(self):
        nrepos = len(self._repos)
        count = 0
        if  nrepos > 0:

            for repo in self._repos.itervalues():
                if repo.persistent is True:
                    count +=1
        return count




    def create_repository(self, root_type=None, nickname=None, repository_key=None, persistent=False):
        """
        New better method to initialize a repository.
        The init_repository method returns both the repo and the root object.
        This is awkward. Now that the repository has a root_object property, it
        is better to just return the repository.
        """
        repo = repository.Repository(repository_key=repository_key, persistent=persistent)

        self.put_repository(repo)

            
        # Set the default branch
        repo.branch(nickname='master')
        # There is no default branch - the branch order is no longer order preserving in the datastore!
           
        # Handle options in the root class argument
        if isinstance(root_type, object_utils.get_gpb_class_from_type_id(GPBTYPE_TYPE)):
            
            try:
                rootobj = repo.create_object(root_type)
            except object_utils.ObjectUtilException, ex:
                raise WorkBenchError('Invalid root object type identifier passed in create_repository. Unrecognized type: "%s"' % str(root_type))
        
            repo._workspace_root = rootobj
        
        elif root_type is not None:
            # Hard to test for either nothing or a GPBTYPE - this works pretty well
            raise WorkBenchError('Invalid root type argument passed in create_repository')
        

        if nickname:
            self.set_repository_nickname(repo.repository_key, nickname)
        
        return repo
      
        
    def init_repository(self, root_type=None, nickname=None):
        """
        Initialize a new repository
        Factory method for creating a repository - this is the responsibility
        of the workbench.
        @param root_type is the object type identifier for the object
        """
        
        log.info('The init_repository method is depricated in favor of create_repository')
        
        repo = self.create_repository(root_type, nickname)
        
        return repo, repo.root_object


    def create_association(self, subject, predicate, obj, commit_msg=None):
        """
        @Brief Create an association repository object in your workbench
        @param subject is a resource instance or a repository object
        @param predicate is a resource instance or a repository object
        @param object is a resource instance or a repository object
        @retval association_repo is a repository object for the association.
        """
        # Create a new repository to hold this data object
        association_repo = self.create_repository(ASSOCIATION_TYPE)

        self._set_association(association_repo, subject, 'subject')
        self._set_association(association_repo, predicate, 'predicate')
        self._set_association(association_repo, obj, 'object')

        if commit_msg is None:
            commit_msg = 'Created association'
        association_repo.commit(commit_msg)

        association = association_manager.AssociationInstance(association_repo, self)

        return association


    def _set_association(self,  association_repo, thing, partname):

        if not hasattr(thing, 'Repository'):
            log.error('Association Error: type, value', type(thing),str(thing))
            raise WorkBenchError('Invalid object passed to Create Association. Only Object Repositories and Instance types can be passed as subject, predicate or object')

        thing_repo = thing.Repository


        id_ref = association_repo.create_object(IDREF_TYPE)
        thing_repo.set_repository_reference(id_ref, current_state=True)

        association_repo.root_object.SetLinkByName(partname,id_ref)




    def set_repository_nickname(self, repositorykey, nickname):
        # Should this throw an error if that nickname already exists?
        self._repository_nicknames[nickname] = repositorykey    
        
    def get_repository(self,key):
        """
        Getter for the repository dictionary and cache
        """

        # Get the nickname if it exists
        rkey = self._repository_nicknames.get(key, key)

        repo = self._repos.get(rkey,None)

        if repo is None:

            try:
                repo = self._repo_cache.pop(rkey)
                self.put_repository(repo)
            except KeyError, ke:
                log.debug('Repository key "%s" not found in cache' % rkey)

        return repo
        
    def list_repositories(self):
        """
        Simple list tool for repository names - not sure this will exist?
        """
        return self._repos.keys()

    def clear_repository_key(self, repo_key):

        repo = self.get_repository(repo_key)
        self.clear_repository(repo)

    def clear_repository(self, repo):

        log.info('Clearing Repository: %s ' % str(repo.repository_key))

        key = repo.repository_key
        repo.clear()

        del self._repos[key]

        # Remove the nickname too - this is dumb - nicknames may be removed anyway. Don't worry about it.
        for k,v in self._repository_nicknames.items():

            if v == key:
                del self._repository_nicknames[k]


    def cache_repository(self, repo):

        log.info('Caching Repository: %s ' % str(repo.repository_key))

        key = repo.repository_key
        # Get rid of the nick name - this is a PITA
        for k,v in self._repository_nicknames.items():
            if v == key:
                del self._repository_nicknames[k]

        # Delete it from the deterministically held repo dictionary
        del self._repos[key]

        repo.purge_workspace()

        repo.purge_associations()

        # Move it to the cached repositories
        self._repo_cache[key] = repo


    def manage_workbench_cache(self, convid_context=None):
        """
        @Brief Move repositories from the level 1 persistent cache to the level two LRU cache
        @param convid_context if not None, move only objects in a particular context
        """

        log.info('Running Manage Workbench Cache...')

        # Can't use iter here - it is actually deleting keys in the dict object.
        for key, repo in self._repos.items():

            if repo.persistent is True:
                continue

            if repo.convid_context == convid_context or convid_context is None:

                if repo.cached is False:
                    self.clear_repository(repo)

                else:
                    self.cache_repository(repo)


        log.info('End Manage Workbench Cache...')


    def clear(self):
        """
        Completely clean the state or the workbench, wipe any repositories and delete references to them.
        """

        log.info('CLEARING THE WORKBENCH - IGNORING PERSISTENCE SETTINGS')

        for repo in self._repos.itervalues():
            repo.clear()

        self._repos.clear()

        #The cache knows to clear its content objects
        self._repo_cache.clear()

        # these are just strings
        self._repository_nicknames.clear()

        # This one is now safe to clear
        self._workbench_cache.clear()



    def put_repository(self,repo):

        if repo.repository_key in self._repo_cache:
            raise WorkBenchError('This repository already exists in the workbench cache - that should not happen!')

        self._repos[repo.repository_key] = repo
        repo.index_hash.cache = self._workbench_cache
        repo._process = self._process

        try:
            repo.convid_context = self._process.context.get('progenitor_convid')
        except AttributeError, ae:
            log.warn('Workbench Process (%s) does not have have a context object!' % self._process)
       
    def reference_repository(self, repo_key, current_state=False):

        repo = self.get_repository(repo_key)

        if current_state == True:
            if repo.status != repo.UPTODATE:
                raise WorkBenchError('Can not reference the current state of a repository which has been modified but not committed')

        # Create a new repository to hold this data object
        repository = self.create_repository(IDREF_TYPE)
        id_ref = repository.root_object
        
        # Use the method of the repository we are tagging to set the reference
        repo.set_repository_reference(id_ref, current_state)
        
        # Commit the new repository object
        repository.commit('Created repository reference')
        
        return id_ref
        
    def fork(self, structure, name):
        """
        Fork the structure in the wrapped gpb object into a new repository.
        """


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

                    log.warn('Blob not found in _get_blobs.')


            keys_to_get.clear()
            for link in new_links_to_get:
                if not blobs.has_key(link.key) and filtermethod(link):
                    keys_to_get.add(link.key)

        return blobs



    @defer.inlineCallbacks
    def op_checkout(self, content, headers, msg):

        log.info('op_checkout - start')

        if not hasattr(content, 'MessageType') or content.MessageType != REQUEST_COMMIT_BLOBS_MESSAGE_TYPE:
             raise WorkBenchError('Invalid checkout request. Bad Message Type!', content.ResponseCodes.BAD_REQUEST)

        response = yield self._process.message_client.create_instance(BLOBS_MESSAGE_TYPE)

        def filtermethod(x):
            """
            Returns true if the passed in link's type is not in the excluded_types list of the passed in message.
            """
            return (x.type not in content.excluded_types)

        # this is inherited by DatastoreWorkbench, which requires the _get_blobs call be a deferred, whereas here it is not.
        # tldr; maybeDeferred necessary.
        blobs = yield defer.maybeDeferred(self._get_blobs, response.Repository, [content.commit_root_object], filtermethod)

        for element in blobs.values():
            link = response.blob_elements.add()
            obj = response.Repository._wrap_message_object(element._element)

            link.SetLink(obj)

        yield self._process.reply_ok(msg, content=response)

        log.info('op_checkout - complete')


    @defer.inlineCallbacks
    def pull(self, origin, repo_name, get_head_content=True, excluded_types=None):
        """
        Pull the current state of the repository
        """

        log.info('pull - start')

        if excluded_types is not None and not hasattr(excluded_types, '__iter__'):
            raise WorkBenchError('Invalid excluded_types argument passed to checkout')

        # Get the scoped name for the process to pull from
        targetname = self._process.get_scoped_name('system', origin)

        log.info('Target Name "%s"' % str(targetname))


        if not isinstance(repo_name, (str, unicode)):
            raise TypeError('Invalid argument (repo_nae) type to workbench pull. Should be string, received: "%s"' % type(repo_name))
        repo = self.get_repository(repo_name)
        
        commit_list = []
        if repo is None:
            #if it does not exist make a new one
            cloning = True
            repo = repository.Repository(repository_key=repo_name, cached=True)
            self.put_repository(repo)
        else:
            cloning = False
            # If we have a current version - get the list of commits
            commit_list = self.list_repository_commits(repo)

        # set excluded types on this repository
        if excluded_types is not None:
            repo.excluded_types = excluded_types        # @TODO: update instead of replace?

        # Create pull message
        pullmsg = yield self._process.message_client.create_instance(PULL_MESSAGE_TYPE)
        pullmsg.repository_key = repo.repository_key
        pullmsg.get_head_content = get_head_content
        pullmsg.commit_keys.extend(commit_list)

        if get_head_content:
            for extype in repo.excluded_types:
                exobj = pullmsg.excluded_types.add()
                exobj.object_id = extype.object_id
                exobj.version = extype.version

        try:
            result, headers, msg = yield self._process.rpc_send(targetname,'pull', pullmsg)
        except ReceivedApplicationError, re:



            ex_msg = re.msg_content
            msg_headers = re.msg_headers

            log.info('ReceivedApplicationError:Response code - %s, Response Message - "%s"' % (ex_msg.MessageResponseCode, ex_msg.MessageResponseBody))

            if cloning:
                # Clear the repository that was created for the clone
                self.clear_repository(repo)

            if ex_msg.MessageResponseCode == ex_msg.ResponseCodes.NOT_FOUND:

                raise WorkBenchError('Pull Operation failed: Repository Key Not Found! "%s"' % ex_msg.MessageResponseCode )
            else:
                raise WorkBenchError('Pull Operation failed: Response code - %s, Response Message - "%s"' % (ex_msg.MessageResponseCode, ex_msg.MessageResponseBody))

        if not hasattr(result, 'MessageType') or result.MessageType != PULL_RESPONSE_MESSAGE_TYPE:
            raise WorkBenchError('Invalid response to pull request. Bad Message Type!')
        

        if result.IsFieldSet('blob_elements') and not get_head_content:
            raise WorkBenchError('Unexpected response to pull request: included blobs but I did not ask for them.')

        # Add any new content to the repository:
        for se in result.commit_elements:

            # Move over new commits
            element = gpb_wrapper.StructureElement(se.GPBMessage)

            repo.index_hash[element.key] = element

        for se in result.blob_elements:
            # Move over any blobs
            element = gpb_wrapper.StructureElement(se.GPBMessage)
            repo.index_hash[element.key] = element

        # Move over the new head object
        head_element = gpb_wrapper.StructureElement(result.repo_head_element.GPBMessage)
        new_head = repo._load_element(head_element)
        new_head.Modified = True
        new_head.MyId = repo.new_id()
        
        # Now merge the state!
        self._update_repo_to_head(repo,new_head)


        # Where to get objects not yet transfered.
        repo.upstream = targetname

        log.info('pull - complete')

        defer.returnValue(result)



    @defer.inlineCallbacks
    def op_pull(self,request, headers, msg):
        """
        The operation which responds to a pull request
        If a process exposes this operation it must provide persistent storage
        for the objects it provides - the data store! When the op is complete
        only the repository and its commits have been transferred. The content
        will be lazy fetched as needed!
        """

        log.info('op_pull - start')


        if not hasattr(request, 'MessageType') or request.MessageType != PULL_MESSAGE_TYPE:
            raise WorkBenchError('Invalid pull request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)


        repo = self.get_repository(request.repository_key)
        if not repo:
            raise WorkBenchError('Repository Key "%s" not found' % request.repository_key, request.ResponseCodes.NOT_FOUND)

        log.debug('Found repository to pull')

        if repo.status == repo.MODIFIED:
            log.debug('Bad repo state for pulling - status: %s' % repo.status)
            raise WorkBenchError('Invalid pull request. Requested Repository is in an invalid state.', request.ResponseCodes.BAD_REQUEST)


        my_commits = self.list_repository_commits(repo)

        puller_has = request.commit_keys

        puller_needs = set(my_commits).difference(puller_has)

        response = yield self._process.message_client.create_instance(PULL_RESPONSE_MESSAGE_TYPE)

        # Create a structure element and put the serialized content in the response


        head_element = self.serialize_mutable(repo._dotgit)
        # Pull out the structure element and use it as the linked object in the message.
        obj = response.Repository._wrap_message_object(head_element._element)

        response.repo_head_element = obj


        log.debug('Created response and head object')

        for commit_key in puller_needs:
            commit_element = repo.index_hash.get(commit_key)
            if commit_element is None:
                raise WorkBenchError('Repository commit object not found in op_pull', request.ResponseCodes.NOT_FOUND)
            link = response.commit_elements.add()
            obj = response.Repository._wrap_message_object(commit_element._element)
            link.SetLink(obj)

        log.debug('Added commits to the response')

        if request.get_head_content:

            keys = [x.GetLink('objectroot').key for x in repo.current_heads()]

            def filtermethod(x):
                """
                Returns true if the passed in link's type is not in the excluded_types list of the passed in message.
                """
                return (x.type not in request.excluded_types)

            blobs = self._get_blobs(response.Repository, keys, filtermethod)

            for element in blobs.itervalues():
                link = response.blob_elements.add()
                obj = response.Repository._wrap_message_object(element._element)

                link.SetLink(obj)

            log.debug('Added blobs to the response')

        yield self._process.reply_ok(msg, content=response)
        log.info('op_pull - complete')


    def serialize_mutable(self, mutable):
        """

        """
        if mutable._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')


        # Create the Structure Element in which the binary blob will be stored
        se = gpb_wrapper.StructureElement()
        repo = mutable.Repository
        for link in  mutable.ChildLinks:

            if  repo.index_hash.has_key(link.key):
                child_se = repo.index_hash.get(link.key)

                # Set the links is leaf property
                link.isleaf = child_se.isleaf


            # Save the link info as a convience for sending!
            se.ChildLinks.add(link.key)

        se.value = mutable.SerializeToString()
        #se.key = sha1hex(se.value)

        # Structure element wrapper provides for setting type!
        se.type = mutable.ObjectType

        # Calculate the sha1 from the serialized value and type!
        # Sha1 is a property - not a method...
        se.key = se.sha1

        # Mutable is never a leaf!
        se.isleaf = False

        # Done setting up the Sturcture Element
        return se



    @defer.inlineCallbacks
    def push_by_name(self, origin, name_or_names):

        if hasattr(name_or_names, '__iter__'):

            repo_or_repos = []
            for name in name_or_names:
                repo = self.get_repository(name)
                if not repo:
                    raise KeyError('Repository name %s not found in work bench to push!' % name)

                repo_or_repos.append(repo)

        else:
            name = name_or_names
            repo_or_repos = self.get_repository(name)
            if not repo_or_repos:
                raise KeyError('Repository name %s not found in work bench to push!' % name)

        result = yield self.push(origin, repo_or_repos)

        defer.returnValue( result )




    @defer.inlineCallbacks
    def push(self, origin, repo_or_repos):
        """
        Push the current state of the repository.
        When the operation is complete - the transfer of all objects in the
        repository is complete.
        """

        log.info('push - start')

        targetname = self._process.get_scoped_name('system', origin)

        # Make a list of the repositories to push if it is not already one
        repos = repo_or_repos
        if not hasattr(repo_or_repos, '__iter__'):
            repos = [repo_or_repos,]

        for item in repos:
            if not hasattr(item, 'Repository'):
                raise WorkBenchError('Invalid argument to push. Only Repositories or Instance objects which wrap a repository may be pushed!')

        # Get any associations that we may need to push
        repositories_and_associations = set()
        for repo in repos:
            repositories_and_associations.add(repo)

            set_of_subject_associations = repo.Repository.associations_as_subject.get_associations()
            repositories_and_associations.update(set_of_subject_associations)

            set_of_object_associations = repo.Repository.associations_as_object.get_associations()
            repositories_and_associations.update(set_of_object_associations)

        instances = list(repositories_and_associations)


        # Create push message
        pushmsg = yield self._process.message_client.create_instance(PUSH_MESSAGE_TYPE)


        #Iterate the list and build the message to send
        for instance in instances:

            # Just in case this thing is an instance object
            repo = instance.Repository

            commit_head = repo.commit_head
            if commit_head is None:
                log.warning('No commits found in repository during push: \n' + str(repo))
                raise WorkBenchError('Can not push a repository which has no commits!')

            repostate = pushmsg.repositories.add()

            repostate.repository_key = repo.repository_key
            # Set the head element
            head_element = self.serialize_mutable(repo._dotgit)
            # Pull out the structure element and use it as the linked object in the message.

            # MUST USE THE REPOSTATE MESSAGES REPOSITY TO WRAP THE ELEMENT.
            # CAN CAUSE HASH CONFLICT DURING THE COPY IF IT IS CREATED IN THE REPO WE ARE PUSHING!
            obj = repostate.Repository._wrap_message_object(head_element._element)
            repostate.repo_head_element = obj

            repostate.blob_keys.extend(self.list_repository_blobs(repo))

        try:
            result, headers, msg = yield self._process.rpc_send(targetname,'push', pushmsg)

            # @TODO Return more info about the result - detect divergence?
        except ReceivedError, re:
            
            log.debug('ReceivedError', str(re))
            raise WorkBenchError('Push returned an exception! "%s"' % re.msg_content)


        log.info('push - complete')

        defer.returnValue(result)
        # @TODO - check results?

        
    @defer.inlineCallbacks
    def op_push(self, pushmsg, headers, msg):
        """
        The Operation which responds to a push.
        
        Operation does not complete until transfer is complete!
        """
        log.info('op_push!')

        if not hasattr(pushmsg, 'MessageType') or pushmsg.MessageType != PUSH_MESSAGE_TYPE:
            raise WorkBenchError('Invalid push request. Bad Message Type!', pushmsg.ResponseCodes.BAD_REQUEST)


        for repostate in pushmsg.repositories:

            repo = self.get_repository(repostate.repository_key)
            if repo is None:
                #if it does not exist make a new one - set it to cached for the time being...
                repo = repository.Repository(repository_key=repostate.repository_key, cached=True)
                self.put_repository(repo)
                repo_keys=set()
            else:

                if repo.status != repo.UPTODATE:
                    raise WorkBenchError('Requested push to a repository is in an invalid state.', request.ResponseCodes.BAD_REQUEST)
                repo_keys = set(self.list_repository_blobs(repo))

            # Get the set of keys in repostate that are not in repo_keys
            need_keys = set(repostate.blob_keys).difference(repo_keys)

            workbench_keys = set(self._workbench_cache.keys())

            local_keys = workbench_keys.intersection(need_keys)

            for key in local_keys:
                try:
                    repo.index_hash.get(key)
                    need_keys.remove(key)
                except KeyError, ke:
                    log.info('Key disappeared - get it from the remote after all')
                    
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


            # Move over the new head object
            head_element = gpb_wrapper.StructureElement(repostate.repo_head_element.GPBMessage)
            new_head = repo._load_element(head_element)
            new_head.Modified = True
            new_head.MyId = repo.new_id()

            # Now merge the state!
            self._update_repo_to_head(repo,new_head)
            


        response = yield self._process.message_client.create_instance(MessageContentTypeID=None)
        response.MessageResponseCode = response.ResponseCodes.OK            
            
        # The following line shows how to reply to a message
        yield self._process.reply_ok(msg, response)
        log.info('op_push: Complete!')


    @defer.inlineCallbacks
    def fetch_links(self, address, links):
        """
        Fetch the linked objects from another service
        Similar to the client pattern but must specify address!

        """
        blobs_request = yield self._process.message_client.create_instance(BLOBS_REQUSET_MESSAGE_TYPE)

        wb_has_keys = set(self._workbench_cache.keys())
        want_keys = set([link.key for link in links])

        need_keys = want_keys.difference(wb_has_keys)

        #for link in links:
        #    assert link.ObjectType == LINK_TYPE, 'Invalid link in list passed to Fetch Links!'
        #    blobs_request.blob_keys.append(link.key)

        blobs_request.blob_keys.extend(need_keys)

        blobs_msg, headers, msg = yield self._process.rpc_send(address,'fetch_blobs', blobs_request)

        elements = {}
        for se in blobs_msg.blob_elements:
            # Put the new objects in the repository
            element = gpb_wrapper.StructureElement(se.GPBMessage)
            elements[element.key] = element

        already_had_keys = want_keys.difference(need_keys)
        for key in already_had_keys:
            elements[key] = self._workbench_cache.get(key)

        defer.returnValue(elements)

    @defer.inlineCallbacks
    def fetch_blobs(self, address, request):
        """
        Fetch the objects by key from another service
        Similar to the client pattern but must specify address!

        """
        objs, headers, msg = yield self._process.rpc_send(address,'fetch_blobs', request)

        defer.returnValue(objs)


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
        
        
    def list_repository_commits(self, repo):
        """
        This method creates a list of the commits that exist in a repository
        The return value is a list of binary SHA1 keys
        """

        '''
        if repo.status == repo.MODIFIED:
            log.warn('Automatic commit called during pull. Commit should be called first!')
            comment='Commiting to send message with wrapper object'
            repo.commit(comment=comment)
        '''
        
        cref_set = set()
        for branch in repo.branches:

            for cref in branch.commitrefs:
                cref_set.add(cref)

        key_set = set()

        while len(cref_set)>0:
            new_set = set()

            for cref in cref_set:

                if cref.MyId not in key_set:
                    key_set.add(cref.MyId)

                    for prefs in cref.parentrefs:
                        new_set.add(prefs.commitref)


            # Now recurse on the ancestors
            cref_set = new_set

        key_list = []
        key_list.extend(key_set)
        return key_list

    def list_repository_blobs(self, repo):
        """
        This method creates a list of all the blobs that exist in a repository
        The return value is a list of binary SHA1 keys

        The method is a bit trivial - candidate for removal!
        """
        '''
        if repo.status == repo.MODIFIED:
            log.warn('Automatic commit called during push. Commit should be called first!')
            comment='Commiting to push repo.'
            repo.commit(comment=comment)
        '''

        return repo.index_hash.keys()


    def _update_repo_to_head(self, repo, head):
        log.debug('_update_repo_to_head: Loading a repository!')

        existing_commits = repo._commit_index.copy()

        log.info('Running load commits...')
        loaded={}
        for branch in head.branches:
            for link in branch.commitrefs.GetLinks():
                self._load_commits(link,loaded=loaded)

        if repo._dotgit == head:
            return

        if len(repo.branches) == 0:
            # if we are doing a clone - pulling a new repository
            repo._dotgit = head
            if len(repo.branches)>1:
                log.warn('Do not assume branch order is unchanged - setting master to branch 0 anyways!')
            repo.branchnicknames['master']=repo.branches[0].branchkey
            return
        
        # The current repository state must be merged with the new head.
        self._merge_repo_heads(repo._dotgit, head, existing_commits=existing_commits)



    def _merge_repo_heads(self, existing_head, new_head, existing_commits=None):

        log.debug('_merge_repo_heads: merging the state of repository heads!')
        log.debug('existing repository head:\n' + existing_head.Debug())
        log.debug('new head:\n' + new_head.Debug())
        repo = existing_head.Repository
        log.debug('Number of commits: %d' % len(repo._commit_index))
        log.debug('Number of hashed objects: %d' % len(repo.index_hash))


        # examine all the branches in new and merge them into existing
        for new_branch in new_head.branches:

            new_branchkey = new_branch.branchkey

            for existing_branch in existing_head.branches:

                if new_branchkey == existing_branch.branchkey:
                    # We need to merge the state of these branches

                    self._resolve_branch_state(existing_branch, new_branch, existing_commits=existing_commits)

                    # We found the new branch in existing - exit the inner for loop - next branch....
                    break

            else:

                # the branch in new is not in existing - add its head and move on
                branch = existing_head.branches.add()

                branch.branchkey = new_branchkey
                # Get the cref and then link it in the existing repository
                for cref in new_branch.commitrefs:
                    bref = branch.commitrefs.add()
                    bref.SetLink(cref)

        log.debug('_merge_repo_heads: merge repository complete')


    def _resolve_branch_state(self, existing_branch, new_branch, existing_commits=None):
        """
        Move everything in new into an updated existing!
        """
        # Get the repositories we are working from
        repo = existing_branch.Repository
        new_repo = new_branch.Repository

        log.debug('_resolve_branch_state: resolving branch state in repository heads!')
        for new_link in new_branch.commitrefs.GetLinks():

            # An indicator for a fast forward merge made on the existing branch
            found = False
            for existing_link in existing_branch.commitrefs.GetLinks():


                # test to see if we these are the same head ref!
                if new_link == existing_link:
                    # If these branches have the same state we are good - continue to the next new cref in the new branch.
                    break

                # Look in the commit index of the existing repo to see if the head of the received message is an old commit
                # This works one way but not the other - it is a short cut!
                elif new_link.key in existing_commits:
                    # The branch in new_repo is out of date with what exists here.
                    # We can completely ignore the new link!
                    break

                # Look in the commit index of the new repo to see if the existing link is an old commit in new repository
                else:


                    existing_cref = repo.get_linked_object(existing_link)
                    new_cref = repo.get_linked_object(new_link)

                    if existing_cref.InParents(new_cref):

                        # The existing repo can be fast forwarded to the new state!
                        # But we must keep looking through the existing_links to see if the push merges our state!
                        found = True
                        existing_link.key = new_link.key # Cheat and just copy the key!

                    # Anything state that is not caught by these three options is
                    # resolved in the else of the for loop by adding this divergent
                    # state to the existing (local) repositories branch

            else:

                # This is a non fastforward merge!
                # The branch has diverged and must be reconciled!
                if not found:
                    bref = existing_branch.commitrefs.add()
                    new_cref = new_repo._commit_index.get(new_link.key)
                    bref.SetLink(new_cref)


        key_set = set()
        duplicates = []
        # Merge any commit refs which have been resolved!
        # If one branch has more than one apperent head but it is actually the same value - it is not a divergence
        for i in range(len(existing_branch.commitrefs)):
            ref_link = existing_branch.commitrefs.GetLink(i)
            if ref_link.key in key_set:
                duplicates.append(i)
            else:
                key_set.add(ref_link.key)

        # Delete them in reverse order!
        duplicates.sort(reverse= True)
        for dup in duplicates:
            del existing_branch.commitrefs[dup]

        log.debug('_resolve_branch_state: resolved branch state in repository heads!')
        # Note this in the branches merge on read field and punt this to some
        # other part of the process.
        return



    def _load_commits(self, link, loaded=None):
                

        if loaded is None:
            loaded = {}


        log.debug('_load_commits - Key: %s' % sha1_to_hex(link.key))
        repo = link.Repository

        try:
            cref = repo.get_linked_object(link)
        except repository.RepositoryError, ex:
            log.exception(str(repo))
            raise WorkBenchError('Commit id not found while loading commits: \n %s' % link.key)
            # This commit ref was not actually sent!

        if cref.ObjectType != COMMIT_TYPE:
            raise WorkBenchError('This method should only load commits!')

        loaded[cref.MyId] = cref

        for parent in cref.parentrefs:
            link = parent.GetLink('commitref')
            # load the linked object no matter what to realize parent child relationships
            obj = repo.get_linked_object(link)

            # Call this method recursively for each link
            if link.key not in loaded:
                self._load_commits(link, loaded=loaded)
        

