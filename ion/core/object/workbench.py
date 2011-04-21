#!/usr/bin/env python
"""
@file ion/core/object/workbench.py
@author David Stuebe
@author Matt Rodriguez
@brief Workbench for operating on GPB backed object structures

TODO
Add persistent store to the work bench. Use it fetch linked objects
"""

from twisted.internet import defer

from google.protobuf import message


from ion.core.object import object_utils
from ion.core.object import repository
from ion.core.object import gpb_wrapper
from ion.core.object import association_manager

from ion.core.exception import ReceivedError
from ion.core.object.gpb_wrapper import OOIObjectError

from ion.core.exception import ReceivedApplicationError, ReceivedContainerError, ApplicationError

import weakref
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from net.ooici.core.container import container_pb2

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



class WorkBenchError(ApplicationError):
    """
    An exception class for errors that occur in the Object WorkBench class
    """

class WorkBench(object):
    
    def __init__(self, process):
    
        self._process = process
        
        self._repos = {}
        
        self._repository_nicknames = {}
        
        """
        A cache - shared between repositories for hashed objects
        """  
        self._workbench_cache = weakref.WeakValueDictionary()

        #@TODO Consider using an index store in the Workbench to keep a cache of associations and keep track of objects

      
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
        Simple getter for the repository dictionary
        """
        
        rkey = self._repository_nicknames.get(key, None)
        if not rkey:
            rkey = key
            
        return self._repos.get(rkey,None)
        
    def list_repositories(self):
        """
        Simple list tool for repository names - not sure this will exist?
        """
        return self._repos.keys()

    def clear_repository_key(self, repo_key):

        repo = self.get_repository(repo_key)
        self.clear_repository(repo)

    def clear_repository(self, repo):

        key = repo.repository_key
        repo.clear()

        del self._repos[key]

        # Remove the nickname too - this is dumb - nicknames may be removed anyway. Don't worry about it.
        for k,v in self._repository_nicknames.items():

            if v == key:
                del self._repository_nicknames[k]


    def clear_non_persistent(self):

        for repo in self._repos.values():

            if repo.persistent is False:
                self.clear_repository(repo)



    def put_repository(self,repo):
        
        self._repos[repo.repository_key] = repo
        repo.index_hash.cache = self._workbench_cache
        repo._process = self._process
       
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

            keys_to_get.clear()
            for link in new_links_to_get:
                if not blobs.has_key(link.key) and filtermethod(link):
                    keys_to_get.add(link.key)

        return blobs



    @defer.inlineCallbacks
    def op_checkout(self, content, headers, msg):

        if not hasattr(content, 'MessageType') or content.MessageType != REQUEST_COMMIT_BLOBS_MESSAGE_TYPE:
             raise WorkBenchError('Invalid checkout request. Bad Message Type!', content.ResponseCodes.BAD_REQUEST)

        response = yield self._process.message_client.create_instance(BLOBS_MESSAGE_TYPE)

        def filtermethod(x):
            """
            Returns true if the passed in link's type is not in the excluded_types list of the passed in message.
            """
            return (x.type not in content.excluded_types)

        blobs = yield defer.maybeDeferred(self._get_blobs, response.Repository, [content.commit_root_object], filtermethod)

        for element in blobs.values():
            link = response.blob_elements.add()
            obj = response.Repository._wrap_message_object(element._element)

            link.SetLink(obj)

        yield self._process.reply_ok(msg, content=response)


    @defer.inlineCallbacks
    def pull(self, origin, repo_name, get_head_content=True, excluded_types=None):
        """
        Pull the current state of the repository
        """

        if excluded_types is None:
            excluded_types = repository.Repository.DefaultExcludedTypes
        elif not hasattr(excluded_types, '__iter__'):
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
            repo = repository.Repository(repository_key=repo_name)
            self.put_repository(repo)
        else:
            cloning = False
            # If we have a current version - get the list of commits
            commit_list = self.list_repository_commits(repo)

        # Create pull message
        pullmsg = yield self._process.message_client.create_instance(PULL_MESSAGE_TYPE)
        pullmsg.repository_key = repo.repository_key
        pullmsg.get_head_content = get_head_content
        pullmsg.commit_keys.extend(commit_list)

        if get_head_content:
            for extype in excluded_types:
                exobj = pullmsg.excluded_types.add()
                exobj.object_id = extype.object_id
                exobj.version = extype.version

        try:
            result, headers, msg = yield self._process.rpc_send(targetname,'pull', pullmsg)
        except ReceivedApplicationError, re:

            log.info('ReceivedApplicationError', str(re))

            ex_msg = re.msg_content
            msg_headers = re.msg_headers

            if cloning:
                # Clear the repository that was created for the clone
                self.clear_repository(repo)
                del repo

            if ex_msg.MessageResponseCode == ex_msg.ResponseCodes.NOT_FOUND:

                raise WorkBenchError('Pull Operation failed: Repository Key Not Found! "%s"' % str(re))
            else:
                raise WorkBenchError('Pull Operation failed for unknown reason "%s"' % str(re))

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
        

        if not hasattr(request, 'MessageType') or request.MessageType != PULL_MESSAGE_TYPE:
            raise WorkBenchError('Invalid pull request. Bad Message Type!', request.ResponseCodes.BAD_REQUEST)


        repo = self.get_repository(request.repository_key)
        if not repo:
            raise WorkBenchError('Repository Key "%s" not found' % request.repository_key, request.ResponseCodes.NOT_FOUND)

        if repo.status != repo.UPTODATE:
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


        for commit_key in puller_needs:
            commit_element = repo.index_hash.get(commit_key)
            if commit_element is None:
                raise WorkBenchError('Repository commit object not found in op_pull', request.ResponseCodes.NOT_FOUND)
            link = response.commit_elements.add()
            obj = response.Repository._wrap_message_object(commit_element._element)
            link.SetLink(obj)


        if request.get_head_content:
            blobs=set()
            for commit in repo.current_heads():

                root_object_link = commit.GetLink('objectroot')

                element = repo.index_hash.get(root_object_link.key, None)
                if element is None:
                    raise WorkBenchError('Repository root object not found in op_pull', request.ResponseCodes.NOT_FOUND)

                new_elements = set([element,])

                while len(new_elements) > 0:
                    children = set()
                    for element in new_elements:
                        blobs.add(element)
                        for child_key in element.ChildLinks:
                            child = repo.index_hash.get(child_key,None)
                            if child is None:
                                raise WorkBenchError('Repository object not found in op_pull', request.ResponseCodes.NOT_FOUND)
                            elif child not in blobs:
                                children.add(child)
                    new_elements = children

            for element in blobs:
                link = response.blob_elements.add()
                obj = response.Repository._wrap_message_object(element._element)

                link.SetLink(obj)

        yield self._process.reply_ok(msg, content=response)


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
            obj = repo._wrap_message_object(head_element._element)
            repostate.repo_head_element = obj

            repostate.blob_keys.extend(self.list_repository_blobs(repo))

        try:
            result, headers, msg = yield self._process.rpc_send(targetname,'push', pushmsg)

            # @TODO Return more info about the result - detect divergence?
        except ReceivedError, re:
            
            log.debug('ReceivedError', str(re))
            raise WorkBenchError('Push returned an exception! "%s"' % re.msg_content)

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
                #if it does not exist make a new one
                repo = repository.Repository(repository_key=repostate.repository_key)
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

        for link in links:
            assert link.ObjectType == LINK_TYPE, 'Invalid link in list passed to Fetch Links!'
            blobs_request.blob_keys.append(link.key)


        blobs_msg, headers, msg = yield self._process.rpc_send(address,'fetch_blobs', blobs_request)

        elements = []
        for se in blobs_msg.blob_elements:
            # Put the new objects in the repository
            element = gpb_wrapper.StructureElement(se.GPBMessage)
            elements.append(element)

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

        if repo._dotgit == head:
            return

        if len(repo.branches) == 0:
            # if we are doing a clone - pulling a new repository
            repo._dotgit = head
            if len(repo.branches)>1:
                log.warn('Do not assume branch order in unchanged - setting master to branch 0 anyways!')
            repo.branchnicknames['master']=repo.branches[0].branchkey
            return
        
        # The current repository state must be merged with the new head.
        self._merge_repo_heads(repo._dotgit, head)



    def _merge_repo_heads(self, existing_head, new_head):

        log.debug('_merge_repo_heads: merging the state of repository heads!')
        log.debug('existing repository head:\n'+str(existing_head))
        log.debug('new head:\n'+str(new_head))

        # examine all the branches in new and merge them into existing
        for new_branch in new_head.branches:

            new_branchkey = new_branch.branchkey

            for existing_branch in existing_head.branches:

                if new_branchkey == existing_branch.branchkey:
                    # We need to merge the state of these branches

                    self._resolve_branch_state(existing_branch, new_branch)

                    # We found the new branch in existing - exit the outter for loop
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


    def _resolve_branch_state(self, existing_branch, new_branch):
        """
        Move everything in new into an updated existing!
        """
        log.debug('_resolve_branch_state: resolving branch state in repository heads!')
        for new_link in new_branch.commitrefs.GetLinks():

            # An indicator for a fast forward merge made on the existing branch
            found = False
            for existing_link in existing_branch.commitrefs.GetLinks():

                # Get the repositories we are working from
                repo = existing_branch.Repository

                # test to see if we these are the same head ref!
                if new_link == existing_link:
                    # If these branches have the same state we are good - continue to the next new cref in the new branch.
                    break

                # Look in the commit index of the existing repo to see if the head of the received message is an old commit
                # This works one way but not the other - it is a short cut!
                elif repo._commit_index.has_key(new_link.key):
                    # The branch in new_repo is out of date with what exists here.
                    # We can completely ignore the new link!
                    break

                # Look in the commit index of the new repo to see if the existing link is an old commit in new repository
                else:

                    self._load_commits(new_link) # Load the new ancestors!
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



    def _load_commits(self, link):
                
        log.debug('_load_commits: Loading all commits in the repository')

        repo = link.Repository

        try:
            cref = repo.get_linked_object(link)
        except repository.RepositoryError, ex:
            log.debug(ex)
            raise WorkBenchError('Commit id not found while loding commits: \n %s' % link.key)
            # This commit ref was not actually sent!
            
        if cref.ObjectType != COMMIT_TYPE:
            raise WorkBenchError('This method should only load commits!')
            
        for parent in cref.parentrefs:
            link = parent.GetLink('commitref')
            # Call this method recursively for each link
            self._load_commits(link)
        
        log.debug('_load_commits: Loaded all commits!')

