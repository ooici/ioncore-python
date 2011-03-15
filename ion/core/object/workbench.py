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

class WorkBenchError(ApplicationError):
    """
    An exception class for errors that occur in the Object WorkBench class
    """

class WorkBench(object):
    
    def __init__(self, myprocess):   
    
        self._process = myprocess
        
        self._repos = {}
        
        self._repository_nicknames = {}
        
        """
        A cache - shared between repositories for hashed objects
        """  
        self._workbench_cache = weakref.WeakValueDictionary()
        #self._workbench_cache = {}

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
           
        # Handle options in the root class argument
        if isinstance(root_type, object_utils.get_gpb_class_from_type_id(GPBTYPE_TYPE)):
            
            try:
                rootobj = repo.create_object(root_type)
            except object_utils.ObjectUtilException, ex:
                raise WorkBenchError('Invalid root object type identifier passed in create_repository. Unrecognized type: "%s"' % str(root_type))
        
            repo._workspace_root = rootobj
        
        elif root_type is not None:
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


    def create_association(self, subject, predicate, obj):
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



        return association_repo


    def _set_association(self,  association_repo, thing, partname):

        if hasattr(thing, 'Repository'):
            # Allow passing a Resource Instance to create an association
            thing = thing.Repository


        if not isinstance(thing, repository.Repository):
            log.error('Association Error: type, value', type(thing),str(thing))
            raise WorkBenchError('Invalid object passed to Create Association. Only Resource Instances or Object Repositories can be passed as subject, predicate or object')


        id_ref = association_repo.create_object(IDREF_TYPE)
        thing.set_repository_reference(id_ref, current_state=True)

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
         
    @defer.inlineCallbacks
    def pull(self, origin, repo_name, get_head_content=True):
        """
        Pull the current state of the repository
        """

        # Get the scoped name for the process to pull from
        targetname = self._process.get_scoped_name('system', origin)

        log.info('Target Name "%s"' % str(targetname))
        repo = self.get_repository(repo_name)
        
        commit_list = []
        if repo is None:
            #if it does not exist make a new one
            repo = repository.Repository(repository_key=repo_name)
            self.put_repository(repo)
        else: 
            # If we have a current version - get the list of commits
            commit_list = self.list_repository_commits(repo)

        # Create pull message
        pullmsg = yield self._process.message_client.create_instance(PULL_MESSAGE_TYPE)
        pullmsg.repository_key = repo.repository_key
        pullmsg.get_head_content = get_head_content
        pullmsg.commit_keys.extend(commit_list)

        try:
            result, headers, msg = yield self._process.rpc_send(targetname,'pull', pullmsg)
        except ReceivedApplicationError, re:

            log.info('ReceivedApplicationError', str(re))

            ex_msg = re.msg_content
            msg_headers = re.msg_headers

            if ex_msg.MessageResponseCode == ex_msg.ResponseCodes.NOT_FOUND:

                raise WorkBenchError('Pull Operation failed: Repository Key Not Found! "%s"' % str(re))
            else:
                raise WorkBenchError('Pull Operation failed for unknown reason "%s"' % str(re))

        if not hasattr(result, 'MessageType') or result.MessageType != PULL_RESPONSE_MESSAGE_TYPE:
            raise WorkBenchError('Invalid response to pull request. Bad Message Type!')
        

        log.info('FIELDS: %s' % str(result.ListSetFields()))
        if result.IsFieldSet('blobs') and not get_head_content:
            raise WorkBenchError('Unexpected response to pull request: included blobs but I did not ask for them.')


        if len(result.commits) == 0:
            # We are upto date!
            return

        # Add any new content to the repository:

        for commit in result.commits:
            repo.index_hash[commit.MyId] = result.Repository.index_hash[commit.MyId]

        for blob in result.blobs:
            repo.index_hash[blob.MyId] = result.Repository.index_hash[blob.MyId]


        # Get the wrapped structure element to meet the old interface to merge...
        head = result.Repository.index_hash[result.repo_head.MyId]
        repo = self._load_repo_from_mutable(head)

        # Where to get objects not yet transfered.
        repo.upstream['service'] = origin
        repo.upstream['process'] = headers.get('reply-to')




        
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
            raise WorkBenchError('Invalid pull request. Bad Message Type!', request.BAD_REQUEST)


        repo = self.get_repository(request.repository_key)
        if not repo:
            raise WorkBenchError('Repository Key "%s" not found' % request.repo_head.repositorykey, request.NOT_FOUND)

        my_commits = self.list_repository_commits(repo)

        puller_has = request.commit_keys

        puller_needs = set(my_commits).difference(puller_has)

        response = yield self._process.message_client.create_instance(PULL_RESPONSE_MESSAGE_TYPE)

        print 'KENEENE<NE'
        response.repo_head = response.Repository.copy_object(repo._dotgit, deep_copy=False)

        print 'KENEENsssssssssE<NE'

        for commit_key in puller_needs:
            link = response.commits.add()
            link.SetLink(repo._commit_index[commit_key])

        print 'LPLPLPLPLPLPLPLPLP', headers

        """
        if request.get_head_content:
            for commit in repo.current_heads():

                root_object_link = commit.GetLink('objectroot')

                element = repo.index_hash.get(root_object_link.key, None)
                if element is None:
                    raise WorkBenchError('Repository root object not found in op_pull', request.NOT_FOUND)

                elements = [element,]

                while len(elements) > 0:
                    children = []
                    for element in elements:
                        link = response.blobs.add()
                        self._link_to_structure_element(link, element)

                        for child_key in element.ChildLinks:
                            child = repo.index_hash.get(child_key,None)
                            if child is None:
                                raise WorkBenchError('Repository object not found in op_pull', request.NOT_FOUND)
                            children.append(child)
                    elemets = children
        """
        print 'JKJKJKJKJKJKJKJKJKJ'

        yield self._process.reply_ok(msg, content=response)



    def _link_to_structure_element(self, link, element):
        """
        Utility method for use in push / pull methods to pass content without decoding it.
        """

        if link.key:
            raise WorkBenchError('Unexpected condition - link already set in link_to_structure_element.')

        # Set the id of the linked wrapper
        link.key = element.key

        # Set the type
        link.type.GPBMessage.CopyFrom(element.type)

        link.isleaf = element.isleaf

        # Add this link to the list of childlinks in the root of the object
        link.ChildLinks.add(link)

        # Add this structure element to the objects owned by the message
        link.Repository.index_hash[element.key] = element


    


    @defer.inlineCallbacks
    def push(self, origin, name_or_names):
        """
        Push the current state of the repository.
        When the operation is complete - the transfer of all objects in the
        repository is complete.
        """
        targetname = self._process.get_scoped_name('system', origin)
        
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

    
        #print 'PUSH TARGET: ',targetname
        try:
            content, headers, msg = yield self._process.rpc_send(targetname,'push', repo_or_repos)
        except ReceivedError, re:
            
            log.debug('ReceivedError', str(re))
            raise WorkBenchError('Push returned an exception! "%s"' % re.msg_content)
            
        defer.returnValue(content)

        
    @defer.inlineCallbacks
    def op_push(self, heads, headers, msg):
        """
        The Operation which responds to a push.
        
        Operation does not complete until transfer is complete!
        """
        log.info('op_push: received content: %s' % heads)
                
        #@TODO Add try catch and return value to this method!
                
        for head in heads:
            repo = self._load_repo_from_mutable(head)
                
            yield self._fetch_repo_objects(repo, headers.get('reply-to'))
            
            
        response = yield self._process.message_client.create_instance(MessageContentTypeID=None)
        response.MessageResponseCode = response.ResponseCodes.OK            
            
        # The following line shows how to reply to a message
        yield self._process.reply_ok(msg, response)
        log.info('op_push: Complete!')

        
    @defer.inlineCallbacks
    def _fetch_repo_objects(self, repo, origin):
        """
        This method does not have a return value? What should it be?
        """
        cref_links = set()
        for branch in repo.branches:
            
            for cref_link in branch.commitrefs.GetLinks():
                cref_links.add(cref_link)
            
        objs_to_get = set()
        refs_touched = set()
        while len(cref_links) > 0:
            
            new_links = set()
            
            for ref_link in cref_links:
                refs_touched.add(ref_link)
                
                cref= repo.get_linked_object(ref_link)
                    
                obj_link = cref.GetLink('objectroot')
                obj = repo.index_hash.get(obj_link.key,None)
                
                if not obj:
                    objs_to_get.add(obj_link)
                    
                    for pref in cref.parentrefs:
                        ref_link = pref.GetLink('commitref')
                        if not ref_link in refs_touched:
                            new_links.add(ref_link)
                
                # If we have that object, assume we have all the objects before it.
                
            cref_links = new_links
            
            
        # Recursively get the structure
        while len(objs_to_get) > 0:

            new_links = set()
            
            # Get the objects we don't have
            ### Call the method defined by the process - not the workbench!
            result = yield self._process.fetch_linked_objects(origin, objs_to_get)
            
            if not result:
                raise WorkBenchError('Fetch failed!')
            
            # Would like to have fetch use reply to - to keep the conversation context but does not work yet...
            #yield self.fetch_linked_objects(msg, objs_to_get)
            
            for link in objs_to_get:
                if not link.isleaf:
                    obj = repo.get_linked_object(link)
                    for child_link in obj.ChildLinks:
                        if repo.index_hash.get(child_link.key, None) is None:
                            # Use get not has key - make sure that it is added to the repository index
                            new_links.add(child_link)
            
            objs_to_get = new_links
    
    @defer.inlineCallbacks
    def fetch_linked_objects(self, address, links):
        """
        Fetch the linked objects from the data store service

        @TODO Update to new message pattern!
        """
            
        cs = object_utils.get_gpb_class_from_type_id(STRUCTURE_TYPE)()
            
        for link in links:
            se_raw = cs.items.add()
            
            se = gpb_wrapper.StructureElement(se_raw)
        
            # Can not set the pointer directly... must set the components
            se.value = link.SerializeToString()
            se.type.CopyFrom(link.ObjectType) # Copy is okay - this is small
            #se.key = gpb_wrapper.sha1hex(se.value)

            # Calculate the sha1 from the serialized value and type!
            se.key = se.sha1
            se.isleaf = link.isleaf # What does this mean in this context?
            
            
        objs, headers, msg = yield self._process.rpc_send(address,'fetch_linked_objects', cs)
        
        # put the dictionary of new objects into the hahsed elements list
        self._hashed_elements.update(objs)
            
        defer.returnValue(objs)
        return
        
            
    @defer.inlineCallbacks
    def op_fetch_linked_objects(self, elements, headers, message):
        """
        Send a linked object back to a requestor if you have it!

        @TODO Update to new message pattern!
        
        """
        log.info('op_fetch_linked_objects: received content type, %s; \n Elements: %s' % (type(elements), str(elements)))
        cs = object_utils.get_gpb_class_from_type_id(STRUCTURE_TYPE)()
        
        # Elements is a dictionary of wrapped structure elements
        for se in elements.values():
            
            assert se.type == LINK_TYPE, 'This is not a link element!'
    
            link = object_utils.get_gpb_class_from_type_id(LINK_TYPE)()
            link.ParseFromString(se.value)

            se = cs.items.add()
        
            item = self._hashed_elements.get(link.key,None)
            
            if not item:
                raise WorkBenchError('Requested object not found!')
                    
            assert item.type == link.type, 'Link type does not match item type!'
            assert item.isleaf == link.isleaf, 'Link isleaf does not match item isleaf!'
            assert item.key == link.key, 'Link key does not match item key!'
            
            # Can not set the pointer directly... must set the components
            se.value = item.value

            se.key = item.key
            se.isleaf = item.isleaf # What does this mean in this context?
            se.type.CopyFrom(item.type) # Copy is okay - this is small
        
        yield self._process.reply(message,content=cs)
        log.info('op_fetch_linked_objects: Complete!')
        

        
        
    def list_repository_commits(self, repo):


        if not repo.status == repo.UPTODATE:
            comment='Commiting to send message with wrapper object'
            repo.commit(comment=comment)

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



    def _load_repo_from_mutable(self,head):
        """
        Load a repository from a mutable - helper for push and pull - methods
        that send and receive an entire repo.
        head is a raw (unwrapped) gpb message
        """
        log.debug('_load_repo_from_mutable: Loading a repository!')
        new_repo = repository.Repository(head=head)
                
        new_repo._workbench = self
            
        new_repo.index_hash.cache = self._workbench_cache
        
        
        # Load all of the commit refs that came with this head.
        
        for branch in new_repo.branches:

            # Check for merge on read condition!            
            for link in branch.commitrefs.GetLinks():
                self._load_commits(new_repo,link)
            
            
        # Check and see if we already have one of these repositories
        existing_repo = self.get_repository(new_repo.repository_key)
        if existing_repo:
            
            # Merge the existing head with the new one.
            repo = self._merge_repo_heads(existing_repo, new_repo)
            
        else:
            repo = new_repo
            
        if not repo.branchnicknames.has_key('master'):
            repo.branchnicknames['master']=repo.branches[0].branchkey
        
        self.put_repository(repo)
        log.debug('_load_repo_from_mutable: returning repo:\n'+str(repo))
        return repo 
            
    
    def _load_commits(self, repo, link):
                
        log.debug('_load_commits: Loading all commits in the repository')
        
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
            self._load_commits(repo, link)
        
        log.debug('_load_commits: Loaded all commits!')
    
    def _merge_repo_heads(self, existing_repo, new_repo):
        
        log.debug('_merge_repo_heads: merging the state of repository heads!')
        log.debug('existing repository:\n'+str(existing_repo))
        log.debug('new_repo:\n'+str(new_repo))
        # examine all the branches in new and merge them into existing
        for new_branch in new_repo.branches:
            
            new_branchkey = new_branch.branchkey
            
            for existing_branch in existing_repo.branches:
                
                if new_branchkey == existing_branch.branchkey:
                    # We need to merge the state of these branches
                    
                    self._resolve_branch_state(existing_branch, new_branch)
                        
                    # We found the new branch in existing - exit the outter for loop
                    break
                
            else:
                    
                # the branch in new is not in existing - add its head and move on
                branch = existing_repo.branches.add()
                
                branch.branchkey = new_branchkey
                # Get the cref and then link it in the existing repository
                for cref in new_branch.commitrefs:
                    bref = branch.commitrefs.add()
                    bref.SetLink(cref)
                
        log.debug('_merge_repo_heads: returning merged repository:\n'+str(existing_repo))
        return existing_repo
    
    
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
                existing_repo = existing_branch.Repository
                new_repo = new_branch.Repository
            
                # test to see if we these are the same head ref!
                if new_link == existing_link:
                    # If these branches have the same state we are good - continue to the next new cref in the new branch. 
                    break
                    
                # Look in the commit index of the existing repo to see if the new link is an old commit to existing
                elif existing_repo._commit_index.has_key(new_link.key):
                    # The branch in new_repo is out of date with what exists here.
                    # We can completely ignore the new link!
                    break   
                    
                # Look in the commit index of the new repo to see if the existing link is an old commit in new repository 
                elif new_repo._commit_index.has_key(existing_link.key):
                    # The existing repo can be fast forwarded to the new state!
                    # But we must keep looking through the existing_links to see if the push merges our state!
                    found = True
                    existing_link.key = new_link.key # Cheat and just copy the key!
                    self._load_commits(existing_repo, new_link) # Load the new ancestors!
                
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