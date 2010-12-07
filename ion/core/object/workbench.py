#!/usr/bin/env python
"""
@file ion/core/object/workbench.py
@author David Stuebe
@Brief Workbench for operating on GPB backed object structures

TODO
Add persistent store to the work bench. Use it fetch linked objects
"""

from twisted.internet import defer

from ion.core.object import repository
from ion.core.object import gpb_wrapper

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from net.ooici.core.container import container_pb2
from net.ooici.core.mutable import mutable_pb2
from net.ooici.core.type import type_pb2
from net.ooici.core.link import link_pb2

class WorkBenchError(Exception):
    """
    An exception class for errors that occur in the Object WorkBench class
    """

class WorkBench(object):
 
    MutableClassType = gpb_wrapper.set_type_from_obj(mutable_pb2.MutableNode())
    LinkClassType = gpb_wrapper.set_type_from_obj(link_pb2.CASRef())
    CommitClassType = gpb_wrapper.set_type_from_obj(mutable_pb2.CommitRef())
 
    def __init__(self, myprocess):   
    
        self._process = myprocess
        
        self._repos = {}
        
        self._repository_nicknames = {}
        
        """
        A dictionary - shared between repositories for hashed objects
        """  
        self._hashed_elements={}
        
        """
        Blobs of data are persisted here when the workspace is initialized with
        a persistent back end
        """
        self._blob_store = None
        
        
        """
        The mutable head and the commits are presisted here when the workspace is
        initialized with a persistent back end
        """
        self._head_store = None
      
        
    def init_repository(self, rootclass=None, nickname=None):
        """
        Initialize a new repository
        Factory method for creating a repository - this is the responsibility
        of the workbench.
        """
        
        repo = repository.Repository()
        repo._workbench = self
            
        repo._hashed_elements = self._hashed_elements
            
        # Set the default branch
        repo.branch(nickname='master')
           
        if rootclass:
            rootobj = repo.create_wrapped_object(rootclass)
        
            repo._workspace_root = rootobj
        
        else:
            rootobj = None
        
        self.put_repository(repo)
        
        if nickname:
            self.set_repository_nickname(repo.repository_key, nickname)
        
        return repo, rootobj
        
    def set_repository_nickname(self,repositorykey, nickname):
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
        
    def put_repository(self,repo):
        
        self._repos[repo.repository_key] = repo
        
        
    def fork(self, structure, name):
        """
        Fork the structure in the wrapped gpb object into a new repository.
        """
         
    @defer.inlineCallbacks
    def pull(self, origin, repo_name):
        """
        Pull the current state of the repository
        """
        targetname = self._process.get_scoped_name('system', origin)
        
        #print 'PULL Targetname: ', targetname
        #print 'PULL RepoName: ', repo_name
        
        repo, headers, msg = yield self._process.rpc_send(targetname,'pull', repo_name)
        
        response = headers.get(self._process.MSG_RESPONSE)
        exception = headers.get(self._process.MSG_EXCEPTION)
        status = headers.get(self._process.MSG_STATUS)
        
        # Handle the response if the repo was not found
        if response != self._process.ION_SUCCESS:
            defer.returnValue((response, exception))
            
        # should have a return value to make sure this worked... ?
        yield self._fetch_repo_objects(repo, headers.get('reply-to'))
        
        defer.returnValue((response, exception))
        
    @defer.inlineCallbacks
    def op_pull(self,content, headers, msg):
        """
        The operation which responds to a pull 
        """
        
        #print 'Received pull request, id:', content
        
        repo = self.get_repository(content)
        
        if repo:
            yield self._process.reply(msg,content=repo)
        else:
            yield self._process.reply(msg,response_code=self._process.APP_RESOURCE_NOT_FOUND)
        
        
    @defer.inlineCallbacks
    def push(self, origin, name):
        """
        Push the current state of the repository
        """
        targetname = self._process.get_scoped_name('system', origin)
        repo = self.get_repository(name)

        if repo:
            
            #print 'PUSH TARGET: ',targetname
            content, headers, msg = yield self._process.rpc_send(targetname,'push', repo)
        
            response = headers.get(self._process.MSG_RESPONSE)
            exception = headers.get(self._process.MSG_EXCEPTION)
        
            status = headers.get(self._process.MSG_STATUS)
        else:
            status = 'OK'
            response = 'Repository name %s not found in work bench to push!' % name
            exception = ''

        if status == 'OK':
            log.info( 'Push returned:'+response)
            defer.returnValue((response, exception))

        else:
            raise Exception, 'Push returned an exception!' % exception
            

        
    @defer.inlineCallbacks
    def op_push(self, repo, headers, msg):
        """
        The Operation which responds to a push
        """
        log.info('op_push: received content type, %s' % type(repo))
                
        yield self._fetch_repo_objects(repo, headers.get('reply-to'))
            
        # The following line shows how to reply to a message
        yield self._process.reply(msg)
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
                obj = self._hashed_elements.get(obj_link.key,None)
                
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
            yield self.fetch_linked_objects(origin, objs_to_get)
            
            # Would like to have fetch use reply to - to keep the conversation context but does not work yet...
            #yield self.fetch_linked_objects(msg, objs_to_get)
            
            for link in objs_to_get:
                if not link.isleaf:
                    obj = repo.get_linked_object(link)
                    for child_link in obj.ChildLinks:
                        if not self._hashed_elements.has_key(child_link.key):
                            new_links.add(child_link)
            
            objs_to_get = new_links
    
    @defer.inlineCallbacks
    def fetch_linked_objects(self, address, links):
        """
        Fetch the linked objects from the data store service
        """     
            
        cs = container_pb2.Structure()
            
        for link in links:
            se = cs.items.add()
        
            # Can not set the pointer directly... must set the components
            se.value = link.SerializeToString()
            
            se.key = gpb_wrapper.sha1hex(se.value)
            se.isleaf = link.isleaf # What does this mean in this context?
            se.type.CopyFrom(link.GPBType) # Copy is okay - this is small
            
        if isinstance(address, str):
            objs, headers, msg = yield self._process.rpc_send(address,'fetch_linked_objects', cs)
        #elif hasattr(address, 'payload'):
        #    # Would like to have fetch use reply to - to keep the conversation context but does not work yet...
        #    objs, headers, reply_msg = yield self._process.reply(address, operation='fetch_linked_objects', content=cs)
        
        for obj in objs:
            self._hashed_elements[obj.key]=obj
        return
        
            
    @defer.inlineCallbacks
    def op_fetch_linked_objects(self, elements, headers, message):
        """
        Send a linked object back to a requestor if you have it!
        """
        log.info('op_fetch_linked_objects: received content type, %s' % type(elements))
        cs = container_pb2.Structure()
                
        for se in elements:
            
            assert se.type == self.LinkClassType, 'This is not a link element!'
    
            link = link_pb2.CASRef()
            link.ParseFromString(se.value)

            se = cs.items.add()
        
            item = self._hashed_elements.get(link.key,None)
            
            if not item:
                raise WorkBenchError('Requested object not found!')
                    
            assert item.type == link.type, 'Link type does not match item type!'
            assert item.isleaf == link.isleaf, 'Link isleaf does not match item isleaf!'
        
            # Can not set the pointer directly... must set the components
            se.value = item.value

            se.key = item.key
            se.isleaf = item.isleaf # What does this mean in this context?
            se.type.CopyFrom(item.type) # Copy is okay - this is small
        
        yield self._process.reply(message,content=cs)
        log.info('op_fetch_linked_objects: Complete!')
        
    def pack_repository_commits(self,repo):
        """
        pack just the mutable head and the commits!
        By default send all commits in the history. Too damn complex on the other
        side to deal with merge otherwise.
        """
        log.debug('pack_repository_commits: Packing repository:\n'+str(repo))
        mutable = repo._dotgit
        
        # Create the Structure Element for the mutable head
        structure = {}
        mutable.RecurseCommit(structure)
        root_obj = structure.get(mutable.MyId)
        # Set it back to modified as soon as we are done!
        mutable.Modified = True
        mutable.MyId = repo.new_id()


        #print 'ROOT Obj', root_obj
            
        cref_set = set()
        for branch in mutable.branches:
            
            for cref in branch.commitrefs:
                cref_set.add(cref)
            
        obj_set = set()
            
        while len(cref_set)>0:                
            new_set = set()
                        
            for cref in cref_set:
                obj_set.add(cref.MyId)
                    
                for prefs in cref.parentrefs:
                    new_set.add(prefs.commitref)
            
            # Now recurse on the ancestors    
            cref_set = new_set
            
            
        # Now make a list of just the keys that we want to send!
        obj_list = []
        for key in obj_set:
            obj_list.append(key)
                
        serialized = self._pack_container(root_obj, obj_list)
        log.debug('pack_repository_commits: Packing Complete!')
        return serialized
                
        
        
    def pack_structure(self, wrapper, include_leaf=True):
        """
        Pack all children of the wrapper stucture into a message. Stop at the leaf
        links if include_leaf=False.
        Return the content as a container object.
        """
        log.debug('pack_structure: Packing wrapper:\n'+str(wrapper))
        
        if not isinstance(wrapper, gpb_wrapper.Wrapper):
            raise WorkBenchError('Pack Structure received a wrapper argument which is not a wrapper?')
        
        repo = wrapper.Repository
        
        if not repo.status == repo.UPTODATE:
            repo.commit(comment='Sending message with wrapper %s'% wrapper.MyId)
        
        obj_set=set()
        root_obj = None
        obj_list = []
        
        # If we are sending the mutable head object
        if wrapper is repo._dotgit:
            structure = {}
            wrapper.RecurseCommit(structure)
            root_obj = structure.get(wrapper.MyId)
            
            items = set()
            for branch in wrapper.branches:
                
                for cref in branch.commitrefs:
                    obj = self._hashed_elements.get(cref.MyId,None)
                    if not obj:
                        # Debugging exception - remove later
                        raise WorkBenchError('Hashed CREF not found! Please call David')
                    items.add(obj)
            
        else:
            # Else we are sending just the commited root object
            root_obj = self._hashed_elements.get(wrapper.MyId,None)
            items = set([root_obj])

        
        # Recurse through the DAG and add the keys to a set - obj_set.
        while len(items) > 0:
            child_items = set()
            for item in items:
                
                if len(item.ChildLinks) >0:
                    
                    obj_set.add(item.key)    
                    
                    for key in item.ChildLinks:
                    
                        obj = self._hashed_elements.get(key,None)
                        if not obj:
                            # Debugging exception - remove later
                            raise WorkBenchError('Hashed CREF not found! Please call David')
                    
                        child_items.add(obj)
                        
                elif include_leaf:
                    obj_set.add(item.key)
                    
            items = child_items

        if root_obj.key in obj_set:
            #Make a list in the right order        
            obj_set.discard(root_obj.key)

        for key in obj_set:
            obj_list.append(key)
        
        #print 'OBJLIST',obj_list
        
        serialized = self._pack_container(root_obj, obj_list)
        log.debug('pack_structure: Packing Complete!')
        return serialized
        
    
    def _pack_container(self, head, object_keys):
        """
        Helper for the sender to pack message content into a container in order
        """
        log.debug('_pack_container: Packing container head and object_keys!')
        # An unwrapped GPB Structure message to put stuff into!
        cs = container_pb2.Structure()
        
        cs.head.key = head._element.key
        cs.head.type.CopyFrom(head._element.type)
        cs.head.isleaf = head._element.isleaf
        cs.head.value = head._element.value
                        
        for key in object_keys:
            hashed_obj = self._hashed_elements.get(key)         
            gpb_obj = hashed_obj._element
                        
            se = cs.items.add()
        
            # Can not set the pointer directly... must set the components
            se.key = gpb_obj.key
            se.isleaf = gpb_obj.isleaf
            se.type.CopyFrom(gpb_obj.type) # Copy is okay - this is small
            se.value = gpb_obj.value # Let python's object manager keep track of the pointer to the big things!
        
        
        log.debug('_pack_container: Packed container!')
        serialized = cs.SerializeToString()
        
        return serialized
        
    def unpack_structure(self, serialized_container):
        """
        Take a container object and load a repository with its contents
        May want to provide more arguments to give this new repository a special
        name based on the 
        """
        log.debug('unpack_structure: Unpacking Structure!')
        head, obj_list = self._unpack_container(serialized_container)
        
        assert len(obj_list) > 0, 'There should be objects in the container!'

        
        if not head:
            # Only fetch links should hit this!
            log.debug('unpack_structure: returning obj_list:'+str(obj_list))
            return obj_list
        
        if head.type == self.MutableClassType:
            
            # This is a pull or clone and we don't know the context here.
            # Return the mutable head as the content and let the process
            # operation figure out what to do with it!
                        
            for item in obj_list:
                self._hashed_elements[item.key]=item
            
            repo = self._load_repo_from_mutable(head)
            log.debug('unpack_structure: returning repository:'+str(repo))
            return repo
        
        else:
                
            for item in obj_list:
                self._hashed_elements[item.key]=item
            
            # Create a new repository for the structure in the container
            repo, none = self.init_repository()
                
           
            # Load the object and set it as the workspace root
            root_obj = repo._load_element(head)
            repo._workspace_root = root_obj
            repo._workspace[root_obj.MyId] = root_obj

            # Create a commit to record the state when the message arrived
            cref = repo.commit(comment='Message for you Sir!')

            # Now load the rest of the linked objects - down to the leaf nodes.
            repo._load_links(root_obj)
            
            
            log.debug('unpack_structure: returning root_obj:'+str(root_obj))
            return root_obj
        
        
        
    def _unpack_container(self,serialized_container):
        """
        Helper for the receiver for unpacking message content
        Returns the content as a list of ids in order now in the workbench
        hashed elements dictionary
        """
            
        log.debug('_unpack_container: Unpacking Container')
        # An unwrapped GPB Structure message to put stuff into!
        cs = container_pb2.Structure()
            
        cs.ParseFromString(serialized_container)
                
        # Return arguments
        head = None
        obj_list=[]
        
        if cs.HasField('head'):
            # The head field is optional - if not included this is a fetch links op            
            head = gpb_wrapper.StructureElement.wrap_structure_element(cs.head)
            #self._hashed_elements[head.key]=head
            obj_list.append(head)
            
        for se in cs.items:
            wse = gpb_wrapper.StructureElement.wrap_structure_element(se)
            
            #self._hashed_elements[wse.key]=wse
            #obj_list.append(wse.key)
            obj_list.append(wse)
        
        log.debug('_unpack_container: returning head:\n'+str(head))
        log.debug('_unpack_container: returning obj_list:\n'+str(obj_list))
        
        return head, obj_list
        
    def _load_repo_from_mutable(self,head):
        """
        Load a repository from a mutable - helper for clone and other methods
        that send and receive an entire repo.
        head is a raw (unwrapped) gpb message
        """
        log.debug('_load_repo_from_mutable: Loading a repository!')
        new_repo = repository.Repository(head)
                
        new_repo._workbench = self
            
        new_repo._hashed_elements = self._hashed_elements
        
        
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
        if repo._commit_index.has_key(link.key):
            return repo._commit_index.get(link.key)

        elif repo._hashed_elements.has_key(link.key):
            
            element = repo._hashed_elements.get(link.key)
            
            
            if not link.type.package == element.type.package and \
                    link.type.cls == element.type.cls:
                raise WorkBenchError('The link type does not match the element type!')
            
            cref = repo._load_element(element)
            
            if cref.GPBType == self.CommitClassType:
                repo._commit_index[cref.MyId]=cref
                cref.ReadOnly = True
            else:
                raise WorkBenchError('This method should only load commits!')
            
            for parent in cref.parentrefs:
                link = parent.GetLink('commitref')
                # Call this method recursively for each link
                self._load_commits(repo, link)
        else:
            raise WorkBenchError('Commit id not found: %s' % link.key)
            # This commit ref was not actually sent!
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
                    bref = branch.commitref.add()
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