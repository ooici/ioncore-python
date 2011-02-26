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

from google.protobuf.internal import decoder

from ion.core.object import object_utils
from ion.core.object import repository
from ion.core.object import gpb_wrapper

from ion.core.exception import ReceivedError
from ion.core.object.gpb_wrapper import OOIObjectError

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from net.ooici.core.container import container_pb2

structure_element_type = object_utils.create_type_identifier(object_id=1, version=1)
structure_type = object_utils.create_type_identifier(object_id=2, version=1)

idref_type = object_utils.create_type_identifier(object_id=4, version=1)
gpbtype_type = object_utils.create_type_identifier(object_id=9, version=1)


class WorkBenchError(Exception):
    """
    An exception class for errors that occur in the Object WorkBench class
    """

class WorkBench(object):
 
    MutableClassType = object_utils.create_type_identifier(object_id=6, version=1)
    LinkClassType = object_utils.create_type_identifier(object_id=3, version=1)
    CommitClassType = object_utils.create_type_identifier(object_id=8, version=1)
    
    
    def __init__(self, myprocess):   
    
        self._process = myprocess
        
        self._repos = {}
        
        self._repository_nicknames = {}
        
        """
        A dictionary - shared between repositories for hashed objects
        """  
        self._hashed_elements={}
      
      
    def create_repository(self, root_type=None, nickname=None):
        """
        New better method to initialize a repository.
        The init_repository method returns both the repo and the root object.
        This is awkward. Now that the repository has a root_object property, it
        is better to just return the repository.
        """
        repo = repository.Repository()
        repo._workbench = self
            
        repo._hashed_elements = self._hashed_elements
            
        # Set the default branch
        repo.branch(nickname='master')
           
        # Handle options in the root class argument
        if isinstance(root_type, object_utils.get_gpb_class_from_type_id(gpbtype_type)):
            
            try:
                rootobj = repo.create_object(root_type)
            except object_utils.ObjectUtilException, ex:
                raise WorkBenchError('Invalid root object type identifier passed in create_repository. Unrecognized type: "%s"' % str(root_type))
        
            repo._workspace_root = rootobj
        
        elif root_type ==None:
            rootobj = None
        else:
            raise WorkBenchError('Invalid root type argument passed in create_repository')
        
        
        self.put_repository(repo)
        
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
       
    def reference_repository(self, repo_key, current_state=False):
    
        repo = self.get_repository(repo_key)
        
        if current_state == True:
            if repo.status != repo.UPTODATE:
                raise WorkBenchError('Can not reference the current state of a repository which has been modified but not committed')

        # Create a new repository to hold this data object
        repository = self.create_repository(idref_type)
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
    def pull(self, origin, repo_name):
        """
        Pull the current state of the repository
        """
        targetname = self._process.get_scoped_name('system', origin)
        
        #print 'PULL Targetname: ', targetname
        #print 'PULL RepoName: ', repo_name
        
        try:
            content, headers, msg = yield self._process.rpc_send(targetname,'pull', repo_name)
        except ReceivedError, re:
            
            log.debug('ReceivedError', str(re))
            #content = re[1]        
            raise WorkBenchError('Pull Operation faild with an exception: "%s"' % str(re))
            
            
        # it is a message instance...
        if hasattr(content, 'MessageResponseCode'):
            if content.MessageResponseCode == content.ResponseCodes.NOT_FOUND:
                defer.returnValue(content)
        else:
            # assume that we got a list of heads to load as repositories...
            heads = content
        
        
        for head in heads:
            repo = self._load_repo_from_mutable(head)
            
            # Where to get objects not yet transfered.
            repo.upstream['service'] = origin
            repo.upstream['process'] = headers.get('reply-to')
            
        # Later there will aways be a message instance, for now fake out the interface 
        response = yield self._process.message_client.create_instance(MessageContentTypeID=None)
        response.MessageResponseCode = response.ResponseCodes.OK
        defer.returnValue(response)
        
    @defer.inlineCallbacks
    def op_pull(self,content, headers, msg):
        """
        The operation which responds to a pull request
        If a process exposes this operation it must provide persistent storage
        for the objects it provides - the data store! When the op is complete
        only the repository and its commits have been transfered. The content
        will be lazy fetched as needed!
        """
        
        #print 'Received pull request, id:', content
        
        repo = self.get_repository(content)
        
        if repo:
            yield self._process.reply(msg,content=repo)
        else:
            
            response = yield self._process.message_client.create_instance(MessageContentTypeID=None)
            response.MessageResponseCode = response.ResponseCodes.NOT_FOUND
            
            yield self._process.reply_ok(msg, content=response)
        
        
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
            raise WorkBenchError('Push returned an exception! "%s"' % content.response_body)
            
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
                        if not self._hashed_elements.has_key(child_link.key):
                            new_links.add(child_link)
            
            objs_to_get = new_links
    
    @defer.inlineCallbacks
    def fetch_linked_objects(self, address, links):
        """
        Fetch the linked objects from the data store service
        """     
            
        cs = object_utils.get_gpb_class_from_type_id(structure_type)()
            
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
        
        # put the dictionary of new objects into the hased elements list
        self._hashed_elements.update(objs)
            
        defer.returnValue(objs)
        return
        
            
    @defer.inlineCallbacks
    def op_fetch_linked_objects(self, elements, headers, message):
        """
        Send a linked object back to a requestor if you have it!
        """
        log.info('op_fetch_linked_objects: received content type, %s; \n Elements: %s' % (type(elements), str(elements)))
        cs = object_utils.get_gpb_class_from_type_id(structure_type)()
        
        # Elements is a dictionary of wrapped structure elements
        for se in elements.values():
            
            assert se.type == self.LinkClassType, 'This is not a link element!'
    
            link = object_utils.get_gpb_class_from_type_id(self.LinkClassType)()
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
        
    def pack_repositories(self, repos):
        
        container_structure = object_utils.get_gpb_class_from_type_id(structure_type)()
        for repo in repos:
            log.debug('pack_repositories: Packing repository:\n'+str(repo))
            container_structure.MergeFrom(self._repo_to_structure(repo))

        #print 'CONTAINER',container_structure
        log.debug('pack_repositories: Packing Complete!')         
        serialized = container_structure.SerializeToString()
        
        return serialized
        
        
        
    def pack_repository(self,repo):
        
        log.debug('pack_repository: Packing repository:\n'+str(repo))
        container_structure = self._repo_to_structure(repo)
        
        log.debug('pack_repository: Packing Complete!')         
        serialized = container_structure.SerializeToString()
        return serialized
        
        
    def _repo_to_structure(self,repo):
        """
        pack just the mutable head and the commits!
        By default send all commits in the history. Too damn complex on the other
        side to deal with merge otherwise.
        """
        
        mutable = repo._dotgit
        
        root_obj = self.serialize_mutable(mutable)
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
                
        
        container_structure = self._pack_container(root_obj, obj_list)
        
        return container_structure
        
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
            comment='Commiting to send message with wrapper object'
            repo.commit(comment=comment)
            
        obj_set=set()
        root_obj = None
        obj_list = []
        
        # If we are sending the mutable head object
        if wrapper is repo._dotgit:
            root_obj = self.serialize_mutable(wrapper)
            
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
        
        container_structure = self._pack_container(root_obj, obj_list)
        log.debug('pack_structure: Packing Complete!')
        
        serialized = container_structure.SerializeToString()
        return serialized
        
    def serialize_mutable(self, mutable):
        """
        
        """
        if mutable._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        
        # Create the Structure Element in which the binary blob will be stored
        se = gpb_wrapper.StructureElement()        
        repo = mutable.Repository
        for link in  mutable.ChildLinks:
                                    
            if  repo._hashed_elements.has_key(link.key):
                child_se = repo._hashed_elements.get(link.key)

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
            
        
        
    
    def _pack_container(self, head, object_keys):
        """
        Helper for the sender to pack message content into a container in order
        Awkward interface. Head is wrapper object, object_keys is a list of keys.
        Should be all objects or all keys...
        """
        log.debug('_pack_container: Packing container head and object_keys!')
        # An unwrapped GPB Structure message to put stuff into!
        cs = object_utils.get_gpb_class_from_type_id(structure_type)()
        
        cs.heads.add()
        cs.heads[0].key = head._element.key
        cs.heads[0].type.CopyFrom(head._element.type)
        cs.heads[0].isleaf = head._element.isleaf
        cs.heads[0].value = head._element.value
                        
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
        return cs
        
    def unpack_structure(self, serialized_container):
        """
        Take a container object and load a repository with its contents
        May want to provide more arguments to give this new repository a special
        name based on the 
        """
        log.debug('unpack_structure: Unpacking Structure!')
        heads, obj_dict = self._unpack_container(serialized_container)
        
        assert len(obj_dict) > 0, 'There should be objects in the container!'

        
        if not heads:
            # Only fetch links should hit this!
            # There is not data structure or repository in this container,
            # just a bunch of blobs to work with in the op.
            log.debug('unpack_structure: returning dictionary of objects:'+str(obj_dict))
            return obj_dict
        
        

        # If it is not a fetch         
        # put the content into the hashed elements list
        self._hashed_elements.update(obj_dict)
        
        
        if heads[0].type == self.MutableClassType:
            for head in heads:
                assert head.type == self.MutableClassType, 'Invalid mixed head type!'
            return heads
        
        # This is a list of root objects - 
        results=[]
        for head in heads:
            # This is what generally happens when a message is recieved!
                            
            
            # Create a new repository for the structure in the container
            repo = self.create_repository()
                
            
            # Load the object and set it as the workspace root
            root_obj = repo._load_element(head)
            repo.root_object = root_obj
                
            # Create a commit to record the state when the message arrived
            cref = repo.commit(comment='Message for you Sir!')

            # Now load the rest of the linked objects - down to the leaf nodes.
            repo._load_links(root_obj)
            
            log.debug('unpack_structure: returning root_obj:'+str(root_obj))
            #repos.append(repo)
            results.append(root_obj)
        
        if len(results) == 1:
            return results[0]
        else:
            return results
        
        
        
    def _unpack_container(self,serialized_container):
        """
        Helper for the receiver for unpacking message content
        Returns the content as a list of ids in order now in the workbench
        hashed elements dictionary
        """
            
        log.debug('_unpack_container: Unpacking Container')
        # An unwrapped GPB Structure message to put stuff into!
        cs = object_utils.get_gpb_class_from_type_id(structure_type)()
            
        try:
            cs.ParseFromString(serialized_container)
        except decoder._DecodeError, de:
            log.debug('Received invalid content: "%s"' % str(serialized_container))
            raise WorkBenchError('Could not decode message content as a GPB container structure!')
                
        # Return arguments
        heads = []
        obj_dict={}
        
        if cs.heads:
            # The head field is optional - if not included this is a fetch links op            
            
            for raw_head in cs.heads:
                
                wrapped_head = gpb_wrapper.StructureElement(raw_head)
                #self._hashed_elements[head.key]=head
                obj_dict[wrapped_head.key] = wrapped_head
                heads.append(wrapped_head)
            
        for se in cs.items:
            wse = gpb_wrapper.StructureElement(se)
            
            obj_dict[wse.key] = wse
        
        log.debug('_unpack_container: returning head:\n'+str(heads))
        log.debug('_unpack_container: returning dictionary of objects:\n'+str(obj_dict))
        
        return heads, obj_dict
        
    def _load_repo_from_mutable(self,head):
        """
        Load a repository from a mutable - helper for push and pull - methods
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
        
        try:
            cref = repo.get_linked_object(link)
        except repository.RepositoryError, ex:
            log.debug(ex)
            raise WorkBenchError('Commit id not found while loding commits: \n %s' % link.key)
            # This commit ref was not actually sent!
            
        if cref.ObjectType != self.CommitClassType:
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