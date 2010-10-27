#!/usr/bin/env python
"""
@Brief Workbench for operating on GPB backed objects

TODO
Remove repository name - what to do instead?

"""

from twisted.internet import defer

from ion.core.object import repository
from ion.core.object import gpb_wrapper


import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from net.ooici.core.container import container_pb2
from net.ooici.core.mutable import mutable_pb2
from net.ooici.core.type import type_pb2

class WorkBench(object):
 
 
    MutableClassType = type_pb2.GPBType()
    MutableClassType.protofile = mutable_pb2.MutableNode.DESCRIPTOR.file.name.split('/')[-1]
    MutableClassType.package = mutable_pb2.MutableNode.DESCRIPTOR.file.package
    MutableClassType.cls = mutable_pb2.MutableNode.DESCRIPTOR.name
 
 
    def __init__(self, myprocess):   
    
        self._process = myprocess
        
        self._repos = {}
        
        self._repo_cntr = 0
        
        self._hashed_elements={}
        """
        A shared dictionary for hashed objects
        """            
        
        
    def clone(self, ID_Ref, name=None):
        """
        Clone a repository from the data store
        Check out the head or 
        """
        # rpc_send - datastore, clone, ID_REf
    
    def _load_repo_from_mutable(self,head):
        """
        Load a repository from a mutable - helper for clone and other methods
        that send and receive an entire repo.
        mutable is a raw (unwrapped) gpb message
        """
        
        
        repo = repository.Repository(head)
                
        repo._workbench = self
            
        repo._hashed_elements = self._hashed_elements
        
        
        mutable = repo._dotgit
        # Load all of the commit refs that came with this head.
        repo._load_links(mutable)
        
        name = None
        if mutable.HasField('name') and mutable.name not in self._repos.keys():
            name = mutable.name
        else:
            name = 'repo_%s' % self._repo_cntr
            self._repo_cntr += 1
        
        
        self._repos[name] = repo
        
        return repo 
            
    
    
        
    def init_repository(self, rootclass=None, name=None):
        """
        Initialize a new repository
        Factory method for creating a repository - this is the responsibility
        of the workbench.
        """
        if not name:
            name = 'repo_%s' % self._repo_cntr
            self._repo_cntr += 1
            
        if name in self._repos.keys():
            raise Exception, 'Can not initialize a new repository with an existing name'
        
        repo = repository.Repository()
        repo._workbench = self
            
        repo._hashed_elements = self._hashed_elements
            
        # Set the default branch
        repo.branch('master')
           
        if rootclass:
            rootobj = repo.create_wrapped_object(rootclass)
        
            repo._workspace_root = rootobj
        
        else:
            rootobj = None
        
        self._repos[name] = repo
        
        return repo, rootobj

        
    def fork(self, structure, name):
        """
        Fork the structure in the wrapped gpb object into a new repository.
        """
        
    @defer.inlineCallbacks
    def push(self, name, target):
        """
        Push the current state of the repository
        """
        #targetname = self._process.get_scoped_name('system', target)
        #print 'TARGET NAME', targetname
        (content, headers, msg) = yield self._process.rpc_send(target,'push', self._repos[name])
        
        print 'CONTENT',type(content), content
        
    @defer.inlineCallbacks
    def op_push(self, content, headers, msg):
        log.info('op_push: '+str(content))
        

        # The following line shows how to reply to a message
        yield self._process.reply(msg, 'result',{'value':'Hello there, '+str(content)}, {})
        
        
    def pull(self,name):
        """
        Pull the current state of the repository
        """
        
    
        
    def fetch_linked_objects(self, links):
        """
        Fetch the linked objects from the data store service
        """
        raise Exception, 'Fetch Linked Objects is not implemented!'
    
    
    def get_repository(self,name):
        """
        Simple getter for the repository dictionary
        """
        return self._repos.get(name,None)
        
    def list_repositories(self):
        """
        Simple list tool for repository names
        """
        return self._repos.keys()
        
    def pack_repository_commits(self,repo, depth=5):
        """
        pack just the mutable head and the commits!
        """

        assert repo in self._repos.values(), 'This repository is not in the process workbench!'
        
        mutable = repo._dotgit
        # Get the Structure Element for the mutable head
        structure = {}
        mutable._recurse_commit(structure)
        root_obj = structure.get(mutable.myid)
            
        cref_set = set()
        for branch in mutable.branches:
            cref = branch.commitref
            
            # Keep track of the commits
            cref_set.add(cref)
            
        obj_set = set()
            
        assert isinstance(depth, (int, long)), 'Pack_Repository_Commits: Depth must be an integer type'

        while depth != 0:
                
            new_set = set()
                
            for commit in cref_set:
                obj_set.add(cref.myid)
                    
                for anc in commit.ancestors:
                    new_set.add(anc.commitref)
            
            # Now recurse on the ancestors    
            cref_set = new_set
            
            depth -= 1
            
        obj_list = []
        for key in obj_set:
            obj_list.append(key)
                
        serialized = self._pack_container(root_obj, obj_list)
        
        return serialized
                
        
        
    def pack_structure(self, wrapper, include_leaf=True):
        """
        Pack all children of the wrapper stucture into a message. Stop at the leaf
        links if include_leaf=False.
        Return the content as a container object.
        """
        assert isinstance(wrapper, gpb_wrapper.Wrapper), 'Pack Structure received a wrapper argument which is not a wrapper?'
        
        repo = wrapper.repository
        assert repo in self._repos.values(), 'This object is not in the process workbench!'
        
        if not repo.status == repo.UPTODATE:
            repo.commit(comment='Sending message with wrapper %s'% wrapper.myid)
        
        obj_set=set()
        root_obj = None
        obj_list = []
        
        # If we are sending the mutable head object
        if wrapper is repo._dotgit:
            structure = {}
            wrapper._recurse_commit(structure)
            root_obj = structure.get(wrapper.myid)
            
            items = set()
            for branch in wrapper.branches:
                cref = branch.commitref
                obj = self._hashed_elements.get(cref.myid,None)
                if not obj:
                    # Debugging exception - remove later
                    raise Exception, 'Hashed CREF not found! Please call David'
                items.add(obj)
            
        else:
            # Else we are sending just the commited root object
            root_obj = self._hashed_elements.get(wrapper.myid,None)
            items = set([root_obj])

        
        # Recurse through the DAG and add the keys to a set - obj_set.
        while len(items) > 0:
            child_items = set()
            for item in items:
                
                if len(item._child_links) >0:
                    
                    obj_set.add(item.key)    
                    
                    for key in item._child_links:
                    
                        obj = self._hashed_elements.get(key,None)
                        if not obj:
                            # Debugging exception - remove later
                            raise Exception, 'Hashed element not found! Please call David'
                    
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
        
        return serialized
        
    
    def _pack_container(self, head, object_keys):
        """
        Helper for the sender to pack message content into a container in order
        """
        
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
        
        
        
        serialized = cs.SerializeToString()
        
        return serialized
        
        
        
        
    def unpack_structure(self, serialized_container):
        """
        Take a container object and load a repository with its contents
        May want to provide more arguments to give this new repository a special
        name based on the 
        """
        
        head, obj_list = self._unpack_container(serialized_container)
        
        if head.type == self.MutableClassType:
            
            # This is a pull or clone and we don't know the context here.
            # Return the mutable head as the content and let the process
            # operation figure out what to do with it!
                        
            repo = self._load_repo_from_mutable(head)
            
            return repo
        
        else:
            
            assert len(obj_list) > 0, 'There should be objects in the container!'
            
            # Create a new repository for the structure in the container
            repo, none = self.init_repository()
                
           
            # Load the object and set it as the workspace root
            root_obj = repo._load_element(head)
            repo._workspace_root = root_obj
            repo._workspace[root_obj.myid] = root_obj

            # Use the helper method to make a commit ref to our new object root
            cref = repo._create_commit_ref(comment='Message for you Sir!')
            
            # Set the current (master) branch to point at this commit
            brnch = repo._current_branch
            brnch.commitref = cref
            
            # Now load the rest of the linked objects - down to the leaf nodes.
            repo._load_links(root_obj)
            
            return root_obj
        
        
        
    def _unpack_container(self,serialized_container):
        """
        Helper for the receiver for unpacking message content
        Returns the content as a list of ids in order now in the workbench
        hashed elements dictionary
        """
            
        # An unwrapped GPB Structure message to put stuff into!
        cs = container_pb2.Structure()
            
        cs.ParseFromString(serialized_container)
                
        obj_list=[]
        for se in cs.items:
            wse = gpb_wrapper.StructureElement.wrap_structure_element(se)
            
            self._hashed_elements[wse.key]=wse
            obj_list.append(wse.key)
        
        head = gpb_wrapper.StructureElement.wrap_structure_element(cs.head)
        self._hashed_elements[head.key]=head
    
        return head, obj_list
        