#!/usr/bin/env python
"""
@Brief Workbench for operating on GPB backed objects
"""

from ion.core.object import repository
from ion.core.object import gpb_wrapper

from net.ooici.core.container import container_pb2

class WorkBench(object):
 
    def __init__(self, process):   
    
        self._process = process
        
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
        
        
    def push(self, name):
        """
        Push the current state of the repository
        """
        
    def pull(self,name):
        """
        Pull the current state of the repository
        """
        
    
        
    def fetch_linked_objects(self, links):
        """
        Fetch the linked objects from the data store service
        """
        return None
    
    
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
        
        root_obj = self._hashed_elements.get(wrapper.myid,None)
        
        # Recurse through the Dag and add the keys to a set - obj_set.
        items = set([root_obj])
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

        #Make a list in the right order        
        obj_list=[root_obj.key] # Start with the root!
        obj_set.discard(root_obj.key)
        for key in obj_set:
            obj_list.append(key)
        
        
        serialized = self._pack_container(obj_list)
        
        return serialized
        
        
        
    
    def _pack_container(self, object_keys, mutable=None):
        """
        Helper for the sender to pack message content into a container in order
        """
        
        # An unwrapped GPB Structure message to put stuff into!
        cs = container_pb2.Structure()
        
        if mutable:
            # Hack this for now...
            cs.mutablehead.CopyFrom(mutable)
        
        
        for key in object_keys:
            hashed_obj = self._hashed_elements.get(key)         
            gpb_obj = hashed_obj._element
            
            
            se = cs.items.add()
        
            # Can not set the pointer directly... must set the components
            se.key = gpb_obj.key
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
        
        obj_list, mutablehead = self._unpack_container(serialized_container)
        
        if mutablehead:
            
            # This is a pull or clone and we don't know the context here.
            # Return the mutable head as the content and let the process
            # operation figure out what to do with it!
            return mutablehead
        
        else:
            
            assert len(obj_list) > 0, 'There should be objects in the container!'
            
            # Create a new repository for the structure in the container
            repo, none = self.init_repository()
                
                
            # Get the root object - the first in the list
            se = self._hashed_elements.get(obj_list[0])
                        
            # Load the object and set it as the workspace root
            root_obj = repo._load_element(se)
            repo._workspace_root = root_obj

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
        
        mh = None
        if cs.HasField('mutablehead'):
            mh = cs.mutablehead
    
        return obj_list, mh
        