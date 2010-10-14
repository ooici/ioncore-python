#!/usr/bin/env python
"""
@Brief Workbench for operating on GPB backed objects
"""

from ion.core.object import repository

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
        
        
    def init_repository(self, rootclass, name=None):
        """
        Initialize a new repository
        Factory method for creating a repository - this is the responsibility
        of the workbench.
        """
        if not name:
            name = 'repo_%s' % self._repo_cntr
            self._repo_cntr + 1
            
        if name in self._repos.keys():
            raise Exception, 'Can not initialize a new repository with an existing name'
        
        repo = repository.Repository()
        repo._workbench = self
            
        repo._hashed_elements = self._hashed_elements
            
        # Set the default branch
        repo.branch('master')
           
        rootobj = repo.create_wrapped_object(rootclass)
        
        repo._workspace_root = rootobj
        
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
        
    def unpack_container(self,container):
        """
        Helper for the receiver for unpacking message content
        Returns the content as a checked out object.
        """

    def pack_container(self, root_object):
        """
        Helper for the sender to pack message content
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
        
        

        
    def _load_repo(self, container):
        """
        Take a container object and load a repository with its contents
        """
        
        
        
        