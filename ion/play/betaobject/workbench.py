#!/usr/bin/env python
"""
@Brief Workbench for operating on GPB backed objects
"""

from ion.play.betaobject import repository

class WorkBench(object):
 
    def __init__(self, process):   
    
        self._process = process
        
        self._repos = {}
        
        self.repo_cntr = 0
        
        
    def clone(self, ID_Ref, name=None):
        """
        Clone a repository from the data store
        Check out the head or 
        """
        # rpc_send - datastore, clone, ID_REf
        
        
    def init(self, gpbtype, name=None):
        """
        Initialize a new repository
        """
        if not name:
            name = 'repo_%s' % self._repo_cntr
            self._repo_cntr + 1
            
        if name in self._repos.keys():
            raise Exception, 'Can not initialize a new repository with an existing name'
        
        repo = self._create_repo(self, gpbtype)
        
        self._repos[name] = repo
        
        return repo
    
    def _create_repo(self, rootclass):
        """
        Factory method for creating a repository - this is the responsibility
        of the workbench.
        """
        inst = repository.Repository()
        inst._workbench = self
        
        # Now create the root object - this is the responsibility of the repo
        root_obj = inst._create_wrapper_object(rootclass)

        # Because it is the root object of a new repo - set it
        inst._workspace_root = root_obj
        
        
        
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
        
        
        
        