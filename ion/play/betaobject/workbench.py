#!/usr/bin/env python
"""
@Brief Workbench for operating on GPB backed objects
"""

class WorkBench(object):
 
    def __init__(self, process):   
    
        self._process = process
        
        
        self._repos = {}
        
        
    def clone(self, ID_Ref, name=None):
        """
        Clone a repository from the data store
        Check out the head or 
        """
        
    def init(self, gpbtype, name):
        """
        Initialize a new repository
        """
        
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
        
    def unpack(self,container):
        """
        Helper for the receiver for unpacking message content
        Returns the content as a checked out object.
        """

    def pack(self, name):
        """
        Helper for the sender to pack message content
        """
        
    
    def _get_repository(self,name):
        return self._repos.get(name,None)
        
    def _set_repository(self,name,    
        