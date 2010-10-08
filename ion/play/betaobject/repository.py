#!/usr/bin/env python


"""
@Brief Repository for managing data structures
"""
from net.ooici.core.link import link_pb2
from net.ooici.core.mutable import mutable_pb2
from net.ooici.core.container import container_pb2

from ion.play.betaobject import gpb_wrapper

class Repository(object):
    
    def __init__(self):
        
        self._object_counter=1
        """
        A counter object used by this class to identify content objects untill
        they are indexed
        """
        
        self._workspace = {}
        """
        A dictionary containing objects which are not yet indexed, linked by a
        counter refrence in the current workspace
        """
        
        self._workspace_root = None
        """
        Pointer to the current root object in the workspace
        """
        
        self._commit_index = {}
        """
        A dictionary containing the commit objects - all immutable content hashed
        """
        
        self._hashed_elements = {}
        """
        All content elements are stored here - from incoming messages and
        new commits - everything goes here. Now it can be decoded for a checkout
        or sent in a message.
        """
        
        
        self._dotgit = gpb_wrapper.Wrapper(mutable_pb2.MutableNode())
        """
        A specially wrapped Mutable GPBObject which tracks branches and commits
        """
        
        self._current_branch = None
        """
        The name of the current branch
        Branch names are generallly nonsense (uuid or some such)
        """
        
        self._stash = {}
        """
        A place to stash the work space under a saved name.
        """
        
        self._workbench=None
        """
        The work bench which this repository belongs to...
        """
        
        
        
    def checkout(self, branch=None, commit_id=None, older_than=None):
        """
        Check out a particular branch
        Specify a commit_id or a date
        """
        
    def commit(self, comment=None):
        """
        Commit the current workspace structure
        """
        
    def branch(self, name):
        """
        Switch to the branch <name> if it is exists and the workspace is clean
        Create a branch from the current location if the workspace i
        """
    
    def stash(self, name):
        """
        Stash the current workspace for later reference
        """
        
    def create_wrapped_object(self, rootclass):        
        
        obj_id = self.get_id()
        self._workspace[obj_id] = gpb_wrapper.Wrapper(self, rootclass(), obj_id)        
        
        
    def get_id(self):
        self._object_counter += 1
        return str(self.__object_counter)
 
        
        