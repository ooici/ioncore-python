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
        
        
        self._dotgit = self.create_wrapped_object(mutable_pb2.MutableNode)
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
        
    def create_wrapped_object(self, rootclass, obj_id=None):        
        
        if not obj_id:
            obj_id = self.new_id()
        obj = gpb_wrapper.Wrapper(self, rootclass(), obj_id)
        self._workspace[obj_id] = obj
        return obj
        
    def new_id(self):
        self._object_counter += 1
        return str(self._object_counter)
     
    def get_linked_object(self, link):
                
        if self._workspace.has_key(link.key):
            return self._workspace.get(link.key)
        elif self._hashed_elements.has_key(link.key):
            
            element = self._hashed_elements.get(link.key)
            
            # Make sure the type is the same!
            if not link.type == element.type:
                raise Exception, 'The link type does not match the element type!'
            
            cls = self._load_class_from_type(link.type)
                
            obj = self.create_wrapped_object(cls, link.key)
            
            obj.ParseFromString(element.value)
            return obj
        else:
            return self._workbench.fetch_linked_objects(link)
            
            
        
    def _load_class_from_type(self,ltype):
        module = ltype.protofile.split('.')[0] + '_pb2'
        cls_name = ltype.cls
                
        temp= __import__(ltype.package, fromlist=module)
        
        mod = getattr(temp,module)
        cls = getattr(mod, cls_name)
        return cls
        
        
    def _set_type_from_obj(self, ltype, wrapped_obj):
        
        msg = wrapped_obj._gpbMessage
                
        ltype.protofile = msg.DESCRIPTOR.file.name.split('/')[-1]        
        ltype.package = msg.DESCRIPTOR.file.package        
        ltype.cls = msg.DESCRIPTOR.name
        
        
    def set_linked_object(self,field, value):        
        # If it is a link - set a link to the value in the wrapper
        if field._gpb_full_name == self.LinkClassName:
            
            #Make sure the link is in the nodes set of children 
            field._child_links.add(field)
            
            # If the link is currently set
            if field.IsInitialized():
                
                if field.key == value._myid:
                    return
                
                
                old_obj = self._workspace.get(field.key,None)
                if old_obj:
                    plinks = old_obj._parent_links
                    plinks.remove(field.key)
                    # If there are no parents left for the object delete it
                    if len(plinks)==0:
                        del self._workspace[field.key]
                    
                
                # Modify the existing link
                setattr(field,'key',value._myid)
                
                # Set the new type
                tp = getattr(field,'type')
                self._set_type_from_obj(tp, field)
                    
            else:
                
                # Set the id of the linked wrapper
                setattr(field,'key',value._myid)
                
                # Set the type
                tp = getattr(field,'type')
                self._set_type_from_obj(tp, field)
                
        else:
            
            raise Exception, 'Can not set a composit field'
            #Over ride Protobufs - I want to be able to set a message directly
        #    field.CopyFrom(value)
        
            