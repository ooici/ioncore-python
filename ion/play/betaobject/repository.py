#!/usr/bin/env python


"""
@Brief Repository for managing data structures
"""
from net.ooici.core.link import link_pb2
from net.ooici.core.mutable import mutable_pb2
from net.ooici.core.container import container_pb2

from ion.play.betaobject import gpb_wrapper

from ion.util import procutils as pu

class Repository(object):
    
    UPTODATE='up to date'
    MODIFIED='modified'
    NOTINITIALIZED = 'This repository is not initialized yet'
    
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
        
        self._hashed_elements = None
        """
        All content elements are stored here - from incoming messages and
        new commits - everything goes here. Now it can be decoded for a checkout
        or sent in a message.
        """
        
        self._dotgit = self.create_wrapped_object(mutable_pb2.MutableNode, storage={})
        """
        A specially wrapped Mutable GPBObject which tracks branches and commits
        It is not 'stored' anywhere - pass in a throwaway dictionary
        """
        
        self._current_branch = None
        """
        The current branch object
        Branch names are generallly nonsense (uuid or some such)
        """
        
        self._detached_head = False
        
        
        self._merged_from = []
        """
        Keep track of branches which were merged into this one!
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
        
        if self.status == self.MODIFIED:
            raise Exception, 'Can not checkout while the workspace is dirty'
        
        # Do some clean up!
        self._workspace = {}
        self._workspace_root = None
        
        # Declare the cref variable
        cref = None
        
        #Declare that it is a detached head!
        detached = False
        
        if branch:
            for item in self._dotgit.branches:
                if item.branchname == branch:
                    self.current_branch = item
                    cref = item.commitref # THIS WILL LOAD IT INTO THE WORKSPACE!
        
            if commit_id:
                detached = True
                pass
            elif older_than:
                detached = True
                pass
        
        elif commit_id:
            self._current_branch = None
            
            if self._hashed_elements.has_key(commit_id):
                cref = self._load_element(element)
                
                self.current_branch = self.create_wrapped_object(mutable_pb2.Branch, storage={})
                self.current_branch.commitref = cref
                self.current_branch.branchname = 'detached head'
                
                detached = True
            else:
                # Check more places? Ask for it from the repository?
                raise Exception, 'Can not checkout an id that does not exist!'
        
        
        # Not complete yet
        if detached == True:
            raise Exception, 'Checking out detached head is not yet implemented!' 
        self._detached_head = detached
            
            
        # Automatically fetch the object from the hashed dictionary or fetch if needed!
        rootobj = cref.objectroot
        self._workspace_root = rootobj
        
        self._load_links(rootobj)
        
        
        
    def commit(self, comment=''):
        """
        Commit the current workspace structure
        """
            
        if self.status == self.MODIFIED:
            structure={}
            self._workspace_root._recurse_commit(structure)
                
            self._hashed_elements.update(structure)
                
            # Now add a Commit Ref     
            cref = repo.create_wrapped_object(mutable_pb2.CommitRef, storage={})
            cref.date = pu.currenttime
            brnch = cref.ancestors.add()
            brnch._gpbMessage.CopyFrom(self._current_branch._gpbMessage)
            
            for mrgd in self._merged_from:
                brnch = cref.ancestors.add()
                brnch._gpbMessage.CopyFrom(mrgd._gpbMessage)
            
            cref.comment = comment
            cref.objectroot = self._workspace_root
            
            # Add the CRef to the hashed elements
            structure={}
            cref._recurse_commit(structure)
            self._hashed_elements.update(structure)
            
            # Add the cref to the active commit objects - for convienance
            self._commit_index[cref._myid] = cref
            
            # Update the head of the current branch
            self._current_branch.commitref = cref
                
                
        elif self.status == self.UPTODATE:
            pass
        else:
            raise Excpetion, 'Repository in invalid state to commit'
        
        # Like git, return the commit id 
        return self._current_branch.commitref._gpbMessage.key
            
        
    def merge(self, branch=None, commit_id = None, older_than=None):
        """
        merge the named 
        """
        
        
    @property
    def status(self):
        """
        Check the status of the current workspace - return a status
          up to date
          changed
        """
        
        if self._workspace_root:
            if self._workspace_root.ismodified:
                return self.MODIFIED
            else:
                return self.UPTODATE
        else:
            return self.NOTINITIALIZED
        
        
    def branch(self, name):
        """
        Create a new branch from the current commit and switch the workspace to the new branch.
        """
        ## Need to check and then clear the workspace???
        #if not self.status == self.UPTODATE:
        #    raise Exception, 'Can not create new branch while the workspace is dirty'
        
        for brnch in self._dotgit.branches:
            if brnch.branchname == name:                
                raise Exception, 'Branch already exists'
            
        brnch = self._dotgit.branches.add()    
        brnch.branchname = name

        if self._current_branch:
            # Get the linked commit
            cref = self._current_branch.commitref
            
            # Set the new branch to point at the commit
            brnch.commitref = cref
            
            if self._detached_head:
                self._workspace_root._set_read_write()
            
        self._current_branch = brnch
        
        
    
    def stash(self, name):
        """
        Stash the current workspace for later reference
        """
        
    def create_wrapped_object(self, rootclass, obj_id=None, storage=None):        
        
        if not obj_id:
            obj_id = self.new_id()
        obj = gpb_wrapper.Wrapper(rootclass())
        obj._repository = self
        obj._myid = obj_id
        obj._root = obj
        obj._parent_links = set()
        obj._child_objs = set()
        
        if storage:
            storage[obj_id] = obj
        else:
            self._workspace[obj_id] = obj
            
        return obj
        
    def new_id(self):
        """
        This id is a purely local concern - not used outside the local scope.
        """
        self._object_counter += 1
        return str(self._object_counter)
     
    def get_linked_object(self, link):
                
        if self._workspace.has_key(link.key):
            return self._workspace.get(link.key)

        elif self._commit_index.has_key(link.key):
            return self._commit_index.get(link.key)

        elif self._hashed_elements.has_key(link.key):
            
            element = self._hashed_elements.get(link.key)
            
            # Make sure the type field is the same!
            if not link.type == element.type:
                raise Exception, 'The link type does not match the element type!'
            
            obj = self._load_element(element)            
            self._workspace[obj._myid]=obj
            return obj
            
        else:
            return self._workbench.fetch_linked_objects(link)
            
    def _load_element(self, element):
        cls = self._load_class_from_type(element.type)
                
        # Do not automatically load it into a particular space...
        obj = self.create_wrapped_object(cls, obj_id=element.key, storage={})
            
        obj.ParseFromString(element.value)
        return obj
        
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
        if field._gpb_full_name == field.LinkClassName:
            
            #@Todo Change assertions to Exceptions?
            
            assert value.isroot == True, \
                'You can not set a link equal to part of a gpb composite!'
            
            assert not field.inparents(value), \
                'You can not create a recursive structure - this value is also a parent of the link you are setting.'
            
            
            #Make sure the link is in the nodes set of children 
            field._child_objs.add(value)
            value._parent_links.add(field)
            
            
            # If the link is currently set
            if field.key:
                
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
            
            raise Exception, 'Can not set a composite field'
            #Over ride Protobufs - I want to be able to set a message directly
        #    field.CopyFrom(value)
        
            