#!/usr/bin/env python
"""
@file ion/core/object/gpb_wrapper.py
@Brief Wrapper for Google Protocol Buffer Message Classes.
These classes are the lowest level of the object management stack
@author David Stuebe
TODO:
Test Repeated Container for non composit fields
Test what happens if you try to use a old reference to a wrapper after checkout
"""

from ion.util import procutils as pu

from ion.core.object.object_utils import set_type_from_obj, sha1bin, sha1hex

from google.protobuf import message
from google.protobuf.internal import containers
    
from net.ooici.core.container import container_pb2
from net.ooici.core.link import link_pb2
from net.ooici.core.type import type_pb2

import hashlib

class OOIObjectError(Exception):
    """
    An exception class for errors that occur in the Object Wrapper class
    """
    
class Wrapper(object):
    '''
    A Wrapper class for intercepting access to protocol buffers message fields.
    For instance, in the example below I can create a wrapper which is
    read-only.
    
    To make the wrapper general - apply to more than one kind of protobuffer -
    we can not use descriptors (properties) to transparently intercept a get or
    set request because they are class attributes - shared between all instances
    of the wrapper class. If we add properties each time we create a wrapper
    instance for a new kind of protobuf, new properties will be added to all
    wrapper instances.
    
    The solution I can up with is clunky! Override the __getattribute__ and
    _setattr__ method to preemptively check a list of fields to get from the
    protocol buffer message. If the key is in the list get/set the deligated
    protocol buffer rather than the wrapper class. The problem is that now we
    can not use the default get/set to initialize our own class or get the list
    of fields!
    
    Organization:
    The meat of the class is at the top - Init and class methods are at the top
    along with overrides for __getattribute__ and __setattr__ which are the heart
    of the wrapper.
    
    Below that are all of the methods of protobuffers exposed by the wrapper.
    
    TODO:
    What about name conflicts between wrapper methods and GPB Fields?
    
    
    '''
        
    LinkClassType = set_type_from_obj(link_pb2.CASRef)
        
    def __init__(self, gpbMessage):
        """
        Initialize the Wrapper class and set up it message type.
        
        """
        # Set list of fields empty for now... so that we can use getter/setters
        object.__setattr__(self,'_gpbFields',[])
        
        
        # Set the deligated message and its machinary
        if not isinstance(gpbMessage, message.Message):
            raise OOIObjectError('Wrapper init argument must be an instance of a GPB message')
        
        self._gpbMessage = gpbMessage
        self._GPBClass = gpbMessage.__class__
        # Get the GPB Field Names
        field_names = self._GPBClass.DESCRIPTOR.fields_by_name.keys()
        # Get the GPB Enum Names
        for enum_type in self._GPBClass.DESCRIPTOR.enum_types:
            for enum_value in enum_type.values:
                field_names.append(enum_value.name)
        
        self._gpb_type = set_type_from_obj(gpbMessage)
        
        
        self._parent_links=None
        """
        A list of all the wrappers which link to me
        """
        
        self._child_links=None
        """
        A list of all the wrappers which I link to
        """
        
        self._myid = None # only exists in the root object
        """
        The name for this object - the SHA1 if it is already hashed or the object
        counter value if it is still in the workspace.
        """
        
        self._root = None
        """
        A reference to the root object wrapper for this protobuffer
        A composit protobuffer object may return 
        """
        
        self._modified =  None # only exists in the root object
        """
        Is this wrapper object modified or commited
        """
        
        self._read_only = None # only exists in the root object
        """
        Set this to be a read only wrapper!
        Used for commit objects and a history checkout... it is only set in the root object
        """
        
        self._repository = None # only exists in the root object
        """
        Need to cary a reference to the repository I am in.
        """
        
        # Now set the fields from that GPB to preempt getter/setter!
        self._gpbFields = field_names
        
    @property
    def Root(self):
        """
        Access to the root object of the nested GPB object structure
        """
        return self._root
    
    @property
    def IsRoot(self):
        """
        Is this wrapped object the root of a GPB Message?
        GPBs are also tree structures and each element must be wrapped
        """
        return self is self._root
    
    @property
    def GPBType(self):
        """
        Could just replace the attribute with the capital name?
        """
        return self._gpb_type
    
    @property
    def GPBMessage(self):
        """
        Could just replace the attribute with the capital name?
        """
        return self._gpbMessage
        
    @property
    def Repository(self):
        return self._root._repository
    
    def _get_myid(self):
        return self._root._myid
    
    def _set_myid(self,value):
        assert isinstance(value, str), 'myid is a string property'
        self._root._myid = value

    MyId = property(_get_myid, _set_myid)
    
    
    def _get_parent_links(self):
        """
        A list of all the wrappers which link to me
        """
        return self._root._parent_links
        
    def _set_parent_links(self,value):
        """
        A list of all the wrappers which link to me
        """
        self._root._parent_links = value

    ParentLinks = property(_get_parent_links, _set_parent_links)
        
    def _get_child_links(self):
        """
        A list of all the wrappers which I link to
        """
        return self._root._child_links
        
    def _set_child_links(self, value):
        """
        A list of all the wrappers which I link to
        """
        self._root._child_links = value
        
    ChildLinks = property(_get_child_links, _set_child_links)
        
    def _get_readonly(self):
        return self._root._read_only
        
    def _set_readonly(self,value):
        assert isinstance(value, bool), 'readonly is a boolen property'
        self._root._read_only = value

    ReadOnly = property(_get_readonly, _set_readonly)
    
    def _get_modified(self):
        return self._root._modified

    def _set_modified(self,value):
        assert isinstance(value, bool), 'modified is a boolen property'
        self._root._modified = value

    Modified = property(_get_modified, _set_modified)
    
    
    def SetLink(self,value):
        if not self.GPBType == self.LinkClassType:
            raise OOIObjectError('Can not set link for non link type!')
        self.Repository.set_linked_object(self,value)
        if not self.Modified:
            self._set_parents_modified()
        
    def SetLinkByName(self,linkname,value):
        link = self.GetLink(linkname)
        link.SetLink(value)
        
    def GetLink(self,linkname):
            
        gpb = self.GPBMessage
        link = getattr(gpb,linkname)
        link = self._rewrap(link)
        if not link.GPBType == self.LinkClassType:
            raise OOIObjectError('The field "%s" is not a link!' % linkname)
        return link
         
    def InParents(self,value):
        '''
        Check recursively to make sure the object is not already its own parent!
        '''
        for item in self.ParentLinks:
            if item.Root is value:
                return True
            if item.InParents(value):
                return True
        return False
    
    def SetStructureReadOnly(self):
        """
        Set these objects to be read only
        """
        self.read_only = True
        for link in self.ChildLinks:
            child = self.Repository.get_linked_object(link)
            child.SetStructureReadOnly()
        
        
    def SetStructureReadWrite(self):
        """
        Set these object to be read write!
        """
        self.read_only = False
        for link in self.ChildLinks:
            child = self.Repository.get_linked_object(link)
            child.SetStructureReadWrite()

    def RecurseCommit(self,structure):
        
        if not self.Modified:
            # This object is already committed!
            return
        
        # Create the Structure Element in which the binary blob will be stored
        se = StructureElement()        
        
        for link in self.ChildLinks:
                        
            # Test to see if it is already serialized!
            if self.Repository._hashed_elements.has_key(link.key):
                child_se = self.Repository._hashed_elements.get(link.key)

                # Set the links is leaf property
                link.isleaf = child_se.isleaf
                
            else:
                child = self.Repository.get_linked_object(link)
                
                # Determine whether this is a leaf node
                if len(child.ChildLinks)==0:
                    #Only change to true - do not set False!
                    link.isleaf = True
                else:
                    link.isleaf = False
            
                child.RecurseCommit(structure)
            
            # Save the link info as a convience for sending!
            se.ChildLinks.add(link.key)
                    
        se.value = self.SerializeToString()
        se.key = sha1hex(se.value)

        # Structure element wrapper provides for setting type!
        se.type = self
        
        # Determine whether I am a leaf
        if len(self.ChildLinks)==0:
            se.isleaf=True
        else:
            se.isleaf = False
            
        
        if self.Repository._workspace.has_key(self.MyId):
            del self.Repository._workspace[self.MyId]
            self.Repository._workspace[se.key] = self
        
        self.MyId = se.key
        
        self.Modified = False
       
        # Set the key value for parent links!
        for link in self.ParentLinks:
            link.key = self.MyId
            
        structure[se.key] = se
        
        

    def FindChildLinks(self):
        """
        Find all of the links in this composit structure
        All of the objects worked on in this method are raw proto buffers messages!
        """
        gpb = self.GPBMessage
        # For each field in the protobuffer message
        for field in gpb.DESCRIPTOR.fields:
            # if the field is a composite - another message
            if field.message_type:
                
                # Get the field of type message
                gpb_field = getattr(gpb,field.name)
                                
                
                # If it is a repeated container type
                if isinstance(gpb_field, containers.RepeatedCompositeFieldContainer):
                    #container = ContainerWrapper(self, gpb_field)                    
                    
                    for item in gpb_field:
                        
                        wrapped_item = self._rewrap(item)
                        if wrapped_item.GPBType == wrapped_item.LinkClassType:
                            self.ChildLinks.add(wrapped_item)
                        else:
                            wrapped_item.FindChildLinks()
                                
                # IF it is a standard message field
                else:
                    if not gpb_field.IsInitialized():
                        # if it is an optional field which is not initialized
                        # it can not hold any links!
                        continue
                    
                    item = self._rewrap(gpb_field)
                    if item.GPBType == item.LinkClassType:
                        self.ChildLinks.add(item)
                    else:
                        item.FindChildLinks()
    
    def _rewrap(self, gpbMessage):
        '''
        Factory method to return a new instance of wrapper for a gpbMessage
        from self - used for access to composite structures, it has all the same
        shared variables as the parent wrapper
        '''
        cls = self.__class__
        # note - cant use @classmethod because I need context from this message
        
        inst = cls(gpbMessage)
        # Over ride the root - rewrap is for instances that derive from the root of a composite gpb
        if hasattr(self,'_root'):
            inst._root = self._root
        return inst

    def __getattribute__(self, key):
        
        # Because we have over-riden the default getattribute we must be extremely
        # careful about how we use it!
        gpbfields = object.__getattribute__(self,'_gpbFields')
        
        if key in gpbfields:
            # If it is a Field defined by the gpb...
            gpb = self.GPBMessage
            value = getattr(gpb,key)                        
            if isinstance(value, containers.RepeatedCompositeFieldContainer):
                # if it is a container field:
                value = ContainerWrapper(self, value)
            elif isinstance(value, message.Message):
                # if it is a message field:
                value = self._rewrap(value)
                if value.GPBType == self.LinkClassType:
                    value = self.Repository.get_linked_object(value)
                
        else:
            # If it is a attribute of this class, use the base class's getattr
            value = object.__getattribute__(self, key)
        return value

    def __setattr__(self,key,value):

        gpbfields = object.__getattribute__(self,'_gpbFields')
        
        if key in gpbfields:
            # If it is a Field defined by the gpb...
            if self.ReadOnly:
                raise OOIObjectError('This object wrapper is read only!')
                        
            gpb = self.GPBMessage

            # If the value we are setting is a Wrapper Object
            if isinstance(value, Wrapper):
            
                if not value.Repository is self.Repository:
                    raise OOIObjectError('These two objects are not in the same repository. \n \
                                         Copying complex objects from one repository to another is not yet supported!')
            
                #Examine the field we are trying to set 
                field = getattr(gpb,key)
                if not isinstance(field, message.Message):
                    raise OOIObjectError('Only a composit field can be set using another message as a value')
                    
                wrapped_field = self._rewrap(field) # This will throw an exception if field is not a gpbMessage
                self.Repository.set_linked_object(wrapped_field,value)
            
            else:
                setattr(gpb, key, value)
                
            # Set this object and it parents to be modified
            self._set_parents_modified()
                
        else:
            v = object.__setattr__(self, key, value)
            
    def _set_parents_modified(self):
        """
        This method recursively changes an objects parents to a modified state
        All links are reset as they are no longer hashed values
        """
        if self.Modified:
            # Be clear about what we are doing here!
            return
        else:
            
            self.Modified = True
                        
            new_id = self.Repository.new_id()
            self.Repository._workspace[new_id] = self.Root
            
            if self.Repository._workspace.has_key(self.MyId):
                del self.Repository._workspace[self.MyId]
            self.MyId = new_id
              
            # When you hit the commit ref - stop!                   
            if self.Root is self.Repository._workspace_root:
                # The commit is on longer really your parent!
                self.ParentLinks=set()
                
            else:
                    
                for link in self.ParentLinks:
                    # Tricky - set the message directly and call modified!                    
                    link.GPBMessage.key = self.MyId
                    link._set_parents_modified()
            
    def __eq__(self, other):
        if not isinstance(other, Wrapper):
            return False
        
        if self is other:
            return True
        
        return self._gpbMessage == other._gpbMessage
    
    def __ne__(self, other):
        # Can't just say self != other_msg, since that would infinitely recurse. :)
        return not self == other
    
    def __str__(self):
        return self._gpbMessage.__str__()
        

    def IsInitialized(self):
        """Checks if the message is initialized.
        
        Returns:
            The method returns True if the message is initialized (i.e. all of its
        required fields are set).
        """
        return self.gpbMessage.IsInitialized()
        
    def SerializeToString(self):
        """Serializes the protocol message to a binary string.
        
        Returns:
          A binary string representation of the message if all of the required
        fields in the message are set (i.e. the message is initialized).
        
        Raises:
          message.EncodeError if the message isn't initialized.
        """
        return self._gpbMessage.SerializeToString()
    
    def ParseFromString(self, serialized):
        """Clear the message and read from serialized."""
        self._gpbMessage.ParseFromString(serialized)
        
    def ListInitializedFields(self):
        """Returns a list of (FieldDescriptor, value) tuples for all
        fields in the message which are not empty.  A singular field is non-empty
        if HasField() would return true, and a repeated field is non-empty if
        it contains at least one element.  The fields are ordered by field
        number"""
        return self._gpbMessage.ListFields()

    def HasField(self, field_name):
        return self._gpbMessage.HasField(field_name)
    
    def ClearField(self, field_name):
        return self._gpbMessage.ClearField(field_name)
        
    #def HasExtension(self, extension_handle):
    #    return self.GPBMessage.HasExtension(extension_handle)
    
    #def ClearExtension(self, extension_handle):
    #    return self.GPBMessage.ClearExtension(extension_handle)
    
    def ByteSize(self):
        """Returns the serialized size of this message.
        Recursively calls ByteSize() on all contained messages.
        """
        return self.GPBMessage.ByteSize()
    
    
    
class ContainerWrapper(object):
    """
    This class is only for use with containers.RepeatedCompositeFieldContainer
    It is not needed for repeated scalars!
    """
    
    LinkClassType = set_type_from_obj(link_pb2.CASRef())
    
    def __init__(self, wrapper, gpbcontainer):
        # Be careful - this is a hard link
        self._wrapper = wrapper
        if not isinstance(gpbcontainer, containers.RepeatedCompositeFieldContainer):
            raise OOIObjectError('The Container Wrapper is only for use with Repeated Composit Field Containers')
        self._gpbcontainer = gpbcontainer
        self.Repository = wrapper.Repository # Hack - make uniform interface to repository
        
    
    def __setitem__(self, key, value):
        """Sets the item on the specified position.
        Depricated"""
        if not isinstance(value, Wrapper):
            raise OOIObjectError('To set an item in a repeated field container, the value must be a Wrapper')
        
        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
        if item.GPBType == self.LinkClassType:
            self.Repository.set_linked_object(item, value)
        else:
            raise OOIObjectError('It is illegal to set a value of a repeated composit field unless it is a CASRef - Link')
         
        self._wrapper._set_parents_modified()
         
        # Don't want to expose SetItem for composits!        
        #else:
        #    item.CopyFrom(value)
        
    def SetLink(self,key,value):
        if not isinstance(value, Wrapper):
            raise OOIObjectError('To set an item in a repeated field container, the value must be a Wrapper')
        
        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
        if item.GPBType == self.LinkClassType:
            self.Repository.set_linked_object(item, value)
        else:
            raise OOIObjectError('It is illegal to set a value of a repeated composit field unless it is a CASRef - Link')
         
        self._wrapper._set_parents_modified()
         
            
    def __getitem__(self, key):
        """Retrieves item by the specified key."""
        value = self._gpbcontainer.__getitem__(key)
        value = self._wrapper._rewrap(value)
        if value.GPBType == self.LinkClassType:
            value = self.Repository.get_linked_object(value)
        return value
    
    def GetLink(self,key):
            
        link = self._gpbcontainer.__getitem__(key)
        link = self._wrapper._rewrap(link)
        assert link.GPBType == self.LinkClassType, 'The field "%s" is not a link!' % linkname
        return link
        
    def GetLinks(self):
        wrapper_list=[]            
        links = self._gpbcontainer[:] # Get all the links!
        for link in links:
            link = self._wrapper._rewrap(link)
            assert link.GPBType == self.LinkClassType, 'The field "%s" is not a link!' % linkname
            wrapper_list.append(link)
        return wrapper_list
    
    def __len__(self):
        """Returns the number of elements in the container."""
        return self._gpbcontainer.__len__()
        
    def __ne__(self, other):
        """Checks if another instance isn't equal to this one."""
        # The concrete classes should define __eq__.
        return not self._gpbcontainer == other._gpbcontainer

    def __eq__(self, other):
        """Compares the current instance with another one."""
        if self is other:
            return True
        if not isinstance(other, self.__class__):
            raise OOIObjectError('Can only compare repeated composite fields against other repeated composite fields.')
        return self._gpbcontainer == other._gpbcontainer

    def __repr__(self):
        """Need to improve this!"""
        return self._gpbcontainer.__repr__()
        
        
    # Composite specific methods:
    def add(self):
        new_element = self._gpbcontainer.add()
        return self._wrapper._rewrap(new_element)
        
    def __getslice__(self, start, stop):
        """Retrieves the subset of items from between the specified indices."""
        wrapper_list=[]
        stop = min(stop, len(self))
        for index in range(start,stop):
            wrapper_list.append(self.__getitem__(index))
        return wrapper_list
    
    def __delitem__(self, key):
        """Deletes the item at the specified position."""
        self._gpbcontainer.__delitem__(key)
        
    def __delslice__(self, start, stop):
        """Deletes the subset of items from between the specified indices."""
        self._gpbcontainer.__delslice__(start, stop)
    
   
    
class StructureElement(object):
    """
    @Brief Wrapper for the container structure element. These are the objects
    stored in the hashed elements table. Mostly convience methods are provided
    here. A set provides references to the child objects so that the content
    need not be decoded to find them.
    """
    def __init__(self):
            
        self._element = container_pb2.StructureElement()
        self.ChildLinks = set()
        
    @classmethod
    def wrap_structure_element(cls,se):
        inst = cls()
        inst._element = se
        return inst
        
        
    #@property
    def _get_type(self):
        return self._element.type
        
    #@type.setter
    def _set_type(self,value):
        self._element.type.protofile = value.GPBType.protofile
        self._element.type.package = value.GPBType.package
        self._element.type.cls = value.GPBType.cls        
     
    type = property(_get_type, _set_type)
     
    #@property
    def _get_value(self):
        return self._element.value
        
    #@value.setter
    def _set_value(self,value):
        self._element.value = value

    value = property(_get_value, _set_value)
        
    #@property
    def _get_key(self):
        return self._element.key
        
    #@key.setter
    def _set_key(self,value):
        self._element.key = value

    key = property(_get_key, _set_key)
    
    def _set_isleaf(self,value):
        self._element.isleaf = value
        
    def _get_isleaf(self):
        return self._element.isleaf
    
    isleaf = property(_get_isleaf, _set_isleaf)
    
    def __str__(self):
        return self._element.__str__()
        
