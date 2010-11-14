#!/usr/bin/env python
"""
@Brief Test implementation of a wrapper for Google Protocol Buffer Message Classes.

TODO:
Test Repeated Container for non composit fields
Test what happens if you try to use a old reference to a wrapper after checkout
"""

from ion.util import procutils as pu

from google.protobuf import message
from google.protobuf.internal import containers
    
from net.ooici.core.container import container_pb2
from net.ooici.core.link import link_pb2
from net.ooici.core.type import type_pb2

import hashlib

    
def sha1hex(val):
    return hashlib.sha1(val).hexdigest().upper()

def sha1bin(val):
    return hashlib.sha1(val).digest()

def sha1(val, bin=True):
    if isinstance(val, BaseObject):
        val = val.value
    if bin:
        return sha1bin(val)
    return sha1hex(val)

def sha1_to_hex(bytes):
    """binary form (20 bytes) of sha1 digest to hex string (40 char)
    """
    hex_bytes = struct.unpack('!20B', bytes)
    almosthex = map(hex, hex_bytes)
    return ''.join([y[-2:] for y in [x.replace('x', '0') for x in almosthex]])

def set_type_from_obj(obj):
    
    gpbtype = type_pb2.GPBType()
    
    # Take just the file name
    gpbtype.protofile = obj.DESCRIPTOR.file.name.split('/')[-1]
    # Get rid of the .proto
    gpbtype.protofile = gpbtype.protofile.split('.')[0]

    gpbtype.package = obj.DESCRIPTOR.file.package
    gpbtype.cls = obj.DESCRIPTOR.name
    
    return gpbtype
    
    
    
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
        
    LinkClassType = set_type_from_obj(link_pb2.CASRef())
        
    def __init__(self, gpbMessage):
        """
        Initialize the Wrapper class and set up it message type.
        
        """
        # Set list of fields empty for now... so that we can use getter/setters
        object.__setattr__(self,'_gpbFields',[])
        
        
        # Set the deligated message and it machinary
        assert isinstance(gpbMessage, message.Message)
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
    def isroot(self):
        return self is self._root
    
    @property
    def GPBType(self):
        return self._gpb_type
    
    @property
    def GPBMessage(self):
        return self._gpbMessage
    
    
    @property
    def root(self):
        return self._root
    
    #@property
    def get_myid(self):
        return self._root._myid
    
    #@myid.setter
    def set_myid(self,value):
        assert isinstance(value, str), 'myid is a string property'
        self._root._myid = value

    myid = property(get_myid, set_myid)
    
    #@property
    def get_readonly(self):
        return self._root._read_only
        
    #@readonly.setter
    def set_readonly(self,value):
        assert isinstance(value, bool), 'readonly is a boolen property'
        self._root._read_only = value

    readonly = property(get_readonly, set_readonly)
    
    #@property
    def get_modified(self):
        return self._root._modified

    #@modified.setter
    def set_modified(self,value):
        assert isinstance(value, bool), 'modified is a boolen property'
        self._root._modified = value

    modified = property(get_modified, set_modified)
    
    
    @property
    def repository(self):
        return self._root._repository
    

    
    def _set_parents_modified(self):
        """
        This method recursively changes an objects parents to a modified state
        All links are reset as they are no longer hashed values
        """
        if self.modified:
            # Be clear about what we are doing here!
            return
        else:
            
            self.modified = True
                        
            new_id = self.repository.new_id()
            self.repository._workspace[new_id] = self.root
            
            if self.repository._workspace.has_key(self.myid):
                del self.repository._workspace[self.myid]
            self.myid = new_id
              
            # When you hit the commit ref - stop!                   
            if self.root is self.repository._workspace_root:
                # The commit is on longer really your parent!
                self.root._parent_links=set()
                
            else:
                    
                for link in self._parent_links:
                    # Tricky - set the message directly and call modified!                    
                    link._gpbMessage.key = self.myid
                    link._set_parents_modified()
    
    def _set_structure_read_only(self):
        """
        Set these objects to be read only
        """
        self.read_only = True
        for link in self._child_links:
            child = self.repository.get_linked_object(link)
            child._set_structure_read_only()
        
        
    def _set_structure_read_write(self):
        """
        Set these object to be read write!
        """
        self.read_only = False
        for link in self._child_links:
            child = self.repository.get_linked_object(link)
            child._set_structure_read_write()
        
    
    
    def rewrap(self, gpbMessage):
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
            inst._parent_links = self._root._parent_links
            inst._child_links = self._root._child_links
        
        return inst


    def __getattribute__(self, key):
        
        # Because we have over-riden the default getattribute we must be extremely
        # careful about how we use it!
        gpbfields = object.__getattribute__(self,'_gpbFields')
        
        if key in gpbfields:
            #print '__getattribute__: self, key:', object.__getattribute__(self,'_gpb_type'), key
            gpb = self._gpbMessage
            value = getattr(gpb,key)
                        
            #print 'Type, Value:',type(value), value
                        
            if isinstance(value, containers.RepeatedCompositeFieldContainer):
                value = ContainerWrapper(self, value)
            elif isinstance(value, message.Message):
                value = self.rewrap(value)
                if value.GPBType == self.LinkClassType:
                    value = self.repository.get_linked_object(value)
                
        else:
            value = object.__getattribute__(self, key)
        return value
    

    def get_link(self,linkname):
            
        gpb = self._gpbMessage
        link = getattr(gpb,linkname)
        link = self.rewrap(link)
        assert link.GPBType == self.LinkClassType, 'The field "%s" is not a link!' % linkname
        return link
        


    def __setattr__(self,key,value):

        gpbfields = object.__getattribute__(self,'_gpbFields')
        
        if key in gpbfields:
            #print '__setattr__: self, key, value:', self._gpb_full_name, key, value

            if self.readonly:
                raise AttributeError, 'This object wrapper is read only!'
                        
            gpb = self._gpbMessage

            # If the value we are setting is a Wrapper Object
            if isinstance(value, Wrapper):
            
                assert value.repository is self.repository, \
                    'Copying complex objects from one repository to another is not yet supported!'
            
                #Examine the field we are trying to set 
                field = getattr(gpb,key)
                assert isinstance(field, message.Message), \
                  'Only a composit field can be set using another message as a value '
                wrapped_field = self.rewrap(field) # This will throw an exception if field is not a gpbMessage
                self.repository.set_linked_object(wrapped_field,value)
            
            else:
                setattr(gpb, key, value)
                
            # Set this object and it parents to be modified
            self._set_parents_modified()
                
        else:
            v = object.__setattr__(self, key, value)
            
    def set_link(self,value):
        assert self.GPBType == self.LinkClassType, 'Can not set link for non link type!'
        self.repository.set_linked_object(self,value)
        if not self.modified:
            self._set_parents_modified()
        
    def set_link_by_name(self,linkname,value):
        link = self.get_link(linkname)
        link.set_link(value)
        
            
    def inparents(self,value):
        '''
        Check recursively to make sure the object is not already its own parent!
        '''
        for item in self._parent_links:
            if item.root is value:
                return True
            if item.inparents(value):
                return True
        return False

    def _recurse_commit(self,structure):
        
        if not self.modified:
            # This object is already committed!
            return
        
        # Create the Structure Element in which the binary blob will be stored
        se = StructureElement()        
        
        for link in self._child_links:
                        
            # Test to see if it is already serialized!
            if self.repository._hashed_elements.has_key(link.key):
                child_se = self.repository._hashed_elements.get(link.key)

                # Set the links is leaf property
                link.isleaf = child_se.isleaf
                
            else:
                child = self.repository.get_linked_object(link)
                
                # Determine whether this is a leaf node
                if len(child._child_links)==0:
                    #Only change to true - do not set False!
                    link.isleaf = True
                else:
                    link.isleaf = False
            
                child._recurse_commit(structure)
            
            # Save the link info as a convience for sending!
            se._child_links.add(link.key)
                    
        se.value = self.SerializeToString()
        se.key = sha1hex(se.value)

        # Structure element wrapper provides for setting type!
        se.type = self
        
        # Determine whether I am a leaf
        if len(self._child_links)==0:
            se.isleaf=True
        else:
            se.isleaf = False
            
        
        if self.repository._workspace.has_key(self.myid):
            del self.repository._workspace[self.myid]
            self.repository._workspace[se.key] = self
        
        self.myid = se.key
        
        self.modified = False
       
        # Set the key value for parent links!
        for link in self._parent_links:
            link.key = self.myid
            
        structure[se.key] = se
        
        

    def _find_child_links(self):
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
                        
                        wrapped_item = self.rewrap(item)
                        if wrapped_item.GPBType == wrapped_item.LinkClassType:
                            self._child_links.add(wrapped_item)
                        else:
                            wrapped_item._find_child_links()
                                
                # IF it is a standard message field
                else:
                    if not gpb_field.IsInitialized():
                        # if it is an optional field which is not initialized
                        # it can not hold any links!
                        continue
                    
                    item = self.rewrap(gpb_field)
                    if item.GPBType == item.LinkClassType:
                        self._child_links.add(item)
                    else:
                        item._find_child_links()
                    
                    
    
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
        return self._gpbMessage.IsInitialized()
        
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
        
    def HasExtension(self, extension_handle):
        return self._gpbMessage.HasExtension(extension_handle)
    
    def ClearExtension(self, extension_handle):
        return self._gpbMessage.ClearExtension(extension_handle)
    
    def ByteSize(self):
        """Returns the serialized size of this message.
        Recursively calls ByteSize() on all contained messages.
        """
        return self._gpbMessage.ByteSize()
    
    
    '''
    Methods not included in the Wrapper:

    def SetInParent(self):
    """Mark this as present in the parent.

    This normally happens automatically when you assign a field of a
    sub-message, but sometimes you want to make the sub-message
    present while keeping it empty.  If you find yourself using this,
    you may want to reconsider your design."""
    raise NotImplementedError

    def MergeFromString(self, serialized):
    """Merges serialized protocol buffer data into this message.

    When we find a field in |serialized| that is already present
    in this message:
      - If it's a "repeated" field, we append to the end of our list.
      - Else, if it's a scalar, we overwrite our field.
      - Else, (it's a nonrepeated composite), we recursively merge
        into the existing composite.

    TODO(robinson): Document handling of unknown fields.

    Args:
      serialized: Any object that allows us to call buffer(serialized)
        to access a string of bytes using the buffer interface.

    TODO(robinson): When we switch to a helper, this will return None.

    Returns:
      The number of bytes read from |serialized|.
      For non-group messages, this will always be len(serialized),
      but for messages which are actually groups, this will
      generally be less than len(serialized), since we must
      stop when we reach an END_GROUP tag.  Note that if
      we *do* stop because of an END_GROUP tag, the number
      of bytes returned does not include the bytes
      for the END_GROUP tag information.
    """
    raise NotImplementedError

    def ParseFromString(self, serialized):
    """Like MergeFromString(), except we clear the object first."""
    self.Clear()
    self.MergeFromString(serialized)

    '''
    
'''
Example Usage:
import GPBObject 
import addressbook_pb2

w = GPBObject.Wrapper(addressbook_pb2.AddressBook())

# Set stuff through the wrapper
w.person.add()
# Notice that all objects returned are wrapped!
w.person[0].name = 'David'

# Get through the wrapper
w.person[0].name

# Get from the deligated class!
w._gpbMessage.person[0].name


'''
class StructureElement(object):
    
    def __init__(self):
            
        self._element = container_pb2.StructureElement()
        
        self._child_links = set()
        
    @classmethod
    def wrap_structure_element(cls,se):
        inst = cls()
        inst._element = se
        return inst
        
        
    #@property
    def get_type(self):
        return self._element.type
        
    #@type.setter
    def set_type(self,value):
        self._element.type.protofile = value.GPBType.protofile
        self._element.type.package = value.GPBType.package
        self._element.type.cls = value.GPBType.cls        
     
    type = property(get_type, set_type)
     
    #@property
    def get_value(self):
        return self._element.value
        
    #@value.setter
    def set_value(self,value):
        self._element.value = value

    value = property(get_value, set_value)
        
    #@property
    def get_key(self):
        return self._element.key
        
    #@key.setter
    def set_key(self,value):
        self._element.key = value

    key = property(get_key, set_key)
    
    def set_isleaf(self,value):
        self._element.isleaf = value
        
    def get_isleaf(self):
        return self._element.isleaf
    
    isleaf = property(get_isleaf, set_isleaf)
    
    def __str__(self):
        return self._element.__str__()
    
    
        
    #def SerializeToString(self):
    #    """Serializes the protocol message to a binary string.
    #    
    #    Returns:
    #      A binary string representation of the message if all of the required
    #    fields in the message are set (i.e. the message is initialized).
    #    
    #    Raises:
    #      message.EncodeError if the message isn't initialized.
    #    """
    #    return self._element.SerializeToString()
    #
    #def ParseFromString(self, serialized):
    #    """Clear the message and read from serialized."""
    #    self._element.ParseFromString(serialized)
        
    
class ContainerWrapper(object):
    """
    This class is only for use with containers.RepeatedCompositeFieldContainer
    It is not needed for repeated scalars!
    """
    
    LinkClassType = set_type_from_obj(link_pb2.CASRef())
    
    def __init__(self, wrapper, gpbcontainer):
        # Be careful - this is a hard link
        self._wrapper = wrapper
        assert isinstance(gpbcontainer, containers.RepeatedCompositeFieldContainer), \
            'The Container Wrapper is only for use with Repeated Composit Field Containers'
        self._gpbcontainer = gpbcontainer
        self.repository = wrapper.repository # Hack - make uniform interface to repository
        
    
    def __setitem__(self, key, value):
        """Sets the item on the specified position.
        Depricated"""
        assert isinstance(value, Wrapper), \
            'To set an item, the value must be a Wrapper'
        
        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper.rewrap(item)
        if item.GPBType == self.LinkClassType:
            self.repository.set_linked_object(item, value)
        else:
            raise Exception, 'It is illegal to set a value of a repeated composit field unless it is a CASRef - Link'
         
        self._wrapper._set_parents_modified()
         
        # Don't want to expose SetItem for composits!        
        #else:
        #    item.CopyFrom(value)
        
    def set_link(self,key,value):
        assert isinstance(value, Wrapper), \
            'To set an item, the value must be a Wrapper'
        
        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper.rewrap(item)
        if item.GPBType == self.LinkClassType:
            self.repository.set_linked_object(item, value)
        else:
            raise Exception, 'It is illegal to set a value of a repeated composit field unless it is a CASRef - Link'
         
        self._wrapper._set_parents_modified()
         
            
    def __getitem__(self, key):
        """Retrieves item by the specified key."""
        value = self._gpbcontainer.__getitem__(key)
        value = self._wrapper.rewrap(value)
        if value.GPBType == self.LinkClassType:
            value = self.repository.get_linked_object(value)
        return value
    
    
    def get_link(self,key):
            
        link = self._gpbcontainer.__getitem__(key)
        link = self._wrapper.rewrap(link)
        assert link.GPBType == self.LinkClassType, 'The field "%s" is not a link!' % linkname
        return link
        
    def get_links(self):
        wrapper_list=[]            
        links = self._gpbcontainer[:] # Get all the links!
        for link in links:
            link = self._wrapper.rewrap(link)
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
            raise TypeError('Can only compare repeated composite fields against '
                          'other repeated composite fields.')
        return self._gpbcontainer == other._gpbcontainer

    def __repr__(self):
        """Need to improve this!"""
        return self._gpbcontainer.__repr__()
        
        
    # Composite specific methods:
    def add(self):
        
        new_element = self._gpbcontainer.add()
        return self._wrapper.rewrap(new_element)
        
    #def MergeFrom(self, other):
    #    """Appends the contents of another repeated field of the same type to this
    #    one, copying each individual message.
    #    """
    #    assert isinstance(other, ContainerWrapper), \
    #    'Invalid argument to merge from: must be a ContainerWrapper'
    #    self._gpbcontainer.MergeFrom(other._gpbcontainer)
    
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
    

        
