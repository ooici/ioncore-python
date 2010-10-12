#!/usr/bin/env python
"""
@Brief Test implementation of a wrapper for Google Protocol Buffer Message Classes.
"""

from ion.util import procutils as pu

from google.protobuf import message
from google.protobuf.internal import containers
    
from net.ooici.core.container import container_pb2
from net.ooici.core.link import link_pb2    
import hashlib

    
def sha1hex(val):
    return hashlib.sha1(val).hexdigest()

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
    
    '''
    # Change this to a type message to be more precise?
    LinkClassName = link_pb2.CASRef.DESCRIPTOR.full_name
    
    def __init__(self, gpbMessage):
        """
        Initialize the Wrapper class and set up it message type.
        
        """
        # Set list of fields empty for now... so that we can use getter/setters
        object.__setattr__(self,'_gpbFields',[])
        object.__setattr__(self,'_gpbEnums',[])
        
        
        # Set the deligated message and it machinary
        assert isinstance(gpbMessage, message.Message)
        self._gpbMessage = gpbMessage
        self._GPBClass = gpbMessage.__class__
        field_names = self._GPBClass.DESCRIPTOR.fields_by_name.keys()
        for enum_type in self._GPBClass.DESCRIPTOR.enum_types:
            for enum_value in enum_type.values:
                field_names.append(enum_value.name)
        
        self._gpb_full_name = gpbMessage.DESCRIPTOR.full_name
        
        self._parent_links=None
        """
        A list of all the wrappers which link to me
        """
        
        self._child_objs=None
        """
        A list of all the wrappers which I link to
        """
        
        self._myid = None
        """
        The name for this object - the SHA1 if it is already hashed or the object
        counter value if it is still in the workspace.
        """
        
        self._root = None
        """
        A reference to the root object wrapper for this protobuffer
        A composit protobuffer object may return 
        """
        
        self._modified =  True
        """
        Is this wrapper object modified or commited
        """
        
        self._read_only = None
        """
        Set this to be a read only wrapper!
        Used for commit objects and a history checkout...
        """
        
        self._repository = None
        """
        Need to cary a reference to the repository I am in.
        """
        
        # Now set the fields from that GPB to preempt getter/setter!
        self._gpbFields = field_names
    
    @property
    def isroot(self):
        return self is self._root
    
    def _set_read_only(self):
        """
        Set these objects to be read only
        """
        self._read_only = True
        for child in self._child_objs:
            child._set_read_only()
        
        
    def _set_read_write(self):
        """
        Set these object to be read write!
        """
        self._read_only = False
        for child in self._child_objs:
            child._set_read_write()
        
    
    
    def rewrap(self, gpbMessage):
        '''
        Factory method to return a new instance of wrapper for a gpbMessage
        from self - used for access to composite structures, it has all the same
        shared variables as the parent wrapper
        '''
        cls = self.__class__
        # note - cant use @classmethod because I need context from this message
        
        inst = cls(gpbMessage)
        # Over ride the root - rewrap is for instances that derive from the root of a composit gpb
        if self._root:
            inst._root = self._root
            inst._parent_links = self._root._parent_links
            inst._child_objs = self._root._child_objs

        inst._myid = self._myid
        inst._repository = self._repository
        
        return inst


    def __getattribute__(self, key):
        
        # Because we have over-riden the default getattribute we must be extremely
        # careful about how we use it!
        gpbfields = object.__getattribute__(self,'_gpbFields')
        
        if key in gpbfields:
            #print '__getattribute__: self, key:', object.__getattribute__(self,'_gpb_full_name'), key
            gpb = self._gpbMessage
            value = getattr(gpb,key)
                        
            if isinstance(value, containers.RepeatedCompositeFieldContainer):
                value = ContainerWrapper(self, value)
            elif isinstance(value, message.Message):
                if value.DESCRIPTOR.full_name == self.LinkClassName:
                    value = self._repository.get_linked_object(value)
                    
                else:
                    value = self.rewrap(value)
                
        else:
            value = object.__getattribute__(self, key)
        return value
    


    def __setattr__(self,key,value):

        gpbfields = object.__getattribute__(self,'_gpbFields')
        
        if key in gpbfields:
            #print '__setattr__: self, key, value:', self._gpb_full_name, key, value

            if self._root._read_only:
                raise AttributeError, 'This object wrapper is read only!'
                        
            gpb = object.__getattribute__(self,'_gpbMessage')

            # If the value we are setting is a Wrapper Object
            if isinstance(value, Wrapper):
            
                assert value._repository == self._repository, \
                    'Copying complex objects from one repository to another is not yet supported!'
            
                #Examine the field we are trying to set 
                field = getattr(gpb,key)
                assert isinstance(field, message.Message), \
                  'Only a composit field can be set using another message as a value '
                wrapped_field = self.rewrap(field) # This will throw an exception if field is not a gpbMessage
                self._repository.set_linked_object(wrapped_field,value)
            
            else:
                setattr(gpb, key, value)
                
            if not self.ismodified:
                self.modified()
                
        else:
            v = object.__setattr__(self, key, value)

    @property
    def ismodified(self):
        return self._modified
    
    def modifed(self):
        self._modified = True
                
        self._myid = self._repository.new_id()
                
        for link in self._parent_links:
            # Tricky - set the message directly and call modified!
            link._gpbMessage.key = self._myid
            link.modified()
            
    def inparents(self,value):
        '''
        Check recursively to make sure the object is not already its own parent!
        '''
        for item in self._parent_links:
            if item._root is value:
                return True
            if item.inparents(value):
                return True
        return False

    def _recurse_commit(self,structure):
        
        if not self.ismodified:
            # This object is already committed!
            return
        
        for child in self._child_objs:
            child._recurse_commit(structure)
            
            # Determine whether this is a leaf node
            if len(child._child_objs)==0:
                for parent in child._parent_links:
                    if parent._root is self:
                        parent.isleaf = True
        
        se = StructureElement()
        
        se.value = self.SerializeToString()
        se.key = sha1hex(se.value)
        # This is confusing and probably not a great idea...
        self._repository._set_type_from_obj(se.type,self)
        
        self._myid = se.key
        for parent in self._parent_links:
            parent.key = self._myid
        
        self._modified = False
        
        structure[se.key] = se
        
        


    
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
        
        
    #def MergeFrom(self, other):
    #    """Merges the contents of the specified message into current message.
    #    
    #    This method merges the contents of the specified message into the current
    #    message. Singular fields that are set in the specified message overwrite
    #    the corresponding fields in the current message. Repeated fields are
    #    appended. Singular sub-messages and groups are recursively merged.
    #    
    #    Args:
    #        other_msg: Message to merge into the current message.
    #    """
    #    if self is other:
    #        return
    #    
    #    assert isinstance(other, Wrapper), \
    #        'MergeFrom can only be performed on another Wrapper Object'
    #    
    #    assert self._gpb_full_name == other._gpb_full_name, \
    #        'MergeFrom can only operate on two wrapped objects of the same type'
    #    
    #    self._gpbMessage.MergeFrom(other._gpbMessage)
    #
    #def CopyFrom(self, other):
    #    """Copies the content of the specified message into the current message.
    #    
    #    The method clears the current message and then merges the specified
    #    message using MergeFrom.
    #    
    #    Args:
    #        other_msg: Message to copy into the current one.
    #    """
    #    if self is other:
    #        return
    #    assert isinstance(other, Wrapper), \
    #        'CopyFrom can only be performed on another Wrapper Object'
    #    
    #    assert self._gpb_full_name == other._gpb_full_name, \
    #        'CopyFrom can only operate on two wrapped objects of the same type'
    #    
    #    
    #    self.Clear()
    #    self.MergeFrom(other_msg)
    #    
    #
    #
    #def Clear(self):
    #    """Clears all data that was set in the message."""
    #        
    #    if self.isroot():
    #        self._workspace={}
    #    
    #    self._gpbMessage.Clear()
    #
    #    for key, linked_node in self._workspace.items():
    #        stillmine = False
    #        for link in linked_node._links:
    #            if link.id == '-1':
    #                # if it is no longer 'set' remove it
    #                linked_node.links.remove(link)
    #            elif link._root == self._root:
    #                stillmine = True
    #                
    #        if not stillmine:
    #            del self._workspace[key]
                
                
                    
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
        
    @property
    def type(self):
        return self._element.type
        
    #@type.setter
    #def type(self,value):
     
    @property
    def value(self):
        return self._element.value
        
    @value.setter
    def value(self,value):
        self._element.value = value
        
    @property
    def key(self):
        return self._element.key
        
    @key.setter
    def key(self,value):
        self._element.key = value
        
        
        
    
class ContainerWrapper(object):
    """
    This class is only for use with containers.RepeatedCompositeFieldContainer
    It is not needed for repeated scalars!
    """
    
    
    def __init__(self, wrapper, gpbcontainer):
        # Be careful - this is a hard link
        self._wrapper = wrapper
        assert isinstance(gpbcontainer, containers.RepeatedCompositeFieldContainer), \
            'The Container Wrapper is only for use with Repeated Composit Field Containers'
        self._gpbcontainer = gpbcontainer
    
    def __setitem__(self, key, value):
        """Sets the item on the specified position."""
        assert isinstance(value, Wrapper), \
            'To set an item, the value must be a Wrapper'
        
        item = self._gpbcontainer.__getitem__(key)
        if item and item.DESCRIPTOR.full_name == self._wrapper.LinkClassName:
            
            item = self._wrapper.rewrap(item)
            self._wrapper._repository.set_linked_object(item, value)
         
        # Don't want to expose SetItem for composits!        
        #else:
        #    item.CopyFrom(value)
        
            
    def __getitem__(self, key):
        """Retrieves item by the specified key."""
        value = self._wrapper.rewrap(self._gpbcontainer[key])
        
        if value._gpbMessage.DESCRIPTOR.full_name == self._wrapper.LinkClassName:
            value = self._wrapper._repository.get_linked_object(value)
        return value
    
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
        
    def MergeFrom(self, other):
        """Appends the contents of another repeated field of the same type to this
        one, copying each individual message.
        """
        assert isinstance(other, ContainerWrapper), \
        'Invalid argument to merge from: must be a ContainerWrapper'
        self._gpbcontainer.MergeFrom(other._gpbcontainer)
    
    def __getslice__(self, start, stop):
        """Retrieves the subset of items from between the specified indices."""
        gpbs = self._gpbcontainer.__getslice__(start, stop)
        wrapper_list=[]
        for item in gpbs:
            wrapper_list.append(self._wrapper.rewrap(item))
        return wrapper_list
    
    def __delitem__(self, key):
        """Deletes the item at the specified position."""
        self._gpbcontainer.__delitem__(key)
        
    def __delslice__(self, start, stop):
        """Deletes the subset of items from between the specified indices."""
        self._gpbcontainer.__delslice__(start, stop)
    

        
