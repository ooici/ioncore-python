#!/usr/bin/env python
"""
@Brief Test implementation of a wrapper for Google Protocol Buffer Message Classes.
"""

from google.protobuf import message
from google.protobuf.internal import containers
    
from net.ooici.core.link import link_pb2    
    
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
    Fix read only or get rid of it? do we need it?
    
    cs.DESCRIPTOR.file.name
    Out[15]: 'net/ooici/core/link/link.proto'
    
    In [16]: cs.DESCRIPTOR.file.package
    Out[16]: 'net.ooici.core.link'
    
    In [17]: cs.DESCRIPTOR.name
    Out[17]: 'CASRef'
    
    In [18]: cs.DESCRIPTOR.full_name
    Out[18]: 'net.ooici.core.link.CASRef'

    
    '''
    
    LinkClassName = 'net.ooici.core.link.CASRef'
    
    def __init__(self, repository, gpbMessage, myid, read_only=False):
        """
        Initialize the Wrapper class and set up it message type.
        
        """
        # Set list of fields empty for now... so that we can use getter/setters
        object.__setattr__(self,'_gpbFields',[])
        object.__setattr__(self,'read_only', read_only)
        
        # Set the deligated message and it machinary
        assert isinstance(gpbMessage, message.Message)
        self._gpbMessage = gpbMessage
        self._GPBClass = gpbMessage.__class__
        field_names = self._GPBClass.DESCRIPTOR.fields_by_name.keys()
        self._gpb_full_name = gpbMessage.DESCRIPTOR.full_name
        
        self._parent_links=[]
        """
        A list of all the wrappers which link to me
        """
        
        self._child_links=[]
        """
        A list of all the wrappers which I link to
        """
        
        self._myid = myid
        """
        The name for this object - the SHA1 if it is already hashed or the object
        counter value if it is still in the workspace.
        """
        
        self.root = self
        """
        A reference to the root object wrapper for this protobuffer
        A composit protobuffer object may return 
        """
        
        self._repository = repository
        """
        Need to cary a reference to the repository I am in.
        """
        
        # Now set the fields from that GPB to preempt getter/setter!
        object.__setattr__(self,'_gpbFields',field_names)

    def isroot(self):
        return self is self.root
    
    """
    @classmethod
    def wrap(cls,gpbMessage,read_only=False):
        inst = cls(gpbMessage,read_only)
        return inst
    """
    
    def rewrap(self, gpbMessage):
        '''
        Factory method to return a new instance of wrapper for a gpbMessage
        from self - used for access to composite structures, it has all the same
        shared variables as the parent wrapper
        '''
        cls = self.__class__
        # note - cant use @classmethod because I need context from this message
        
        inst = cls(gpbMessage,self.read_only)
        inst._root = self._root
        return inst


    def __getattribute__(self, key):
        
        # Because we have over-riden the default getattribute we must be extremely
        # careful about how we use it!
        gpbfields = object.__getattribute__(self,'_gpbFields')
        
        
        if key in gpbfields:
            print '__getattribute__: self, key:', object.__getattribute__(self,'_gpb_full_name'), key
            gpb = object.__getattribute__(self,'_gpbMessage')
            value = getattr(gpb,key)

            #print 'Value', value, type(value), hasattr(value,'__iter__')
                        
            if isinstance(value, containers.RepeatedCompositeFieldContainer):
                value = ContainerWrapper(self, value)
            elif isinstance(value, message.Message):
                if value.DESCRIPTOR.full_name == self.LinkClassName:
                    value = self.get_name(value.id)
                    
                else:
                    value = self.rewrap(value)
                
        else:
            value = object.__getattribute__(self, key)
        return value        

    def __setattr__(self,key,value):

        gpbfields = object.__getattribute__(self,'_gpbFields')
        read_only = object.__getattribute__(self,'read_only')
        
        if key in gpbfields:
            print '__setattr__: self, key, value:', self._gpb_full_name, key, value

            if read_only:
                raise AttributeError, 'This object wrapper is read only!'
            
            # Setter helper method
            self._set_gpb_field(key, value)
            
        else:
            v = object.__setattr__(self, key, value)
    
 
    def _set_gpb_field(self, key, value):
        
        gpb = object.__getattribute__(self,'_gpbMessage')
        # If the value we are setting is a Wrapper Object
        if isinstance(value, Wrapper):
            
            #Examin the field we are trying to set 
            field = getattr(gpb,key)
            wrapped_field = self.rewrap(field)
            self._set_message(wrapped_field,value)
            
        else:
            setattr(gpb, key, value)
    
    def _set_message(self,field, value):        
        # If it is a link - set a link to the value in the wrapper
        if field._gpb_full_name == self.LinkClassName:
            
            current = self._workspace.get(field.id,None)
            if current:
                # Modify the existing value already lined in the worksapce
                current.CopyFrom(value)
                # Reset the type name
                setattr(field,'type',value._gpb_full_name)
                    
            else:
                
                # add a reference to links in the value in the wrapper
                value._links.append(self)
                    
                # Get a new local identity for this new link
                idx = self.get_id()
                    
                # add the value in the wrapper to the local workspace
                self._workspace[idx] = value
                
                # Set the type and id of the linked wrapper
                setattr(field,'id',idx)
                # Set the type name
                setattr(field,'type',value._gpb_full_name)
                
        else:
            #Over ride Protobufs - I want to be able to set a message directly
            field.CopyFrom(value)

    
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
        
        
    def MergeFrom(self, other):
        """Merges the contents of the specified message into current message.
        
        This method merges the contents of the specified message into the current
        message. Singular fields that are set in the specified message overwrite
        the corresponding fields in the current message. Repeated fields are
        appended. Singular sub-messages and groups are recursively merged.
        
        Args:
            other_msg: Message to merge into the current message.
        """
        if self is other:
            return
        
        assert isinstance(other, Wrapper), \
            'MergeFrom can only be performed on another Wrapper Object'
        
        assert self._gpb_full_name == other._gpb_full_name, \
            'MergeFrom can only operate on two wrapped objects of the same type'
        
        self._gpbMessage.MergeFrom(other._gpbMessage)
    
    def CopyFrom(self, other):
        """Copies the content of the specified message into the current message.
        
        The method clears the current message and then merges the specified
        message using MergeFrom.
        
        Args:
            other_msg: Message to copy into the current one.
        """
        if self is other:
            return
        assert isinstance(other, Wrapper), \
            'CopyFrom can only be performed on another Wrapper Object'
        
        assert self._gpb_full_name == other._gpb_full_name, \
            'CopyFrom can only operate on two wrapped objects of the same type'
        
        
        self.Clear()
        self.MergeFrom(other_msg)
        
    
    
    def Clear(self):
        """Clears all data that was set in the message."""
            
        if self.isroot():
            self._workspace={}
        
        self._gpbMessage.Clear()
    
        for key, linked_node in self._workspace.items():
            stillmine = False
            for link in linked_node._links:
                if link.id == '-1':
                    # if it is no longer 'set' remove it
                    linked_node.links.remove(link)
                elif link._root == self._root:
                    stillmine = True
                    
            if not stillmine:
                del self._workspace[key]
                
                
                    
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
        
    def ListFields(self):
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
        item = self.__getitem__(key)
        if item._gpb_full_name == self._wrapper.LinkClassName:
            
            self._wrapper._set_message(item, value)
                
        else:
            item.CopyFrom(value)
        
            
    def __getitem__(self, key):
        """Retrieves item by the specified key."""
        return self._wrapper.rewrap(self._gpbcontainer[key])
    
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
    

        
