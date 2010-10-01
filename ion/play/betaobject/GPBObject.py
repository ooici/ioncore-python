#!/usr/bin/env python
"""
@Brief Test implementation of a wrapper for Google Protocol Buffer Message Classes.
"""

from google.protobuf import message
from google.protobuf.internal import containers
    
class Wrapper(object):
    '''
    A Wrapper class for intercepting access to protocol buffers message fields.
    For instance, in the example below I can create a wrapper which is
    read-only*.
    
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
    
    '''
    
    
    
    def __init__(self, gpbMessage, read_only=False):
        
        # Set list of fields empty for now... so that we can use getter/setters
        object.__setattr__(self,'_gpbFields',[])
        object.__setattr__(self,'read_only', read_only)
        
        # Set the 
        assert isinstance(gpbMessage, message.Message)
        
        self._gpbMessage = gpbMessage
        self._GPBClass = gpbMessage.__class__
        field_names = self._GPBClass.DESCRIPTOR.fields_by_name.keys()
        
        # Now set the fields to preempt!
        object.__setattr__(self,'_gpbFields',field_names)

    @classmethod
    def wrap(cls,gpbMessage,read_only=False):
        inst = cls(gpbMessage,read_only)
        return inst
    
    
    def rewrap(self, gpbMessage):
        '''
        Factory method to return a new instance of wrapper for a gpbMessage
        from self - used for access to composite structures, it has all the same
        shared variables as the parent wrapper
        '''
        cls = self.__class__
        
        inst = cls(gpbMessage,self.read_only)
        return inst


    def __getattribute__(self, key):
        
        # Because we have over-riden the default getattribute we must be extremely
        # careful about how we use it!
        gpbfields = object.__getattribute__(self,'_gpbFields')
        
        if key in gpbfields:
            gpb = object.__getattribute__(self,'_gpbMessage')
            value = getattr(gpb,key)

            print 'Value', value, type(value), hasattr(value,'__iter__')
            
            if isinstance(value, containers.RepeatedCompositeFieldContainer):
                value = ContainerWrapper(value)
            elif isinstance(value, message.Message):
                value = self.rewrap(value)
                
        else:
            value = object.__getattribute__(self, key)
        return value        

    def __setattr__(self,key,value):

        gpbfields = object.__getattribute__(self,'_gpbFields')
        read_only = object.__getattribute__(self,'read_only')
        if key in gpbfields:
            if read_only:
                raise AttributeError, 'This object wrapper is read only!'
            
            gpb = object.__getattribute__(self,'_gpbMessage')
            setattr(gpb, key, value)
        else:
            v = object.__setattr__(self, key, value)
    
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
        assert isinstance(other, Wrapper), \
            'MergeFrom can only be performed on another Wrapper Object'
        
        # There does not seem to be any way to check if these are mergable?
        # What happens if they are not the same kind of GPB Message?
        self._gpbMessage.MergeFrom(other._gbpMessage)
    
    def CopyFrom(self, other_msg):
        """Copies the content of the specified message into the current message.
        
        The method clears the current message and then merges the specified
        message using MergeFrom.
        
        Args:
            other_msg: Message to copy into the current one.
        """
        if self is other_msg:
            return
        self.Clear()
        self.MergeFrom(other_msg)
    
    def Clear(self):
        """Clears all data that was set in the message."""
        self._gpbMessage.Clear()
    
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
    
    def __init__(self,gpbcontainer):
        self._gpbcontainer = gpbcontainer
    
    def __getitem__(self, key):
        """Retrieves item by the specified key."""
        return Wrapper.wrap(self._gpbcontainer[key])
    
    def __len__(self):
        """Returns the number of elements in the container."""
        return len(self._gpbcontainer)
        
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
        return repr(self._gpbcontainer)
        
        
    # Composite specific methods:
    def add(self):
        new_element = self._gpbcontainer.add()
        return Wrapper.wrap(new_element)
        
    
# This is a mess - sets class properties!    
#class GPBWrapperMeta(type):
#    
#    def __new__(cls, name,bases,dictionary):
#        
#        print 'NEW!'
#        
#        print dir(cls)
#        
#        superclass = super(GPBWrapperMeta, cls)
#        return superclass.__new__(cls, name, bases, dictionary)
#    
#    def __init__(cls,name,bases,dictionary):
#        
#        print 'INIT!'
#        
#        GPBClass = dictionary['GPBMessageClass']   
#        #_AddMessageProperties(cls, GPBClass)
#        cls.test = 5
#        
#        print dir(cls)
#
#                            
#        superclass = super(GPBWrapperMeta, cls)
#        superclass.__init__(name, bases, dictionary)
#    
#    
#class GPBWrapper(object):
#    __metaclass__ = GPBWrapperMeta
#    GPBMessageClass = test_pb2.Link
#    
#    def __init__(self):
#        print "HELLO WORLD"
#        self._gpbmessage = self.GPBMessageClass()
#        print 'Hello Dave'
    

# I am pretty sure this 'simpler' approach will not work...
#class Simpler(object):
#    
#    def __init__(self, GPBClass):
#        print "HELLO WORLD"
#        self._gpbmessage = GPBClass()
#        print 'Hello Dave'
#    
#    
#    def __set__(self,name,value):
#        setattr(self._gpbmessage, name, value)
#            
#    #def __getattr__(self,name):
#    #    if name in self._gpbmessage:
#    #        return getattr(self._gpbmessage, name)
#    #    else:
#    #        object.__getattr__(self, name)
#    #


# Old - use for reference only
#class GPBObject(object):
#    #implements(IObject)
#    
#    
#    def __new__(cls, GPBClass):
#        
#        #_AddMessageProperties(cls, GPBClass)
#
#        return object.__new__(cls)
#        
#    
#    def __init__(self, GPBClass):
#        
#        self.obj_cntr=0
#        """
#        A counter object used by this class to identify content objects untill
#        they are indexed
#        """
#        
#        self._workspace = {}
#        """
#        A dictionary containing objects which are not yet indexed, linked by a
#        counter refrence in the current workspace
#        """
#        
#        self._index = {}
#        """
#        A dictionary containing the objects which are already indexed by content
#        hash
#        """
#        
#        self._gpbmessage = GPBClass()
#        
#        
#        
#        
#        #obj_id = self.get_id()        
#        #self._workspace[obj_id] = GPBContainer(GPBClass())
#        #self.stash('Root',obj_id)                
#        #self.stash_apply('Root')
#        
#def _AddMessageProperties(cls, GPBClass):
#    
#    assert issubclass(GPBClass, message.Message)
#    
#    for field in GPBClass.DESCRIPTOR.fields_by_name.values():
#        print 'HEHREHEHEHEH'
#
#        _AddFieldProperty(cls, field)
#    
#    
#def _AddFieldProperty(cls,field):
#    field_name = field.name
#    def getter(self):
#        print 'Field Name:', field_name
#        field_value = getattr(self._gpbmessage, field_name)
#        return field_value
#    getter.__module__ = None
#    getter.__doc__ = 'Getter for %s.' % field_name
#    
#    def setter(self, new_value):
#        print 'Field Name:', field_name
#        setattr(self._gpbmessage, field_name, new_value)
#    setter.__module__ = None
#    setter.__doc__ = 'Setter for %s.' % field_name
#    
#    doc = 'Magic attribute generated for wrapper of "%s" proto field.' % field_name
#    setattr(cls, field_name, property(getter, setter, doc=doc))
#        
    #def get_id(self):
    #    self.obj_cntr += 1
    #    return str(self.obj_cntr)
    #    
    #def stash(self, name,obj_id):
    #    # What to do if the name already exists?
    #    self._stash[name] = obj_id
    #    
    #def stash_apply(self, stash_name=''):
    #    obj_id = self._stash.get(stash_name,None)
    #    
    #    self.container = self._workspace.get(obj_id, self._index.get(obj_id))
    #    
    #    self.content = self.container.gpbinstance
    #    
    #    
    #    
    #    
    #def link(self, gpblink, ):
    #    """
    #    Create a link object to the current content.
    #    """
    #    
    #def set_link(self, link, node):
    #    """
    #    Set the link and add the node to the workspace
    #    """
    #    
    #def get_link(self, link):
    #    """
    #    Get the linked node
    #    """
        


#class RevealAccess(object):
#    """A data descriptor that sets and returns values
#       normally and prints a message logging their access.
#    """
#
#    def __init__(self, initval=None, name='var'):
#        self.val = initval
#        self.name = name
#
#    def __get__(self, obj, objtype):
#        print 'Retrieving', self.name
#        print 'self',self
#        print 'obj:',obj
#        print 'objtype:',objtype
#        return self.val
#
#    def __set__(self, obj, val):
#        print 'Updating' , self.name
#        print 'obj:',obj
#
#        self.val = val
#
#
#class MyClass(object):
#    x=RevealAccess(10,'var"X"')
#    y=5
    
    

class SP(object):
    '''
    Simple Properties Class - not descriptors for x only work on the class!
    '''
    def getx(self): return self.__x
    def setx(self, value): self.__x = value
    def delx(self): del self.__x
    x = property(getx, setx, delx, "I'm the 'x' property.")
    
    def __init__(self):
        self.__x = None
    
    
class PW(object):
    
    def __init__(self, spc):
        
        object.__setattr__(self,'alist',[])        
        assert issubclass(spc, SP)
        self.sp = spc()
        self.spc = spc
        self.alist = ['x']
    

    def __getattribute__(self, key):
        
        alist = object.__getattribute__(self,'alist')
        
        if key in alist:
            sp = object.__getattribute__(self,'sp')
            v = getattr(sp,key)
        else:
            v = object.__getattribute__(self, key)
        return v        
    

    def __setattr__(self,key,value):
        alist = object.__getattribute__(self,'alist')
        
        if key in alist:
            sp = object.__getattribute__(self,'sp')
            setattr(sp, key, value)
        else:
            v = object.__setattr__(self, key, value)
        

        
