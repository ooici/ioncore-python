#!/usr/bin/env python
"""
@file ion/core/object/gpb_wrapper.py
@brief Wrapper for Google Protocol Buffer Message Classes.
These classes are the lowest level of the object management stack
@author David Stuebe
TODO:
Finish test of new Invalid methods using weakrefs - make sure it is deleted!
"""

from ion.util import procutils as pu
from ion.util.cache import memoize
from ion.core.object.object_utils import get_type_from_obj, sha1bin, sha1hex, sha1_to_hex, ObjectUtilException, create_type_identifier

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from google.protobuf import message
from google.protobuf.internal import containers
    
from net.ooici.core.container import container_pb2
from net.ooici.core.link import link_pb2
from net.ooici.core.type import type_pb2
from ion.util.cache import memoize

import hashlib

class OOIObjectError(Exception):
    """
    An exception class for errors that occur in the Object Wrapper class
    """

class BaseWrapper(object):
    """ Abstract base class for Wrapper and its metaclass-mutated variants. """

class WrappedProperty(object):
    """ Data descriptor (like a property) for passing through GPB properties from the Wrapper. """

    def __init__(self, wrapper, name, doc=None):
        self.wrapper = wrapper
        self.name = name
        if doc: self.__doc__ = doc
        
    def __get__(self, wrapper, objtype=None):
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper._gpbMessage, self.name)

        # Or it may be something more complex that we need to operate on...
        if isinstance(field, containers.RepeatedScalarFieldContainer):
            result = ScalarContainerWrapper.factory(wrapper, field)
        elif isinstance(field, containers.RepeatedCompositeFieldContainer):
            result = ContainerWrapper.factory(wrapper, field)
        elif isinstance(field, message.Message):
            result = wrapper._rewrap(field)

            if result.ObjectType == wrapper.LinkClassType:
                result = wrapper.Repository.get_linked_object(result)
        else:
            # Probably bad that the common case comes last!
            result = field

        return result

    def __set__(self, wrapper, value):
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if wrapper.ReadOnly:
            raise OOIObjectError('This object wrapper is read only!')

        # If the value we are setting is a Wrapper Object
        if isinstance(value, Wrapper):
            if value._invalid:
                raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

            # get the callable and call it!
            wrapper.SetLinkByName(self.name, value)
        else:
            setattr(wrapper._gpbMessage, self.name, value)

        # Set this object and it parents to be modified
        wrapper._set_parents_modified()

        return None

class WrapperType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.
    """

    _type_cache = {}

    def __call__(self, gpbMessage, *args, **kwargs):
        # Cache the custom-built classes
        msgType, clsType = type(gpbMessage), None

        try:
            if msgType in WrapperType._type_cache:
                clsType = WrapperType._type_cache[msgType]
            else:
                clsName = '%s_%s' % (self.__name__, msgType.__name__)
                clsDict = {}

                # Now setup the properties to map through to the GPB object
                descriptor = msgType.DESCRIPTOR
                fieldNames = descriptor.fields_by_name.keys()

                for fieldName in fieldNames:
                    fieldType = getattr(msgType, fieldName)
                    prop = WrappedProperty(self, fieldName, doc=fieldType.__doc__)
                    clsDict[fieldName] = prop

                # Also grab the enums
                if hasattr(descriptor, 'enum_values_by_name'):
                    clsDict.update(dict((k,v.number) for k,v in descriptor.enum_values_by_name.iteritems()))
                    
                clsType = WrapperType.__new__(WrapperType, clsName, (self,), clsDict)
                WrapperType._type_cache[msgType] = clsType

            # Finally allow the instantiation to occur, but slip in our new class type
            obj = super(WrapperType, clsType).__call__(gpbMessage, *args, **kwargs)
                
        except Exception, ex:
            x = 84

        return obj

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

    __metaclass__ = WrapperType
        
    LinkClassType = create_type_identifier(object_id=3, version=1)
        
    def __init__(self, gpbMessage):
        """
        Initialize the Wrapper class and set up it message type.
        
        """
        
        object.__setattr__(self,'_gpbFields',[])
        """
        A list of fields names empty for now... so that we can use getter/setters
        on the data elements of the wrapped proto buffer
        """

        object.__setattr__(self,'_invalid',False)
        """
        Use this field to invalidate a message wrapper when it should be deleted.
        This ensures that any references which have not gone out of scope can not
        cause trouble!
        """
        
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

        try:
            self._gpb_type = get_type_from_obj(gpbMessage)
        except ObjectUtilException, re:
            self._gpb_type = None
            
            
        self._root=None
        """
        A reference to the root object wrapper for this protobuffer
        A composit protobuffer object may return 
        """
        
        self._bytes = None
        """
        Used in _load_element to create a proxy object. The bytes are not loaded
        parsed until the object is needed!
        """
        
        self._parent_links=None
        """
        A list of all the other wrapper objects which link to me
        """
        
        self._child_links=None
        """
        A list of my child link wrappers
        """
        
        self._derived_wrappers=None
        """
        A container for all the wrapper objects which are rewrapped, derived
        from a root object wrapper
        """
        
        self._myid = None # only exists in the root object
        """
        The name for this object - the SHA1 if it is already hashed or the object
        counter value if it is still in the workspace.
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
        Need to carry a reference to the repository I am in.
        """
        
        # Now set the fields from that GPB to preempt getter/setter!
        self._gpbFields = field_names


    @property
    def Invalid(self):
        return self._invalid
    
    def Invalidate(self):
        if self.IsRoot:
            for item in self.DerivedWrappers.values():
                item.Invalidate()
            
        self._derived_wrappers = None
        self._gpbMessage = None
        self._parent_links = None
        self._child_links = None
        self._myid = None
        self._repository = None
        self._bytes = None
        self._root = None
        object.__setattr__(self,'_gpbFields',[])
        
        self._invalid = True
        
    @property
    def ObjectClass(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        
        #return self._GPBClass
        return self._GPBClass
    
    @property
    def DESCRIPTOR(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        
        #return self._gpbMessage.DESCRIPTOR
        return self._gpbMessage.DESCRIPTOR
        
    @property
    def Root(self):
        """
        Access to the root object of the nested GPB object structure
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        #return self._root
        return self._root
    
    @property
    def IsRoot(self):
        """
        Is this wrapped object the root of a GPB Message?
        GPBs are also tree structures and each element must be wrapped
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self is self._root
    
    @property
    def ObjectType(self):
        """
        Could just replace the attribute with the capital name?
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        #return self._gpb_type
        return self._gpb_type
    
    @property
    def GPBMessage(self):
        """
        Could just replace the attribute with the capital name?
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        # If this is a proxy object which references its serialized value load it!
        
        #bytes = self._bytes
        bytes = self._bytes
        if  bytes != None:
            #self.ParseFromString(bytes)
            pfs = self.ParseFromString
            pfs(bytes)
            #self._bytes = None
            object.__setattr__(self,'_bytes', None)
            
        #return self._gpbMessage
        return self._gpbMessage
        
    @property
    def Repository(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        return root._repository
    
    @property
    def DerivedWrappers(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        return root._derived_wrappers
    
    def _get_myid(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        return root._myid
    
    def _set_myid(self,value):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        assert isinstance(value, str), 'myid is a string property'
        root = self.Root
        object.__setattr__(root,'_myid', value)

    MyId = property(_get_myid, _set_myid)
    
    
    def _get_parent_links(self):
        """
        A list of all the wrappers which link to me
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        return root._parent_links
        
    def _set_parent_links(self,value):
        """
        A list of all the wrappers which link to me
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        object.__setattr__(root,'_parent_links', value)

    ParentLinks = property(_get_parent_links, _set_parent_links)
        
    def _get_child_links(self):
        """
        A list of all the wrappers which I link to
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        return root._child_links
        
    def _set_child_links(self, value):
        """
        A list of all the wrappers which I link to
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        object.__setattr__(root,'_child_links', value)
        
    ChildLinks = property(_get_child_links, _set_child_links)
        
    def _get_readonly(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        return root._read_only
        
    def _set_readonly(self,value):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        assert isinstance(value, bool), 'readonly is a boolen property'
        root = self.Root
        object.__setattr__(root,'_read_only', value)

    ReadOnly = property(_get_readonly, _set_readonly)
    
    def _get_modified(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        return root._modified

    def _set_modified(self,value):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        assert isinstance(value, bool), 'modified is a boolen property'
        root = self.Root
        object.__setattr__(root,'_modified',value)

    Modified = property(_get_modified, _set_modified)
    
    
    def SetLink(self,value):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        if not self.ObjectType == self.LinkClassType:
            raise OOIObjectError('Can not set link for non link type!')
            
        self.Repository.set_linked_object(self,value)
        if not self.Modified:
            #self._set_parents_modified()
            self._set_parents_modified()
            
        
    def SetLinkByName(self,linkname,value):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        #link = self.GetLink(linkname)
        link = self.GetLink(linkname)
        #link.SetLink(value)
        link.SetLink(value)
        
    def GetLink(self,linkname):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        
        gpb = self.GPBMessage
        link = getattr(gpb,linkname)
        #link = self._rewrap(link)
        link = self._rewrap(link)
        
        if not link.ObjectType == self.LinkClassType:
            raise OOIObjectError('The field "%s" is not a link!' % linkname)
        return link
         
    def InParents(self,value):
        '''
        Check recursively to make sure the object is not already its own parent!
        '''
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        for item in self.ParentLinks:
            if item.Root is value:
                return True
            # if item.InParents(value):
            if item.InParents(value):
                return True
        return False
    
    def SetStructureReadOnly(self):
        """
        Set these objects to be read only
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        object.__setattr__(self, 'read_only',True)
        for link in self.ChildLinks:
            child = self.Repository.get_linked_object(link)
            child.SetStructureReadOnly()
            child.SetStructureReadOnly()
        
    def SetStructureReadWrite(self):
        """
        Set these object to be read write!
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        object.__setattr__(self, 'read_only',False)
        for link in self.ChildLinks:
            child = self.Repository.get_linked_object(link)
            #child.SetStructureReadWrite()
            child.SetStructureReadWrite()

    def RecurseCommit(self,structure):
        """
        Recursively build up the serialized structure elements which are needed
        to commit this wrapper and reset all the links using its CAS name.
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        if not  self.Modified:
            # This object is already committed!
            return
        
        # Create the Structure Element in which the binary blob will be stored
        se = StructureElement()        
        repo = self.Repository
        for link in  self.ChildLinks:
                        
            # Test to see if it is already serialized!
            
            if  repo._hashed_elements.has_key(link.key):
                child_se = repo._hashed_elements.get(link.key)

                # Set the links is leaf property
                link.isleaf = child_se.isleaf
                
            else:
                child = repo.get_linked_object(link)
                
                # Determine whether this is a leaf node
                if len(child.ChildLinks)==0:
                    link.isleaf = True
                else:
                    link.isleaf = False
            
                child.RecurseCommit(structure)
            
            # Save the link info as a convience for sending!
            se.ChildLinks.add(link.key)
                    
        se.value = self.SerializeToString()
        #se.key = sha1hex(se.value)

        # Structure element wrapper provides for setting type!
        se.type = self.ObjectType
        
        # Calculate the sha1 from the serialized value and type!
        # Sha1 is a property - not a method...
        se.key = se.sha1
        
        # Determine whether I am a leaf
        if len(self.ChildLinks)==0:
            se.isleaf=True
        else:
            se.isleaf = False
            
        
        if repo._workspace.has_key(self.MyId):
            
            del repo._workspace[self.MyId]
            repo._workspace[se.key] = self

        object.__setattr__(self,'MyId',se.key)
        
        object.__setattr__(self,'Modified',False)
       
        # Set the key value for parent links!
        for link in self.ParentLinks:
            # Can not use object.__setattr__ on gpb fields!
            link.key = self.MyId
            
        structure[se.key] = se
        
        

    def FindChildLinks(self):
        """
        Find all of the links in this composit structure
        All of the objects worked on in this method are raw proto buffers messages!
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        gpb = self.GPBMessage
        # For each field in the protobuffer message
        for field in gpb.DESCRIPTOR.fields:
            # if the field is a composite - another message
            if field.message_type:
                
                # Get the field of type message
                gpb_field = getattr(gpb,field.name)
                                
                
                # If it is a repeated container type
                if isinstance(gpb_field, containers.RepeatedCompositeFieldContainer):
                    
                    for item in gpb_field:
                        
                        wrapped_item = self._rewrap(item)
                        if wrapped_item.ObjectType == wrapped_item.LinkClassType:
                            self.ChildLinks.add(wrapped_item)
                        else:
                            wrapped_item.FindChildLinks()
                                
                # IF it is a standard message field
                else:
                    if not gpb_field.IsInitialized():
                        # if it is an optional field which is not initialized
                        # it can not hold any links!
                        continue
                    
                    rr= self._rewrap
                    item = rr(gpb_field)
                    if item.ObjectType == item.LinkClassType:
                        self.ChildLinks.add(item)
                    else:
                        fcl = item.FindChildLinks
                        fcl()
    
    def AddParentLink(self, link):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        for parent in self.ParentLinks:
            
            if parent.GPBMessage is link.GPBMessage:
                break
        else:
            self.ParentLinks.add(link)
        
    def _rewrap(self, gpbMessage):
        '''
        Factory method to return a new instance of wrapper for a gpbMessage
        from self - used for access to composite structures, it has all the same
        shared variables as the parent wrapper
        '''
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        # Check the root wrapper objects list of derived wrappers
        objhash = gpbMessage.__hash__()
        dw = self.DerivedWrappers
        if objhash in dw:
            return dw[objhash]
        
        # Else make a new one...
        inst = Wrapper(gpbMessage)        
        inst._root = self._root
        
        # Add it to the list of objects which derive from the root wrapper
        dw[objhash] = inst
        
        return inst
        
    def _set_parents_modified(self):
        """
        This method recursively changes an objects parents to a modified state
        All links are reset as they are no longer hashed values
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        if self.Modified:
            # Be clear about what we are doing here!
            return
        else:
            
            object.__setattr__(self,'Modified', True)
            
            # Get the repository            
            repo = self.Repository
            
            new_id = repo.new_id()
            repo._workspace[new_id] = self.Root
            
            if repo._workspace.has_key(self.MyId):
                del repo._workspace[self.MyId]
            object.__setattr__(self,'MyId', new_id)
              
            # When you hit the commit ref - stop!                   
            if self.Root is repo._workspace_root:
                # The commit is no longer really your parent!
                object.__setattr__(self,'ParentLinks',set())
                
            else:
                
                for link in self.ParentLinks:
                    # Tricky - set the message directly and call modified!
                    #link.GPBMessage.key = self.MyId
                    link.GPBMessage.key = self.MyId
                    #link._set_parents_modified()
                    link._set_parents_modified()
            
    def __eq__(self, other):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        if not isinstance(other, Wrapper):
            return False
        
        if self is other:
            return True
        
        return self.GPBMessage == other.GPBMessage
    
    def __ne__(self, other):
        # Can't just say self != other_msg, since that would infinitely recurse. :)
        return not self == other
    
    def __str__(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        if self.ObjectType == self.LinkClassType:
            msg = '\nkey: %s \ntype { %s }' % (sha1_to_hex(self.GPBMessage.key), self.GPBMessage.type)
        else:
            msg = '\n' +self.GPBMessage.__str__()
            
        return msg
        
    def Debug(self):
        output  = '================== Wrapper (Modified = %s)====================\n' % self.Modified
        output += 'Wrapper ID: %s \n' % self.MyId
        output += 'Wrapper IsRoot: %s \n' % self.IsRoot
        output += 'Wrapper ParentLinks: %s \n' % str(self.ParentLinks)
        output += 'Wrapper ChildLinks: %s \n' % str(self.ChildLinks)
        output += 'Wrapper current value:\n'
        output += str(self) + '\n'
        output += '================== Wrapper Complete ========================='
        return output

    def IsInitialized(self):
        """Checks if the message is initialized.
        
        Returns:
            The method returns True if the message is initialized (i.e. all of its
        required fields are set).
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        #return self.GPBMessage.IsInitialized()
        return self.GPBMessage.IsInitialized()
        
    def SerializeToString(self):
        """Serializes the protocol message to a binary string.
        
        Returns:
          A binary string representation of the message if all of the required
        fields in the message are set (i.e. the message is initialized).
        
        Raises:
          message.EncodeError if the message isn't initialized.
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        #return self.GPBMessage.SerializeToString()
        return self.GPBMessage.SerializeToString()
    
    def ParseFromString(self, serialized):
        """Clear the message and read from serialized."""
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        # Do not use the GPBMessage method - it will recurse!
        #self._gpbMessage.ParseFromString(serialized)
        self._gpbMessage.ParseFromString(serialized)
        
    def ListSetFields(self):
        """Returns a list of (FieldDescriptor, value) tuples for all
        fields in the message which are not empty.  A singular field is non-empty
        if IsFieldSet() would return true, and a repeated field is non-empty if
        it contains at least one element.  The fields are ordered by field
        number"""
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        #return self.GPBMessage.ListFields()
        
        field_list = self.GPBMessage.ListFields()
        fnames=[]
        for desc, val in field_list:
            fnames.append(desc.name)
        return fnames

    def IsFieldSet(self, field_name):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        try:
            #result = self.GPBMessage.HasField(field_name)
            result = self.GPBMessage.HasField(field_name)
        except ValueError, ex:
            raise OOIObjectError('The "%s" object definition does not have a field named "%s"' % \
                    (str(self.ObjectClass), field_name))
            
        return result

    def HasField(self, field_name):
        log.warn('HasField is depricated because the name is confusing. Use IsFieldSet')
        #return self.IsFieldSet(field_name)
        return self.IsFieldSet(field_name)
    
    def ClearField(self, field_name):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        GPBMessage = self.GPBMessage
            
        #if not GPBMessage.IsFieldSet(field_name):
        #    # Nothing to clear
        #    return
            
        # Get the raw GPB field
        try: 
            GPBField = getattr(GPBMessage, field_name)
        except AttributeError, ex:
            raise OOIObjectError('The "%s" object definition does not have a field named "%s"' % \
                    (str(self.ObjectClass), field_name))
            
        if isinstance(GPBField, containers.RepeatedScalarFieldContainer):
            objhash = GPBField.__hash__()
            del self.DerivedWrappers[objhash]
            # Nothing to do - just clear the field. It can not contain a link            

        elif isinstance(GPBField, containers.RepeatedCompositeFieldContainer):
            for item in GPBField:
                wrapped_field = self._rewrap(item)
                wrapped_field._clear_derived_message()
                
                item_hash = item.__hash__()
                del self.DerivedWrappers[item_hash]
                
            objhash = GPBField.__hash__()
            del self.DerivedWrappers[objhash]            

        elif isinstance(GPBField, message.Message):
            wrapped_field = self._rewrap(GPBField)
            wrapped_field._clear_derived_message()
            
            objhash = GPBField.__hash__()
            del self.DerivedWrappers[objhash]
        
        #Now clear the field
        self.GPBMessage.ClearField(field_name)
        # Set this object and it parents to be modified
        self._set_parents_modified()
            
    def _clear_derived_message(self):
        """
        Helper method for ClearField
        """
        if self.ObjectType == self.LinkClassType:
            child_obj = self.Repository.get_linked_object(self)
            # Remove this link from the list of parents
            child_obj.ParentLinks.remove(self)
                
            # This is the only one, remove it as a child
            self.ChildLinks.remove(self)
            
        for field_name in self.DESCRIPTOR.fields_by_name.keys():
            # Recursively remove all 
            self.ClearField(field_name)
        
    #def HasExtension(self, extension_handle):
    #    return self.GPBMessage.HasExtension(extension_handle)
    
    #def ClearExtension(self, extension_handle):
    #    return self.GPBMessage.ClearExtension(extension_handle)
    
    def ByteSize(self):
        """Returns the serialized size of this message.
        Recursively calls ByteSize() on all contained messages.
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self.GPBMessage.ByteSize()
    
    
    
class ContainerWrapper(object):
    """
    This class is only for use with containers.RepeatedCompositeFieldContainer
    It is not needed for repeated scalars!
    """
    
    LinkClassType = create_type_identifier(object_id=3, version=1)
    
    def __init__(self, wrapper, gpbcontainer):
        # Be careful - this is a hard link
        self._wrapper = wrapper
        if not isinstance(gpbcontainer, containers.RepeatedCompositeFieldContainer):
            raise OOIObjectError('The Container Wrapper is only for use with Repeated Composit Field Containers')
        self._gpbcontainer = gpbcontainer
        self.Repository = wrapper.Repository

    @classmethod
    def factory(cls, wrapper, gpbcontainer):
        
        # Check the root wrapper objects list of derived wrappers before making a new one
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        objhash = gpbcontainer.__hash__()
        dw = wrapper.DerivedWrappers
        if dw.has_key(objhash):
            return dw[objhash]
        
        inst = cls(wrapper, gpbcontainer)
            
        # Add it to the list of objects which derive from the root wrapper
        dw[objhash] = inst
        return inst
        

    @property
    def Root(self):
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        return self._wrapper.Root

    @property
    def Invalid(self):
        if not self._wrapper:
            return True
        return self._wrapper._invalid
    
    def Invalidate(self):
        self._gpbcontainer = None
        self._wrapper = None
        self.Repository = None
    
    def __setitem__(self, key, value):
        """Sets the item on the specified position.
        Depricated"""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        
        if not isinstance(value, Wrapper):
            raise OOIObjectError('To set an item in a repeated field container, the value must be a Wrapper')
        
        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
        if item.ObjectType == self.LinkClassType:
            self.Repository.set_linked_object(item, value)
        else:
            raise OOIObjectError('It is illegal to set a value of a repeated composit field unless it is a CASRef - Link')
         
        self._wrapper._set_parents_modified()
         
        
    def SetLink(self,key,value):
        
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        
        if not isinstance(value, Wrapper):
            raise OOIObjectError('To set an item in a repeated field container, the value must be a Wrapper')
        
        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
        if item.ObjectType == self.LinkClassType:
            self.Repository.set_linked_object(item, value)
        else:
            raise OOIObjectError('It is illegal to set a value of a repeated composit field unless it is a CASRef - Link')
         
        self._wrapper._set_parents_modified()
         
            
    def __getitem__(self, key):
        """Retrieves item by the specified key."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        value = self._gpbcontainer.__getitem__(key)
        value = self._wrapper._rewrap(value)
        if value.ObjectType == self.LinkClassType:
            value = self.Repository.get_linked_object(value)
        return value
    
    def GetLink(self,key):
            
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        link = self._gpbcontainer.__getitem__(key)
        link = self._wrapper._rewrap(link)
        assert link.ObjectType == self.LinkClassType, 'The field "%s" is not a link!' % linkname
        return link
        
    def GetLinks(self):
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        wrapper_list=[]            
        links = self._gpbcontainer[:] # Get all the links!
        for link in links:
            link = self._wrapper._rewrap(link)
            assert link.ObjectType == self.LinkClassType, 'The field "%s" is not a link!' % linkname
            wrapper_list.append(link)
        return wrapper_list
    
    def __len__(self):
        """Returns the number of elements in the container."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._gpbcontainer.__len__()
        
    def __ne__(self, other):
        """Checks if another instance isn't equal to this one."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if not isinstance(other, self.__class__):
            raise OOIObjectError('Can only compare repeated composite fields against other repeated composite fields.')
        # The concrete classes should define __eq__.
        return not self._gpbcontainer == other._gpbcontainer

    def __eq__(self, other):
        """Compares the current instance with another one."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        
        if self is other:
            return True
        
        if not isinstance(other, self.__class__):
            raise OOIObjectError('Can only compare repeated composite fields against other repeated composite fields.')
        return self._gpbcontainer == other._gpbcontainer

    def __repr__(self):
        """Need to improve this!"""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._gpbcontainer.__repr__()
        
        
    # Composite specific methods:
    def add(self):
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        new_element = self._gpbcontainer.add()
        
        self._wrapper._set_parents_modified()
        return self._wrapper._rewrap(new_element)
        
    def __getslice__(self, start, stop):
        """Retrieves the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        wrapper_list=[]
        for index in range(0, len(self))[start:stop]:
            wrapper_list.append(self.__getitem__(index))
            
        # Does it make sense to return a list?
        return wrapper_list
    
    def __delitem__(self, key):
        """Deletes the item at the specified position."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        self._wrapper._set_parents_modified()
            
        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
            
        item._clear_derived_message()
            
        self._gpbcontainer.__delitem__(key)
        
    def __delslice__(self, start, stop):
        """Deletes the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        i_range = range(0, len(self))[start:stop]
        for index in reversed(i_range):
            self.__delitem__(index)
    
    
    
class ScalarContainerWrapper(object):
    """
    This class is only for use with containers.RepeatedCompositeFieldContainer
    It is not needed for repeated scalars!
    """
        
    def __init__(self, wrapper, gpbcontainer):
        # Be careful - this is a hard link
        self._wrapper = wrapper
        if not isinstance(gpbcontainer, containers.RepeatedScalarFieldContainer):
            raise OOIObjectError('The Container Wrapper is only for use with Repeated Composit Field Containers')
        self._gpbcontainer = gpbcontainer
        self.Repository = wrapper.Repository 

    @classmethod
    def factory(cls, wrapper, gpbcontainer):
        
        # Check the root wrapper objects list of derived wrappers before making a new one
        objhash = gpbcontainer.__hash__()
        dw = wrapper.DerivedWrappers
        if dw.has_key(objhash):
            return dw[objhash]
        
        inst = cls(wrapper, gpbcontainer)
            
        # Add it to the list of objects which derive from the root wrapper
        dw[objhash] = inst
        return inst

    @property
    def Invalid(self):
        if not self._wrapper:
            return True
        return self._wrapper._invalid
    
    def Invalidate(self):
        self._gpbcontainer = None
        self._wrapper = None
        self.Repository = None
    
    def append(self, value):
        """Appends an item to the list. Similar to list.append()."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
           
        self._gpbcontainer.append(value)
        self._wrapper._set_parents_modified()

    def insert(self, key, value):
        """Inserts the item at the specified position. Similar to list.insert()."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
           
        self._gpbcontainer.insert(key, value)
        self._wrapper._set_parents_modified()

    def extend(self, elem_seq):
        """Extends by appending the given sequence. Similar to list.extend()."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
           
        self._gpbcontainer.extend(elem_seq)
        self._wrapper._set_parents_modified()

    def remove(self, elem):
        """Removes an item from the list. Similar to list.remove()."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
           
        self._gpbcontainer.remove(elem)
        self._wrapper._set_parents_modified()

    
    def __getslice__(self, start, stop):
        """Retrieves the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._gpbcontainer._values[start:stop]

    def __len__(self):
        """Returns the number of elements in the container."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return len(self._gpbcontainer._values)

    def __getitem__(self, key):
        """Retrieves the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._gpbcontainer._values[key]

    def __setitem__(self, key, value):
        """Sets the item on the specified position."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
           
        self._gpbcontainer.__setitem__(key, value)
        self._wrapper._set_parents_modified()

    def __setslice__(self, start, stop, values):
        """Sets the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
           
        self._gpbcontainer.__setslice__(start, stop, values)
        self._wrapper._set_parents_modified()

    def __delitem__(self, key):
        """Deletes the item at the specified position."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
           
        del self._gpbcontainer._values[key]
        self._gpbcontainer._message_listener.Modified()
        self._wrapper._set_parents_modified()

    def __delslice__(self, start, stop):
        """Deletes the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
            
        self._gpbcontainer._values.__delslice__(start,stop)
        self._gpbcontainer._message_listener.Modified()
        self._wrapper._set_parents_modified()

    def __eq__(self, other):
        """Compares the current instance with another one."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        
        if self is other:
            return True
        # Special case for the same type which should be common and fast.
        if isinstance(other, self.__class__):
            return other._gpbcontainer._values == self._gpbcontainer._values
        # We are presumably comparing against some other sequence type.
        return other == self._gpbcontainer._values

    def __ne__(self, other):
        """Checks if another instance isn't equal to this one."""
        # The concrete classes should define __eq__.
        return not self == other

    def __repr__(self):
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return repr(self._gpbcontainer._values)
    
    
class StructureElement(object):
    """
    @brief Wrapper for the container structure element. These are the objects
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
        
    @property
    def sha1(self):
        """
        Make the sha1 safe for empty contents but also type safe.
        Take use the sha twice so that we don't need to concatinate long strings!
        """
        #################
        ## This is the method that you can compare in Java
        #################
        ## Get the length of the binary arrays
        #sha_len = 20
        #type_len = self.type.ByteSize()
        #
        ## Convert to signed integer bytes
        #fmt = '!%db' % type_len
        #type_bytes = struct.unpack('!%db' % type_len , self.type.SerializeToString())
        #
        ## Convert the sha1 of the content to signed integer bytes
        #c_sha_bytes = struct.unpack('!20b', sha1bin(self.value))
        #
        ## Concatinate the the byte arrays as integers
        #cat_bytes = list(c_sha_bytes) + list(type_bytes)
        #
        ## Get the length of the concatination and convert to byte array
        #fmt = '!%db' % (type_len+sha_len)
        #sha_cat = struct.pack(fmt, *cat_bytes)
        #
        ##print 'sha1hex(sha_cat):',sha1hex(sha_cat)
        ##print 'sha1hex(sha1bin(self.value) + self.type.SerializeToString()):',sha1hex(sha1bin(self.value) + self.type.SerializeToString())
        #
        ## Return the sha1 of the byte array
        #return sha1bin(sha_cat)
        #################
        # This does the same thing much faster and shorter!
        #################
        return sha1bin(sha1bin(self.value) + self.type.SerializeToString())
        
    #@property
    def _get_type(self):
        return self._element.type
        
    #@type.setter
    def _set_type(self,obj_type):
        self._element.type.object_id = obj_type.object_id
        self._element.type.version = obj_type.version
     
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
        #return sha1_to_hex(self._element.key)
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
        msg = ''
        if len(self._element.key)==20:
            msg  = 'Hexkey: "'+sha1_to_hex(self._element.key) +'"\n'
        return msg + self._element.__str__()
        
