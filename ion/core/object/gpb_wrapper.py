#!/usr/bin/env python
"""
@file ion/core/object/gpb_wrapper.py
@brief Wrapper for Google Protocol Buffer Message Classes.
These classes are the lowest level of the object management stack
@author David Stuebe
@author 
TODO:
"""

from ion.core.object.object_utils import get_type_from_obj, sha1bin, sha1hex,\
    sha1_to_hex, ObjectUtilException, create_type_identifier, get_gpb_class_from_type_id, OOIObjectError

import StringIO

from ion.core.object.object_utils import CDM_GROUP_TYPE, CDM_DATASET_TYPE, CDM_ATTRIBUTE_TYPE, CDM_DIMENSION_TYPE, CDM_VARIABLE_TYPE

# Get the object decorators used on all wrapper methods!
from ion.core.object.object_utils import _gpb_source, _gpb_source_root

import struct

from google.protobuf import message
from google.protobuf.internal import containers
from google.protobuf import descriptor

from ion.core.object.cdm_methods import dataset
from ion.core.object.cdm_methods import variables
from ion.core.object.cdm_methods import attribute
from ion.core.object.cdm_methods import group
from ion.core.object.cdm_methods import attribute_merge

import ion.util.ionlog
from ion.core import ioninit

CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)

STRUCTURE_ELEMENT_TYPE = create_type_identifier(object_id=1, version=1)
LINK_TYPE = create_type_identifier(object_id=3, version=1)

class WrappedEnum(object):
    """ Data descriptor (like a property) for passing through GPB enums from the Wrapper. """

    def __init__(self, val, doc=None):
        self.val = val
        if doc: self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        return self.val

    def __set__(self, obj, value):
        raise AttributeError('Enums are read-only.')

    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')


class EnumType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.
    """

    _type_cache = {}

    def __call__(cls, enum_type_descriptor, *args, **kwargs):
        # Cache the custom-built classes

        assert isinstance(enum_type_descriptor, descriptor.EnumDescriptor)

        clsType = None
        enum_name = enum_type_descriptor.name
        enum_full_name = enum_type_descriptor.full_name

        if enum_full_name in EnumType._type_cache:
            clsType = EnumType._type_cache[enum_full_name]
        else:
            clsName = '%s_%s' % (cls.__name__, enum_name)
            clsDict = {}

            lookup = {}
            clsDict['lookup'] = lookup

            for name, val_desc in enum_type_descriptor.values_by_name.items():
                prop = WrappedEnum(val_desc.number)

                clsDict[name] = prop

                lookup[val_desc.number] = name

            clsType = EnumType.__new__(EnumType, clsName, (cls,), clsDict)

            # Also set the enum descriptors _after_ building the class so the descriptor doesn't go away
            #if hasattr(descriptor, 'enum_values_by_name'):
            #    for k,v in descriptor.enum_values_by_name.iteritems():
            #        setattr(clsType, k, WrappedEnum(v.number))

            EnumType._type_cache[enum_full_name] = clsType

        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(EnumType, clsType).__call__(enum_type_descriptor, *args, **kwargs)
        return obj


class EnumObject(object):
    '''
    A Class for GPB Enum access
    '''

    __metaclass__ = EnumType

    def __init__(self, enum_type_descriptor):
        """
        Instantiate a class with properties to get GPB Enum Values
        """


class WrappedProperty(object):
    def __init__(self, name, doc=None, field_type=None, field_enum=None):
        self.name = name
        if doc: self.__doc__ = doc

        if field_type is None:
            pass
        elif field_type == 8:
            self.field_type = "TYPE_BOOL"
        elif field_type == 12:
            self.field_type = "TYPE_BYTES"
        elif field_type == 1:
            self.field_type = "TYPE_DOUBLE"
        elif field_type == 14:
            self.field_type = "TYPE_ENUM"
        elif field_type == 7:
            self.field_type = "TYPE_FIXED32"
        elif field_type == 6:
            self.field_type = "TYPE_FIXED64"
        elif field_type == 2:
            self.field_type = "TYPE_FLOAT"
        elif field_type == 10:
            self.field_type = "TYPE_GROUP"
        elif field_type == 5:
            self.field_type = "TYPE_INT32"
        elif field_type == 3:
            self.field_type = "TYPE_INT64"
        elif field_type == 2:
            self.field_type = "TYPE_FLOAT"
        elif field_type == 11:
            self.field_type = 'TYPE_MESSAGE'
        elif field_type == 15:
            self.field_type = 'TYPE_SFIXED32'
        elif field_type == 16:
            self.field_type = 'TYPE_SFIXED64'
        elif field_type == 17:
            self.field_type = 'TYPE_SINT32'
        elif field_type == 18:
            self.field_type = 'TYPE_SINT64'
        elif field_type == 9:
            self.field_type = 'TYPE_STRING'
        elif field_type == 13:
            self.field_type = 'TYPE_UINT32'
        elif field_type == 4:
            self.field_type = 'TYPE_UINT64'
        else:
            raise OOIObjectError('Unknow field type "%s" in property constructor.' % field_type)

        self.field_enum = field_enum

    def __get__(self, wrapper, objtype=None):
        raise NotImplementedError('Abstract base class for property wrappers: __get__')

    def _get_backdoor(self, wrapper):
        raise NotImplementedError('Abstract base class for property wrappers: _get_backdoor')

    def __set__(self, wrapper, value):
        raise NotImplementedError('Abstract base class for property wrappers: __set__')


    def __delete__(self, wrapper):
        raise NotImplementedError('Abstrat base class for property wrappers: __delete__')


class WrappedMessageProperty(WrappedProperty):
    """ Data descriptor (like a property) for passing through GPB properties of Type Message from the Wrapper. """

    def __get__(self, wrapper, objtype=None):
        if wrapper.Invalid:
            log.error(wrapper.Debug())
            raise OOIObjectError('Can not get message (composite) property - %s - in a wrapper which is invalidated.' % self.name)
            # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper.GPBMessage, self.name)
        result = wrapper._rewrap(field)

        if result.ObjectType == LINK_TYPE:
            result = wrapper.Repository.get_linked_object(result)

        return result

    def _get_backdoor(self, wrapper):
        if wrapper.Invalid:
            log.error(wrapper.Debug())
            raise OOIObjectError('Can not get message (composite) property - %s -in a wrapper which is invalidated.' % self.name)
            # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper.GPBMessage, self.name)
        result = wrapper._rewrap(field)

        return result


    def __set__(self, wrapper, value):
        if wrapper.Invalid:
            log.error(wrapper.Debug())
            raise OOIObjectError('Can not set message (composite) property - %s - in a wrapper which is invalidated.' % self.name)

        if wrapper.ReadOnly:
            raise OOIObjectError('This object wrapper is read only!')

        wrapper.SetLinkByName(self.name, value)
        wrapper._set_parents_modified()

        return None

    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')


class WrappedRepeatedScalarProperty(WrappedProperty):
    """ Data descriptor (like a property) for passing through GPB properties of Type Repeated Scalar from the Wrapper. """

    def __get__(self, wrapper, objtype=None):
        if wrapper.Invalid:
            log.error(wrapper.Debug())
            raise OOIObjectError('Can not get repeated scalar property - %s - in a wrapper which is invalidated.'% self.name)
            # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper.GPBMessage, self.name)

        return ScalarContainerWrapper.factory(wrapper, field)

    def __set__(self, wrapper, value):
        raise AttributeError('Assignment is not allowed for field name "%s" of type Repeated Scalar in ION Object' % self.name)

    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')


class WrappedRepeatedCompositeProperty(WrappedProperty):
    """ Data descriptor (like a property) for passing through GPB properties of Type Repeated Composite from the Wrapper. """

    def __get__(self, wrapper, objtype=None):
        if wrapper.Invalid:
            log.error(wrapper.Debug())
            raise OOIObjectError('Can not "get" from a repeated composite property - %s - in a wrapper which is invalidated.' % self.name)

        # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper.GPBMessage, self.name)

        return ContainerWrapper.factory(wrapper, field)

    def _get_backdoor(self, wrapper, objtype=None):
        if wrapper.Invalid:
            log.error(wrapper.Debug())
            raise OOIObjectError(
                'Can not get_backdoor from a repeated composite property - %s - in a wrapper which is invalidated.' % self.name)

        # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper.GPBMessage, self.name)

        return ContainerWrapper.factory(wrapper, field)

    def __set__(self, wrapper, value):
        raise AttributeError(
            'Assignment is not allowed for field name - "%s" - of type Repeated Composite in ION Object' % self.name)

    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')


class WrappedScalarProperty(WrappedProperty):
    """ Data descriptor (like a property) for passing through GPB properties of Type Scalar from the Wrapper. """

    def __get__(self, wrapper, objtype=None):
        # This may be the result we were looking for, in the case of a simple scalar field
        if wrapper.Invalid:
            log.error(wrapper.Debug())
            raise OOIObjectError('Can not get scalar property - %s - in a wrapper which is invalidated.' % self.name)

        return getattr(wrapper.GPBMessage, self.name)

    def __set__(self, wrapper, value):
        if wrapper.Invalid:
            log.error(wrapper.Debug())
            raise OOIObjectError('Can not set scalar property - %s -in a wrapper which is invalidated.' % self.name)

        if wrapper.ReadOnly:
            raise OOIObjectError('This object wrapper is read only!')

        setattr(wrapper.GPBMessage, self.name, value)

        # Set this object and it parents to be modified
        wrapper._set_parents_modified()

        return None

    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')


class CommitCounter(object):
    """
    Class used to count the number of recursive calls to commit a data structure
    """
    count = 0


class WrapperType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.
    
    This approach is generally applicable to wrap data structures. It is extremely powerful!
    """

    _type_cache = {}

    recurse_counter = CommitCounter()


    def __call__(cls, gpbMessage, *args, **kwargs):
        # Cache the custom-built classes
        msgType, clsType = type(gpbMessage), None

        if msgType in WrapperType._type_cache:
            clsType = WrapperType._type_cache[msgType]
        else:
            # Check that the object we are wrapping is a Google Message object
            if not isinstance(gpbMessage, message.Message):
                raise OOIObjectError('Wrapper init argument must be an instance of a GPB message')

            # Get the class name
            clsName = '%s_%s' % (cls.__name__, msgType.__name__)
            clsDict = {}
            properties = {}
            enums = {}

            clsDict['_GPBClass'] = gpbMessage.__class__
            clsDict['_Properties'] = properties
            clsDict['_Enums'] = enums

            clsDict['recurse_count'] = WrapperType.recurse_counter

            # Now setup the properties to map through to the GPB object
            descriptor = msgType.DESCRIPTOR

            # Add the enums of the message class
            if hasattr(descriptor, 'enum_types_by_name'):
                for enum_name, enum_desc in descriptor.enum_types_by_name.iteritems():
                    enum_obj = EnumObject(enum_desc)
                    clsDict[enum_name] = enum_obj
                    enums[enum_name] = enum_obj

            # Add the property wrappers for each of the fields of the message
            for fieldName, field_desc in descriptor.fields_by_name.items():
                fieldType = getattr(msgType, fieldName)

                # Add any enums for the fields the message contains
                enum_desc = field_desc.enum_type
                field_enum = None
                if enum_desc:
                    field_enum = EnumObject(enum_desc)
                    clsDict[enum_desc.name] = field_enum
                    enums[enum_desc.name] = field_enum

                field_type = field_desc.type

                prop = None
                if field_desc.label == field_desc.LABEL_REPEATED:
                    if field_desc.cpp_type == field_desc.CPPTYPE_MESSAGE:
                        prop = WrappedRepeatedCompositeProperty(fieldName, doc=fieldType.__doc__, field_type=field_type,
                                                                field_enum=field_enum)
                    else:
                        prop = WrappedRepeatedScalarProperty(fieldName, doc=fieldType.__doc__, field_type=field_type,
                                                             field_enum=field_enum)
                else:
                    if field_desc.cpp_type == field_desc.CPPTYPE_MESSAGE:
                        prop = WrappedMessageProperty(fieldName, doc=fieldType.__doc__, field_type=field_type,
                                                      field_enum=field_enum)
                    else:
                        prop = WrappedScalarProperty(fieldName, doc=fieldType.__doc__, field_type=field_type,
                                                     field_enum=field_enum)

                clsDict[fieldName] = prop
                properties[fieldName] = prop

            # Set the object type:
            if clsDict.has_key('_MessageTypeIdentifier'):
                mti = clsDict['_MessageTypeIdentifier']
                obj_type = create_type_identifier(object_id=mti._ID,\
                                                  version=mti._VERSION)
            else:
                obj_type = create_type_identifier(object_id=-99,\
                                                  version=1)
            clsDict['_gpb_type'] = obj_type
            # the obj_type can now be used for adding special methods to the Wrapper for certain types

            # Special methods for certain object types:
            WrapperType._add_specializations(cls, obj_type, clsDict)

            VALIDATE_ATTRS = CONF.getValue('VALIDATE_ATTRS', True)
            if VALIDATE_ATTRS:
                def obj_setter(self, k, v):
                    if self._init and not hasattr(self, k):
                        raise AttributeError(\
                            '''Cant add properties to the ION object wrapper for object Class "%s".\n'''
                            '''Unknown property name - "%s"; value - "%s"''' % (self._GPBClass, k, v))
                    super(Wrapper, self).__setattr__(k, v)

                clsDict['__setattr__'] = obj_setter

            clsDict['_init'] = False

            clsType = WrapperType.__new__(WrapperType, clsName, (cls,), clsDict)

            WrapperType._type_cache[msgType] = clsType


        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(WrapperType, clsType).__call__(gpbMessage, *args, **kwargs)

        return obj


    def _add_specializations(cls, obj_type, clsDict):
        #--------------------------------------------------------------#
        # Attach specialized methods to object class dictionaries here #
        #--------------------------------------------------------------#
        if obj_type == LINK_TYPE:
            @_gpb_source
            def obj_setlink(self, value, ignore_copy_errors=False):

                self.Repository.set_linked_object(self, value, ignore_copy_errors=ignore_copy_errors)
                if not self.Modified:
                    self._set_parents_modified()
                return

            clsDict['SetLink'] = obj_setlink

            @_gpb_source
            def copy_link(self,link):

                if link.ObjectType != LINK_TYPE:
                    log.error(link.Debug())
                    log.error(self.Debug())
                    raise OOIObjectError('Can not copy_link from an object that is not a link!')

                self.GPBMessage.CopyFrom(link.GPBMessage)

                self.ChildLinks.add(self)

                try:
                    obj = self.Repository.get_linked_object(link)
                    obj.AddParentLink(link)
                except KeyError:
                    log.warn('Copy Link does not have the linked object!')

            clsDict['CopyLink'] = copy_link

        elif obj_type == CDM_DATASET_TYPE:
            clsDict['MakeRootGroup'] = dataset._make_root_group
            clsDict['ShowVariableNames'] = dataset._get_variable_names
            clsDict['ShowGlobalAttributes'] = dataset._get_group_attributes_for_display
            clsDict['ShowVariableAttributes'] = dataset._get_variable_attributes_for_display

        elif obj_type == CDM_GROUP_TYPE:
            clsDict['AddGroup'] = group._add_group_to_group
            clsDict['AddAttribute'] = group._add_attribute
            clsDict['AddDimension'] = group._add_dimension
            clsDict['AddVariable'] = group._add_variable
            clsDict['FindGroupByName'] = group._find_group_by_name
            clsDict['FindAttributeByName'] = group._find_attribute_by_name
            clsDict['FindDimensionByName'] = group._find_dimension_by_name
            clsDict['FindVariableByName'] = group._find_variable_by_name
            clsDict['FindVariableIndexByName'] = group._find_variable_index_by_name
            clsDict['FindAttributeIndexByName'] = group._find_attribute_index_by_name
            clsDict['HasAttribute'] = group._cdm_resource_has_attribute
            clsDict['RemoveAttribute'] = group._remove_attribute
            clsDict['SetAttribute'] = group._set_attribute
            clsDict['SetDimension'] = group._set_dimension

            clsDict['MergeAttSrc'] = attribute_merge.MergeAttSrc
            clsDict['MergeAttDst'] = attribute_merge.MergeAttDst
            clsDict['MergeAttGreater'] = attribute_merge.MergeAttGreater
            clsDict['MergeAttLesser'] = attribute_merge.MergeAttLesser
            clsDict['MergeAttDstOver'] = attribute_merge.MergeAttDstOver
            clsDict['_GetNumericValue'] = attribute_merge._GetNumericValue




            # Allow the DataType enum to be accessible by this Wrapper...
            #------------------------------------------------------------------------#
            # Additional helper methods for attaching specialized attributes/methods #
            #------------------------------------------------------------------------#
            def __add_data_type_enum(clsDict):
                # Get a handle to the data_type field from the variable class
                VAR_CLASS = get_gpb_class_from_type_id(CDM_VARIABLE_TYPE)
                field_desc = None
                for name, desc in VAR_CLASS.DESCRIPTOR.fields_by_name.items():
                    if name == "data_type":
                        field_desc = desc

                # Add the enum definitions from the data_type field (can only be one - DataType)
                enum_desc = field_desc.enum_type
                if enum_desc and not enum_desc.name in clsDict:
                    clsDict[enum_desc.name] = EnumObject(enum_desc)


            __add_data_type_enum(clsDict)


        elif obj_type == CDM_ATTRIBUTE_TYPE:
            clsDict['GetValue'] = attribute._get_attribute_value_by_index
            clsDict['GetValues'] = attribute._get_attribute_values
            # clsDict['SetValue'] = _get_attribute_values
            # clsDict['SetValues'] = _get_attribute_values
            clsDict['GetLength'] = attribute._get_attribute_values_length
            clsDict['GetDataType'] = attribute._get_attribute_data_type
            clsDict['IsSameType'] = attribute._attribute_is_same_type




        elif obj_type == CDM_VARIABLE_TYPE:
            clsDict['GetUnits'] = variables._get_var_units
            clsDict['GetStandardName'] = variables._get_var_std_name
            clsDict['GetNumDimensions'] = variables._get_var_num_dims
            clsDict['GetNumBoundedArrays'] = variables._get_var_num_ba
            clsDict['AddAttribute'] = group._add_attribute
            clsDict['FindAttributeByName'] = group._find_attribute_by_name
            clsDict['FindDimensionByName'] = group._find_dimension_by_name
            clsDict['FindAttributeIndexByName'] = group._find_attribute_index_by_name
            clsDict['HasAttribute'] = group._cdm_resource_has_attribute
            clsDict['RemoveAttribute'] = group._remove_attribute
            clsDict['SetAttribute'] = group._set_attribute
            clsDict['SetDimension'] = group._set_dimension

            clsDict['GetValue'] = variables.GetValue

            clsDict['MergeAttSrc'] = attribute_merge.MergeAttSrc
            clsDict['MergeAttDst'] = attribute_merge.MergeAttDst
            clsDict['MergeAttGreater'] = attribute_merge.MergeAttGreater
            clsDict['MergeAttLesser'] = attribute_merge.MergeAttLesser
            clsDict['MergeAttDstOver'] = attribute_merge.MergeAttDstOver
            clsDict['_GetNumericValue'] = attribute_merge._GetNumericValue


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


    def __init__(self, gpbMessage):
        """
        Initialize the Wrapper class and set up it message type.

        """

        self._gpbMessage = gpbMessage
        """
        The gpbMessage is the data object which this instance of wrapper provides
        proxy access to. This wrapper controls both the message object and its
        nested child objects in the case of a composite message. Each child in
        the nested object is returned as a wrapped object all of which are subordinate
        to the wrapper for the root of the composite message.
        """

        self._root = None
        """
        A reference to the root object wrapper for this protobuffer
        A composit protobuffer object may return
        """

        self._invalid = None
        """
        Used to determine whether the wrapper is a currently valid object in the
        version framework. Invalid states can be created when old references to
        an object remain after a differnt version is checked out - similar to
        having a file or directory open that does not exist in the currently
        checked out branch of a source repository.
        """

        # This filed now comes from the metaclass
        #self._gpb_type = None
        """
        The Type ID object for the wrapped object of this class
        """

        self._bytes = None
        """
        Used in _load_element to create a proxy object. The bytes are not loaded
        parsed until the object is needed!
        """

        self._parent_links = None
        """
        A list of all the other wrapper objects which link to me
        """

        self._child_links = None
        """
        A list of my child link wrappers
        """

        self._derived_wrappers = None
        """
        A container for all the wrapper objects which are rewrapped, derived
        from a root object wrapper
        """

        self._myid = None # only exists in the root object
        """
        The name for this object - the SHA1 if it is already hashed or the object
        counter value if it is still in the workspace.
        """

        self._modified = None # only exists in the root object
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

        self._source = self
        """
        To avoid invalidating during when there is a hash conflict in the workspace - set the twin...
        """

        self.__no_string = CONF.getValue('STR_GPBS', False)

        # Hack to prevent setting properties in a class instance
        self._init = True

        #frame = sys._getframe(2)
        #frames = []
        #for i in range(6):
        #    frames.append(frame)
        #    frame = frame.f_back
        #print 'Name: %s, Caller names: ' % type(self), [frame.f_code.co_name for frame in frames]


    GPBSource = _gpb_source

    GPBSourceRoot = _gpb_source_root


    @classmethod
    def _create_object(cls, msgtype):
        """
        This is a convience method to create unattached wrapper objects.
        Generally this method should not be used - it is a hook for testing.
        """

        gpbMessage = get_gpb_class_from_type_id(msgtype)()

        obj = cls(gpbMessage)
        obj._repository = None
        obj._root = obj
        obj._parent_links = set()
        obj._child_links = set()
        obj._derived_wrappers = {}
        obj._read_only = False
        obj._myid = '-1'
        obj._modified = True
        obj._invalid = False

        return obj

    @property
    def Invalid(self):
        return self._source._invalid

    def noisy_invalidate(self):
        pass

    def Invalidate(self, other=None):


        self.noisy_invalidate()
        if other is not None:

            #log.debug('Invalidating self:\n%s' % self.Debug())

            #log.debug('Invalidating self with Other:\n%s \n\n' % other.Debug())

            if self.ObjectType != other.ObjectType:
                log.error(self.Debug())
                raise OOIObjectError('Can not invalidate by merge when two objects are not the same type')

            if self._invalid:
                log.error(self.Debug())
                raise OOIObjectError('It is unexpected to try and invalidate an object with a new source a second time')

            if self._source is not self:
                log.error(self.Debug())
                raise OOIObjectError(
                    'It is unexpected to try and invalidate an object which already has an alternate source')

            if self.Repository is not other.Repository:
                log.error(self.Debug())
                raise OOIObjectError('Can not invalidate by passing a wrapper from another repository')

            if other.Invalid:
                log.error('Error while invalidating self - other is invalid too!\nSelf: %s\nOther: %s'% (self.Debug(), other.Debug()))
                raise OOIObjectError('Can not invalidate self with other when other is already invalid')

        else:
            #log.debug('Invalidating self:\n%s' % self.Debug())

            other = self

            #if self._invalid:
            if self.Invalid:
                return

        if other is not self:
            # If we are doing an invalidate to other...
            self._merge_derived_wrappers(other._source)


        elif self.IsRoot:
            # If this is a straight invalidation - clear the derived wrappers if root
            for item in self.DerivedWrappers.values():
                item.Invalidate()

        # Source must always be set to self or another gpb_wrapper object!
        self._source = other._source

        self._derived_wrappers = None
        self._gpbMessage = None
        self._parent_links = None
        self._child_links = None
        self._myid = None
        self._bytes = None

        # Do not clear root or Repository

        self._invalid = True


    def _merge_derived_wrappers(self, other):
        for name, prop in self._Properties.iteritems():
            if prop.field_type == 'TYPE_MESSAGE':
                self_obj = prop._get_backdoor(self)

                other_obj = prop._get_backdoor(other)
                if self_obj is other_obj:
                    raise OOIObjectError('The back door property getter failed!')


                log.debug('Invalidating message property: %s' % prop.name)
                if isinstance(self_obj, ContainerWrapper):
                    # Make sure to get the derive object not what it links to!
                    for gpb_item_self, gpb_item_other in zip(self_obj._gpbcontainer, other_obj._gpbcontainer):

                        item_self = self_obj._wrapper._rewrap(gpb_item_self)
                        item_other = other_obj._wrapper._rewrap(gpb_item_other)

                        item_self.Invalidate(other = item_other)

                else:
                    self_obj.Invalidate(other = other_obj)


    @property
    @GPBSource
    def ObjectClass(self):
        return self._GPBClass

    @property
    @GPBSource
    def DESCRIPTOR(self):
        return self._gpbMessage.DESCRIPTOR

    @property
    @GPBSourceRoot
    def Root(self):
        """
        Access to the root object of the nested GPB object structure
        """
        return self

    @property
    @GPBSource
    def IsRoot(self):
        """
        Is this wrapped object the root of a GPB Message?
        GPBs are also tree structures and each element must be wrapped
        """
        return self is self._root

    @property
    @GPBSource
    def ObjectType(self):
        """
        Could just replace the attribute with the capital name?
        """
        return self._gpb_type

    @property
    @GPBSource
    def GPBMessage(self):
        """
        If this is a proxy object which references its serialized value load it!
        """
        bytes = self._bytes
        if  bytes != None:
            self.ParseFromString(bytes)
            self._bytes = None
        return self._gpbMessage

    @property
    @GPBSourceRoot
    def Repository(self):
        return self._repository

    @property
    @GPBSourceRoot
    def DerivedWrappers(self):
        return self._derived_wrappers

    @GPBSourceRoot
    def _get_myid(self):
        return self._myid

    @GPBSourceRoot
    def _set_myid(self, value):
        assert isinstance(value, str), 'myid is a string property'
        self._myid = value

    MyId = property(_get_myid, _set_myid)


    @GPBSourceRoot
    def _get_parent_links(self):
        """
        A list of all the wrappers which link to me
        """
        return self._parent_links

    @GPBSourceRoot
    def _set_parent_links(self, value):
        """
        A list of all the wrappers which link to me
        """

        self._parent_links = value

    ParentLinks = property(_get_parent_links, _set_parent_links)

    @GPBSourceRoot
    def _get_child_links(self):
        """
        A list of all the wrappers which I link to
        """
        return self._child_links

    @GPBSourceRoot
    def _set_child_links(self, value):
        """
        A list of all the wrappers which I link to
        """
        self._child_links = value

    ChildLinks = property(_get_child_links, _set_child_links)

    @GPBSourceRoot
    def _get_readonly(self):
        return self._read_only

    @GPBSourceRoot
    def _set_readonly(self, value):
        assert isinstance(value, bool), 'readonly is a boolen property'
        self._read_only = value

    ReadOnly = property(_get_readonly, _set_readonly)

    @GPBSourceRoot
    def _get_modified(self):
        return self._modified

    @GPBSourceRoot
    def _set_modified(self, value):
        assert isinstance(value, bool), 'modified is a boolen property'
        self._modified = value

    Modified = property(_get_modified, _set_modified)


    @GPBSource
    def SetLinkByName(self, linkname, value):
        link = self.GetLink(linkname)
        link.SetLink(value)

    @GPBSource
    def GetLink(self, linkname):
        gpb = self.GPBMessage
        link = getattr(gpb, linkname)
        link = self._rewrap(link)

        if not link.ObjectType == LINK_TYPE:
            raise OOIObjectError('The field "%s" is not a link!' % linkname)
        return link

    @GPBSource
    def InParents(self, value):
        '''
        Check recursively to make sure the object is not already its own parent!
        '''
        if not value.IsRoot:
            raise OOIObjectError('Can not test lineage of object that is not a root object')

        for item in self.ParentLinks:
            if item.Root is value:
                return True
            if item.InParents(value):
                return True
        return False

    @GPBSource
    def SetStructureReadOnly(self):
        """
        Set these objects to be read only
        """

        self.ReadOnly = True
        for link in self.ChildLinks:
            try:
                child = self.Repository.get_linked_object(link)
                child.SetStructureReadOnly()
            except KeyError:
                # we're just setting things read only, don't worry if we couldn't get part of it.
                # this is intentionally happening in ingestion
                pass

    @GPBSource
    def SetStructureReadWrite(self):
        """
        Set these object to be read write!
        """

        self.ReadOnly = False
        for link in self.ChildLinks:
            child = self.Repository.get_linked_object(link)
            child.SetStructureReadWrite()

    @GPBSource
    def RecurseCommit(self, structure):
        """
        Recursively build up the serialized structure elements which are needed
        to commit this wrapper and reset all the links using its CAS name.
        """

        # Should this error if called on a non root object?
        if not self.IsRoot:
            raise OOIObjectError('Can not call Recurse Commit on a non root object wrapper.')

        self.recurse_count.count += 1
        local_cnt = self.recurse_count.count
        log.debug('Entering Recurse Commit: recurse counter - %d, Object Type - %s, child links - %d, objects to commit - %d, Modified - %s' %
              (local_cnt, type(self), len(self.ChildLinks), len(structure), self.Modified))

        if not  self.Modified:
            # This object is already committed!
            log.debug('Exiting Recurse Commit: recurse counter - %d' % local_cnt)

            return

        # Create the Structure Element in which the binary blob will be stored
        se = StructureElement()
        repo = self.Repository

        for link in  self.ChildLinks:

            if link.Invalid:
                log.error('Link in child links is invalid!')
                log.debug('Current Wrapper: %s' % self.Debug())
                log.debug('Invalid Link %s' % link.Debug())

            # Test to see if it is already serialized!
            child_se = repo.index_hash.get(link.key, structure.get(link.key, None))
            #child_se = repo.index_hash.get(link.key, None)

            #log.debug('Setting child Link: %s' % str(child_se))
            if  child_se is not None:
                # Set the links is leaf property
                link.isleaf = child_se.isleaf

            else:
                # if isleaf set, type set, and the key is an actual SHA1 - we don't need to recurse into it or do anything, really.
                if link.IsFieldSet('isleaf') and link.IsFieldSet('type') and len(link.key) == 20:
                    log.debug('Disregarding un-index-hashed link %s' % link.key)
                    pass
                else:
                    child = repo.get_linked_object(link)

                    # Determine whether this is a leaf node
                    if len(child.ChildLinks) == 0:
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
        if len(self.ChildLinks) is 0:
            se.isleaf = True
        else:
            se.isleaf = False

        # Done setting up the Structure Element
        structure[se.key] = se
        # It does not matter if we are replacing an existing se with the same key - the content - including the child links must be identical!


        # This will be true for any object which is not a core object such as a commit
        # We don't want to worry about what is in the workspace - that is the repositories job.
        # if I am currently in the work space the commited version of me should be too!
        if repo._workspace.has_key(self.MyId):
            # Remove my old name
            del repo._workspace[self.MyId]

            # Now deal with some nastyness
            # Possible DAG structure created by hash conflict - two wrappers of the same type with the same value in one data structure
            if se.key in repo._workspace:
                # If this is true we have just done a lot of extra work to create the Structure element - but there is no way to know before creating it!

                # Get the other object with the same name that is already committed...
                other = repo._workspace[se.key]

                #print "Self Plinks:",self.ParentLinks
                #print "Other Plinks:",other.ParentLinks
                other.ParentLinks.update(self.ParentLinks)

                #print "Other Plinks After:",other.ParentLinks

                # Invalidate ourself
                self.Invalidate(other)

            else:
                # Now add it back the workspace under the new name
                repo._workspace[se.key] = self

        # Test to see if we need to set MyId - don't mess up the case where self now points to another source which is already commited.
        if self.MyId != se.key:
            self.MyId = se.key
            self.Modified = False

        # Set the key value for parent links!
        # This will only be reached once for a given child object. Set all parents
        # now and the child will return as unmodified when the other parents ask it
        # to recurse commit.

        #log.debug('Current Wrapper: %s' % self.Debug())

        #cnt = 1
        #for link in self.ParentLinks:
        #    log.debug('Parrent Link # %d \n%s' % (cnt, link.Debug()))
        #    cnt += 1


        for link in self.ParentLinks:
            if link.Invalid:
                log.error('Link in parent links is invalid!')
                log.debug('Current Wrapper: %s' % self.Debug())
                log.debug('Invalid Link %s' % link.Debug())


            if link.key != se.key:
                link.key = se.key

        log.debug('Exiting Recurse Commit: recurse counter - %d' % local_cnt)


    @GPBSource
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
                gpb_field = getattr(gpb, field.name)


                # If it is a repeated container type
                if isinstance(gpb_field, containers.RepeatedCompositeFieldContainer):
                    for item in gpb_field:
                        wrapped_item = self._rewrap(item)
                        if wrapped_item.ObjectType == LINK_TYPE:
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
                    if item.ObjectType == LINK_TYPE:
                        self.ChildLinks.add(item)
                    else:
                        item.FindChildLinks()

    @GPBSource
    def AddParentLink(self, link):
        #if self.Invalid:

        for parent in self.ParentLinks:
            if parent.GPBMessage is link.GPBMessage:
                break
        else:
            self.ParentLinks.add(link)

    @GPBSource
    def _rewrap(self, gpbMessage):
        '''
        Factory method to return a new instance of wrapper for a gpbMessage
        from self - used for access to composite structures, it has all the same
        shared variables as the parent wrapper
        '''

        # Check the root wrapper objects list of derived wrappers
        if gpbMessage in self.DerivedWrappers:
            return self.DerivedWrappers[gpbMessage]

        # Else make a new one...
        inst = Wrapper(gpbMessage)
        inst._root = self.Root
        inst._invalid=False

        # Add it to the list of objects which derive from the root wrapper
        self.DerivedWrappers[gpbMessage] = inst

        return inst

    @GPBSource
    def _set_parents_modified(self):
        """
        This method recursively changes an objects parents to a modified state
        All links are reset as they are no longer hashed values
        """

        if self.Modified:
            # Be clear about what we are doing here!
            # If it has already been modified we are done.
            return
        else:
            self.Modified = True

            # Get the repository
            repo = self.Repository

            new_id = repo.new_id()
            repo._workspace[new_id] = self.Root

            if repo._workspace.has_key(self.MyId):
                del repo._workspace[self.MyId]
            self.MyId = new_id

            # When you hit the commit ref - stop!
            if self.Root is repo._workspace_root:
                # The commit is no longer really your parent!
                self.ParentLinks = set()

            else:
                for link in self.ParentLinks:
                    # Tricky - set the message directly and call modified!
                    #link.GPBMessage.key = self.MyId
                    link.GPBMessage.key = self.MyId
                    #link._set_parents_modified()
                    link._set_parents_modified()

    @GPBSource
    def __eq__(self, other):
        if not isinstance(other, Wrapper):
            return False

        if self is other:
            return True

        return self.GPBMessage == other.GPBMessage

    @GPBSource
    def __ne__(self, other):
        # Can't just say self != other_msg, since that would infinitely recurse. :)
        return not self == other

    def __str__(self):
        """
        Since str is used in debugging it should never return an exception - no matter the state of the wrapper object
        """

        '''
        # Too complex - don't do things that might raise errors in str method!
        if self._gpb_type == LINK_TYPE:
            key = self._gpbMessage.key
            try:
                key = sha1_to_hex(self._gpbMessage.key)
            except struct.error, er:
                pass
            msg = '\nkey: %s \ntype { %s }' % (key, self._gpbMessage.type)
        else:
            msg = '\n' +self._gpbMessage.__str__()
        '''

        if not self.__no_string:
            return 'GPB NO STRING!'

        #log.critical('HOLY SHIT STILL HERE!')

        if self._source is self:
            msg = '\n %s \n' % str(self._gpbMessage)
        else:
            msg = '\n %s \n' % str(self._source._gpbMessage)

        return msg

    def Debug(self):
        """
        Since Debug is for debugging it should never return an exception - no matter the state of the wrapper object
        """
        output = '================== GPB Wrapper ====================\n'

        key = self._myid
        try:
            key = sha1_to_hex(key)
        except struct.error, er:
            pass

        output += 'Wrapper ID: %s \n' % key
        output += 'Wrapper repr: %s \n' % repr(self)
        output += 'Wrapper Invalid: %s \n' % self._invalid
        output += 'Wrapper ReadOnly: %s \n' % self._read_only
        output += 'Wrapper IsRoot: %s \n' % str(self._root is self)

        # This is dangerous - this can result in an exception loop!
        if hasattr(self._repository, '_dotgit') and not self._repository._dotgit.Invalid:
            output += 'Repository: %s \n' % str(self._repository.repository_key)
        else:
            output += 'Repository: %s \n' % str(self._repository)

        if self._root:
            output += 'Wrapper ParentLinks: %s \n' % str(self._parent_links)
            output += 'Wrapper ChildLinks: %s \n' % str(self._child_links)
            output += 'Wrapper Modified: %s \n' % self._root._modified

        output += 'Wrapper Type: %s \n' % str(self._gpb_type)
        # DO NOT UNCOMMENT THE FOLLOWING LINES unless you know what you are doing and you will recomment them!
        # They are more expensive than you can possibly imagine
        #output += 'Wrapper current value:\n'
        #output += str(self) + '\n'
        if self._source is not self:
            output += '================== Has Other source! =========================\n'
            output += self._source.Debug()
            output += '================== end source! =========================\n'
        else:
            output += 'Source is Self!\n'
        output += '================== Wrapper Complete =========================\n'
        return output

    @GPBSource
    def IsInitialized(self):
        """Checks if the message is initialized.

        Returns:
            The method returns True if the message is initialized (i.e. all of its
        required fields are set).
        """
        return self.GPBMessage.IsInitialized()

    @GPBSource
    def SerializeToString(self):
        """Serializes the protocol message to a binary string.

        Returns:
          A binary string representation of the message if all of the required
        fields in the message are set (i.e. the message is initialized).

        Raises:
          message.EncodeError if the message isn't initialized.
        """

        try:
            serialized = self.GPBMessage.SerializeToString()
        except message.EncodeError, ex:
            log.info(ex)
            raise OOIObjectError(
                'Could not serialize object - likely due to unset required field in a core object: %s' % str(self))

        return serialized

    @GPBSource
    def ParseFromString(self, serialized):
        """Clear the message and read from serialized."""

        # Do not use the GPBMessage method - it will recurse!
        self._gpbMessage.ParseFromString(serialized)

    @GPBSource
    def ListSetFields(self):
        """Returns a list of (FieldDescriptor, value) tuples for all
        fields in the message which are not empty.  A singular field is non-empty
        if IsFieldSet() would return true, and a repeated field is non-empty if
        it contains at least one element.  The fields are ordered by field
        number"""

        field_list = self.GPBMessage.ListFields()
        fnames = []
        for desc, val in field_list:
            fnames.append(desc.name)
        return fnames

    @GPBSource
    def IsFieldSet(self, field_name):
        GPBMessage = self.GPBMessage
        # Get the raw GPB field
        try:
            GPBField = getattr(GPBMessage, field_name)
        except AttributeError, ex:
            raise OOIObjectError('The "%s" object definition does not have a field named "%s"' %\
                                 (str(self.ObjectClass), field_name))

        if isinstance(GPBField, containers.RepeatedScalarFieldContainer):
            return len(GPBField) > 0

        elif isinstance(GPBField, containers.RepeatedCompositeFieldContainer):
            if len(GPBField) == 0:
                return False
            for item in GPBField:
                if len(item.ListFields()) > 0:
                    return True
            else:
                return False

        else:
            return GPBMessage.HasField(field_name)

    def HasField(self, field_name):
        log.warn('HasField is depricated because the name is confusing. Use IsFieldSet')
        return self.IsFieldSet(field_name)

    @GPBSource
    def ClearField(self, field_name):
        GPBMessage = self.GPBMessage

        #if not GPBMessage.IsFieldSet(field_name):
        #    # Nothing to clear
        #    return

        # Get the raw GPB field
        try:
            GPBField = getattr(GPBMessage, field_name)
        except AttributeError, ex:
            raise OOIObjectError('The "%s" object definition does not have a field named "%s"' %\
                                 (str(self.ObjectClass), field_name))

        if isinstance(GPBField, containers.RepeatedScalarFieldContainer):
            del self.DerivedWrappers[GPBField]
            # Nothing to do - just clear the field. It can not contain a link

        elif isinstance(GPBField, containers.RepeatedCompositeFieldContainer):
            for item in GPBField:
                wrapped_field = self._rewrap(item)
                wrapped_field._clear_derived_message()

                del self.DerivedWrappers[item]

            del self.DerivedWrappers[GPBField]

        elif isinstance(GPBField, message.Message):
            wrapped_field = self._rewrap(GPBField)
            wrapped_field._clear_derived_message()

            del self.DerivedWrappers[GPBField]

        # Nothing to be done for scalar fields - just clear it.

        #Now clear the field
        self.GPBMessage.ClearField(field_name)
        # Set this object and it parents to be modified
        self._set_parents_modified()

    @GPBSource
    def _clear_derived_message(self):
        """
        Helper method for ClearField
        """
        if self.ObjectType == LINK_TYPE:
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

    @GPBSource
    def ByteSize(self):
        """Returns the serialized size of this message.
        Recursively calls ByteSize() on all contained messages.
        """
        return self.GPBMessage.ByteSize()

    @GPBSource
    def PPrint(self, offset=''):

        fid = StringIO.StringIO()

        fid.write('%s%s\n' % (offset, self.ObjectClass))

        try:
            for name, field in self._Properties.iteritems():
                try:
                    field_val = field.__get__(self)
                except KeyError, ke:
                    log.debug('KeyError during get field: %s' % ke)

                    fid.write('''%s{Field Name - "%s" : Field Type - %s : %s} \n''' % (
                    offset, name, field.field_type, 'KeyError - object not found in local workbench'))
                    continue

                if isinstance(field, WrappedMessageProperty):
                    try:
                        val = 'Field Value - \n%s \n%s' % (field_val.PPrint(offset=offset + '  '), offset)
                    except AttributeError, ae:
                        log.debug('Unset CasRef Field Name: %s: Catching Attribute Error: %s ' % (name, ae))
                        val = 'Field Value - None'
                    except Exception, ex:
                        log.exception('Unexpected state in a WrappedMessageProperty.')
                        fid.write('%s Exception while printing field name - %s, Exception - %s\n' % (offset, name, ex))
                        continue

                elif isinstance(field, WrappedRepeatedCompositeProperty):
                    try:
                        val = 'Field Value - \n%s \n%s' % (field_val.PPrint(offset=offset + '  ', name=name), offset)
                    except Exception, ex:
                        log.exception('Unexpected state in a WrappedRepeatedCompositeProperty.')
                        fid.write('%s Exception while printing field name - %s, Exception - %s\n' % (offset, name, ex))
                        continue

                elif isinstance(field, WrappedRepeatedScalarProperty):
                    scalars = field_val
                    if len(scalars) > 20:
                        val = '# of Values - %i: Field Values - "%s"... truncated at 20 values!' % (
                        len(scalars), str(scalars[:20]))
                    else:
                        val = '# of Values - %i: Field Values - "%s"' % (len(scalars), str(scalars[:]))

                else:
                    item = field_val
                    if field.field_type == 'TYPE_ENUM':
                        item = field.field_enum.lookup.get(item, 'Invalid Enum Value!')

                    val = 'Field Value - "%s"' % str(item)

                fid.write('''%s{Field Name - "%s" : Field Type - %s : %s} \n''' % (offset, name, field.field_type, val))
        except Exception, ex:
            log.exception('Unexpected Exception in PPrint Wrapper!')
            fid.write('Exception! %s' % ex)

        finally:
            msg = fid.getvalue()
            fid.close()

        return msg


class ContainerWrapper(object):
    """
    This class is only for use with containers.RepeatedCompositeFieldContainer
    It is not needed for repeated scalars!
    """

    def __init__(self, wrapper, gpbcontainer):
        # Be careful - this is a hard link
        self._wrapper = wrapper
        if not isinstance(gpbcontainer, containers.RepeatedCompositeFieldContainer):
            raise OOIObjectError('The Container Wrapper is only for use with Repeated Composite Field Containers')
        self._gpbcontainer = gpbcontainer
        self.Repository = wrapper.Repository
        self._source = self

    def GPBSourceCW(func):
        def call_func(self, *args, **kwargs):
            func_name = func.__name__
            '''
            print 'GPB INVALID CW'
            print 'func name', func_name, func
            print 'args', args
            print 'kwargs', kwargs
            '''
            wrapper = self._wrapper._source
            if wrapper._invalid:
                log.error(wrapper.Debug())
                raise OOIObjectError('Can not access Invalidated Container Wrapper Object in function "%s"' % func_name)

            source = self._source

            return func(source, *args, **kwargs)

        return call_func


    @classmethod
    def factory(cls, wrapper, gpbcontainer):
        #print cls, type(wrapper), type(gpbcontainer)
        #objhash = hash(gpbcontainer)
        dw = wrapper.DerivedWrappers
        if gpbcontainer in dw:
            return dw[gpbcontainer]

        inst = cls(wrapper, gpbcontainer)

        # Add it to the list of objects which derive from the root wrapper
        dw[gpbcontainer] = inst
        return inst


    @property
    def Root(self):
        return self._wrapper._root


    @property
    def Invalid(self):
        return self.Root.Invalid

    def Invalidate(self, source=None):
        self._gpbcontainer = None
        if source is not None:
            self._source = source

    @GPBSourceCW
    def __setitem__(self, key, value):
        """
        Sets the item in the specified position.
        """

        if not isinstance(value, Wrapper):
            raise OOIObjectError('To set an item in a repeated field container, the value must be a Wrapper')

        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
        if item.ObjectType == LINK_TYPE:
            self.Repository.set_linked_object(item, value)
        else:
            raise OOIObjectError(
                'It is illegal to set a value of a repeated composite field unless it is a CASRef - Link')

        self._wrapper._set_parents_modified()


    @GPBSourceCW
    def SetLink(self, key, value, ignore_copy_errors=False):
        if not isinstance(value, Wrapper):
            raise OOIObjectError('To set an item in a repeated field container, the value must be a Wrapper')

        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
        if item.ObjectType == LINK_TYPE:
            self.Repository.set_linked_object(item, value, ignore_copy_errors)
        else:
            raise OOIObjectError(
                'It is illegal to set a value of a repeated composit field unless it is a CASRef - Link')

        self._wrapper._set_parents_modified()

    @GPBSourceCW
    def __getitem__(self, key):
        """Retrieves item by the specified key."""

        value = self._gpbcontainer.__getitem__(key)
        value = self._wrapper._rewrap(value)
        if value.ObjectType == LINK_TYPE:
            value = self.Repository.get_linked_object(value)
        return value

    @GPBSourceCW
    def GetLink(self, key):
        link = self._gpbcontainer.__getitem__(key)
        link = self._wrapper._rewrap(link)
        assert link.ObjectType == LINK_TYPE, 'The field "%s" is not a link!' % link.ObjectClass
        return link

    @GPBSourceCW
    def GetLinks(self):
        wrapper_list = []
        links = self._gpbcontainer[:] # Get all the links!
        for link in links:
            link = self._wrapper._rewrap(link)
            assert link.ObjectType == LINK_TYPE, 'The field "%s" is not a link!' % link.ObjectClass
            wrapper_list.append(link)
        return wrapper_list

    @GPBSourceCW
    def __len__(self):
        """Returns the number of elements in the container."""

        return self._gpbcontainer.__len__()

    @GPBSourceCW
    def __ne__(self, other):
        """Checks if another instance isn't equal to this one."""

        return not self._gpbcontainer == other._gpbcontainer

    @GPBSourceCW
    def __eq__(self, other):
        """Compares the current instance with another one."""

        if self is other:
            return True

        if not isinstance(other, self.__class__):
            raise OOIObjectError('Can only compare repeated composite fields against other repeated composite fields.')
        return self._gpbcontainer == other._gpbcontainer

    @GPBSourceCW
    def __str__(self):
        """Need to improve this!"""

        return self._gpbcontainer.__str__()


    # Composite specific methods:
    @GPBSourceCW
    def add(self):
        new_element = self._gpbcontainer.add()

        self._wrapper._set_parents_modified()
        return self._wrapper._rewrap(new_element)

    @GPBSourceCW
    def __getslice__(self, start, stop):
        """Retrieves the subset of items from between the specified indices."""

        wrapper_list = []
        for index in range(0, len(self))[start:stop]:
            wrapper_list.append(self.__getitem__(index))

        # Does it make sense to return a list?
        return wrapper_list

    @GPBSourceCW
    def __delitem__(self, key):
        """Deletes the item at the specified position."""

        self._wrapper._set_parents_modified()

        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)

        item._clear_derived_message()

        self._gpbcontainer.__delitem__(key)

    @GPBSourceCW
    def __delslice__(self, start, stop):
        """Deletes the subset of items from between the specified indices."""

        i_range = range(0, len(self))[start:stop]
        for index in reversed(i_range):
            self.__delitem__(index)


    @GPBSourceCW
    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    @GPBSourceCW
    def PPrint(self, offset='', name='items'):

        length = len(self)

        if length is 0:
            return '%s[ ]' % offset
        else:
            fid = StringIO.StringIO()        
            fid.write('%s[ # of %s - %i \n' % (offset, name, length))


        try:

                #n = min(10, length)

            # Print all repeated composites
            for i in range(length):
                #log.debug('Printing field # %d' % i)
                try:
                    val = self[i].PPrint(offset=offset + '  ')
                    fid.write('''%s%s# %i - %s  \n''' % (offset, name, i, val))

                except AttributeError, ae:
                    log.debug('Attribute error while calling pprint on repeated composite: %s' % ae)
                    fid.write('''%s%s# %i - %s  \n''' % (offset, name, i, 'Repeated Link Not Set!'))

                except KeyError, ke:
                    log.debug('KeyError while calling pprint on repeated composite: %s' % ke)
                    fid.write('''%s%s# %i - %s  \n''' % (offset, name, i, 'Repeated Link object not found!!'))

                except Exception, ex:
                    log.exception('Unexpected Exception while calling pprint on repeated composite')
                    fid.write('''%s%s# %i - Exception: %s  \n''' % (offset, name, i, ex))


                    #if length > 10:
            #    msg += offset + '... truncated printing list at 10 items!\n'


            fid.write(offset + '] ')

        except Exception, ex:
            log.exception('Unexpected Exception in PPrint CompositeContainer!')
            fid.write('Exception! %s' % ex)

        finally:
            msg = fid.getvalue()
            fid.close()

        return msg


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
        #objhash = hash(gpbcontainer)
        dw = wrapper.DerivedWrappers
        if gpbcontainer in dw:
            return dw[gpbcontainer]

        inst = cls(wrapper, gpbcontainer)

        # Add it to the list of objects which derive from the root wrapper
        dw[gpbcontainer] = inst
        return inst


    def GPBSourceSCW(func):
        def call_func(self, *args, **kwargs):
            func_name = func.__name__
            '''
            print 'GPB INVALID CW'
            print 'func name', func_name, func
            print 'args', args
            print 'kwargs', kwargs
            '''
            wrapper = self._wrapper._source
            if wrapper._invalid:
                log.error(wrapper.Debug())
                raise OOIObjectError(
                    'Can not access Invalidated Scalar Container Wrapper Object in function "%s"' % func_name)

            return func(self, *args, **kwargs)

        return call_func

    @property
    def Root(self):
        return self._wrapper._root

    @property
    def Invalid(self):
        return self.Root.Invalid

    def Invalidate(self, source=None):
        self._gpbcontainer = None
        if source is not None:
            self._source = source

    @GPBSourceSCW
    def append(self, value):
        """Appends an item to the list. Similar to list.append()."""

        self._gpbcontainer.append(value)
        self._wrapper._set_parents_modified()

    @GPBSourceSCW
    def insert(self, key, value):
        """Inserts the item at the specified position. Similar to list.insert()."""

        self._gpbcontainer.insert(key, value)
        self._wrapper._set_parents_modified()

    @GPBSourceSCW
    def extend(self, elem_seq):
        """Extends by appending the given sequence. Similar to list.extend()."""

        self._gpbcontainer.extend(elem_seq)
        self._wrapper._set_parents_modified()

    @GPBSourceSCW
    def remove(self, elem):
        """Removes an item from the list. Similar to list.remove()."""

        self._gpbcontainer.remove(elem)
        self._wrapper._set_parents_modified()


    @GPBSourceSCW
    def __getslice__(self, start, stop):
        """Retrieves the subset of items from between the specified indices."""

        return self._gpbcontainer._values[start:stop]


    @GPBSourceSCW
    def __len__(self):
        """Returns the number of elements in the container."""

        return len(self._gpbcontainer._values)


    @GPBSourceSCW
    def __getitem__(self, key):
        """Retrieves the subset of items from between the specified indices."""

        return self._gpbcontainer._values[key]


    @GPBSourceSCW
    def __setitem__(self, key, value):
        """Sets the item on the specified position."""
        self._gpbcontainer.__setitem__(key, value)
        self._wrapper._set_parents_modified()

    @GPBSourceSCW
    def __setslice__(self, start, stop, values):
        """Sets the subset of items from between the specified indices."""

        self._gpbcontainer.__setslice__(start, stop, values)
        self._wrapper._set_parents_modified()

    @GPBSourceSCW
    def __delitem__(self, key):
        """Deletes the item at the specified position."""

        del self._gpbcontainer._values[key]
        self._gpbcontainer._message_listener.Modified()
        self._wrapper._set_parents_modified()

    @GPBSourceSCW
    def __delslice__(self, start, stop):
        """Deletes the subset of items from between the specified indices."""

        self._gpbcontainer._values.__delslice__(start, stop)
        self._gpbcontainer._message_listener.Modified()
        self._wrapper._set_parents_modified()

    @GPBSourceSCW
    def __eq__(self, other):
        """Compares the current instance with another one."""

        if self is other:
            return True
            # Special case for the same type which should be common and fast.
        if isinstance(other, self.__class__):
            return other._gpbcontainer._values == self._gpbcontainer._values
            # We are presumably comparing against some other sequence type.
        return other == self._gpbcontainer._values

    @GPBSourceSCW
    def __ne__(self, other):
        """Checks if another instance isn't equal to this one."""
        # The concrete classes should define __eq__.
        return not self == other

    @GPBSourceSCW
    def __str__(self):
        return str(self._gpbcontainer._values)

    @GPBSourceSCW
    def __iter__(self):
        for i in range(len(self)):
            yield self[i]


class StructureElementError(Exception):
    """
    An error class for structure elements
    """


class StructureElement(object):
    """
    @brief Wrapper for the container structure element. These are the objects
    stored in the hashed elements table. Mostly convience methods are provided
    here. A set provides references to the child objects so that the content
    need not be decoded to find them.
    """

    def __init__(self, se=None):
        if se:
            self._element = se
        else:
            self._element = get_gpb_class_from_type_id(STRUCTURE_ELEMENT_TYPE)()
        self.ChildLinks = set()

    @classmethod
    def parse_structure_element(cls, blob):
        se = get_gpb_class_from_type_id(STRUCTURE_ELEMENT_TYPE)()
        se.ParseFromString(blob)

        instance = cls(se)

        if instance.key != instance.sha1:
            log.error('The sha1 key does not match the value. The data is corrupted! \n' +\
                      'Element key %s, Calculated key %s' % (sha1_to_hex(instance.key), sha1_to_hex(instance.sha1)))
            raise StructureElementError('Error reading serialized structure element. Sha1 value does not match.')

        return instance

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
    def _set_type(self, obj_type):
        self._element.type.object_id = obj_type.object_id
        self._element.type.version = obj_type.version

    type = property(_get_type, _set_type)

    #@property
    def _get_value(self):
        return self._element.value

    #@value.setter
    def _set_value(self, value):
        self._element.value = value

    value = property(_get_value, _set_value)

    #@property
    def _get_key(self):
        #return sha1_to_hex(self._element.key)
        return self._element.key

    #@key.setter
    def _set_key(self, value):
        self._element.key = value

    key = property(_get_key, _set_key)

    def _set_isleaf(self, value):
        self._element.isleaf = value

    def _get_isleaf(self):
        return self._element.isleaf

    isleaf = property(_get_isleaf, _set_isleaf)

    def __str__(self):
        msg = ''
        if len(self._element.key) == 20:
            msg =   'Key:    ' + sha1_to_hex(self._element.key) + '\n'
        msg = msg + 'Type:   ' + str(self._element.type) + '\n'
        msg = msg + 'IsLeaf: ' + str(self._element.isleaf) + '\n'
        msg = msg + 'El Len: ' + str(self.__sizeof__())
        return msg

    def serialize(self):
        return self._element.SerializeToString()


    def __sizeof__(self):
        #size = len(self._element.value) + 34
        #print 'Esimtate: ', size
        #print 'GPB Size: ', self._element.ByteSize()

        return self._element.ByteSize()
