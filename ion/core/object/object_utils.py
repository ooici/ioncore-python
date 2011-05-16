#!/usr/bin/env python
"""
@file ion/core/object/object_utils.py
@brief Tools and utilities for object management 
@author David Stuebe
"""

from ion.util.cache import memoize

from net.ooici.core.type import type_pb2

import hashlib
import struct
import os
from google.protobuf import message
from google.protobuf.internal import containers

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# Globals
gpb_id_to_class = {}

class ObjectUtilException(Exception):
    """ Exceptions specific to Object Utilities. """
    pass

class OOIObjectError(Exception):
    """
    An exception class for errors that occur in the Object Wrapper class
    """


def __eq__gpbtype(self, other):
    ''' Improve performance on GPBType comparisons enormously. '''
    if self is other: return True
    if self is None or other is None: return False
    return self.object_id == other.object_id and self.version == other.version
setattr(type_pb2.GPBType, '__eq__', __eq__gpbtype)

def __hash_by_id(self):
    return id(self)
    #return hash(repr(self))
    
setattr(containers.BaseContainer, '__hash__', __hash_by_id)
setattr(message.Message, '__hash__', __hash_by_id)
#setattr(containers.RepeatedCompositeFieldContainer, '__hash__', __hash_by_id)
#setattr(containers.RepeatedScalarFieldContainer, '__hash__', __hash_by_id)

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
    return ''.join([y[-2:] for y in [x.replace('x', '0') for x in almosthex]]).upper()

@memoize(0)
def get_enum_from_descriptor(descriptor):
    """
    @TODO Needs cleaning up - should be more robust + get the version
    """

    ENUM_NAME = '_MessageTypeIdentifier'
    ENUM_ID_NAME = '_ID'

    if hasattr(descriptor, 'enum_types'):
        for enum_type in descriptor.enum_types:
            if enum_type.name == ENUM_NAME:
                for val in enum_type.values:
                    if val.name == ENUM_ID_NAME:
                        return val

    raise ObjectUtilException(\
        '''This object has no Message Type Identifier enum: %s'''\
        % (str(descriptor.name)))

@memoize(0)
def get_type_from_descriptor(descriptor):
    """    
    Operates on instances and classes of gpb messages!
    @TODO Needs cleaning up - should be more robust + get the version 
    """

    val = get_enum_from_descriptor(descriptor)
    obj_type = create_type_identifier(val.number, 1)
    return obj_type
                    
def get_type_from_obj(obj):
    return get_type_from_descriptor(obj.DESCRIPTOR)

def set_type_from_obj(obj, type):
    if isinstance(type, message.Message):
        gpb_type = type
    else:
        gpb_type = object.__getattribute__(type, 'GPBMessage')

    new_type = get_type_from_obj(obj)
    gpb_type.CopyFrom(new_type)

@memoize(0)
def create_type_identifier(object_id='', version=1):
    """
    This returns an unwrapped GPB object to the application level
    """        

    # Temporary restriction to version == 1. Temporary sanity check!
    if version != 1:
        msg = '''Protocol Buffer Object VERSION in the MessageTypeIdentifier should be 1. \n'''
        msg += '''Explicit versioning is not yet supported.\n'''
        msg +='''Arguments to create_type_identifier: object_id - "%s"; version - "%s"'''\
            % (str(object_id), str(version))
        raise ObjectUtilException(msg)

    try:
        #ObjectType = type_pb2.GPBType(int(object_id), int(version))
        ObjectType = type_pb2.GPBType()
        ObjectType.object_id = int(object_id)
        ObjectType.version = int(version)
    except ValueError, ex:
        raise ObjectUtilException(\
            '''Protocol Buffer Object IDs must be integers:object_id - "%s"; version - "%s"'''\
            % (str(object_id), str(version)))
        
    return ObjectType

def build_gpb_lookup(rootpath):
    """
    To be called once on package initialization.
    The given package must include a list named "protos" specifying which protocol buffer files to import.
    @param rootpath The full path of the package to import the Protocol Buffers classes from.
    """

    ENUM_NAME = '_MessageTypeIdentifier'
    ENUM_ID_NAME = '_ID'
    ENUM_VERSION_NAME = '_VERSION'
    
    global gpb_id_to_class
    gpb_id_to_class = {}

    root = __import__(rootpath)
    protos = root.protos
    for proto in protos:
        protopath = '%s.%s' % (rootpath, proto)
        m = __import__(protopath)

    msg_classes = message.Message.__subclasses__()
    for msg_class in msg_classes:
        if msg_class.__module__.startswith(rootpath):
            if hasattr(msg_class, 'DESCRIPTOR'):
                descriptor = msg_class.DESCRIPTOR
                if hasattr(descriptor, 'enum_types'):
                    for enum_type in descriptor.enum_types:
                        if enum_type.name == ENUM_NAME:
                            for val in enum_type.values:
                                if val.name == ENUM_ID_NAME:
                                    if gpb_id_to_class.has_key(val.number):
                                        old_def = str(gpb_id_to_class[val.number].__module__)
                                        new_def = str(msg_class.__module__)
                                        gpb_num = str(val.number)
                                        raise ObjectUtilException('Duplicate _MessageTypeIdentifier for '\
                                                                      + 'ID# %s in %s; original definition in %s' \
                                                                      % (gpb_num, new_def, old_def))

                                    gpb_id_to_class[val.number] = msg_class
                                elif val.name == ENUM_VERSION_NAME:
                                    # Eventually this will implement versioning...
                                    # For now return an error if the version is not 1
                                    if val.number != 1:
                                        msg = '''Protocol Buffer Object VERSION in the MessageTypeIdentifier should be 1. \n'''
                                        msg += '''Explicit versioning is not yet supported.\n'''
                                        msg +='''Invalid Object Class: "%s"'''\
                                            % (str(msg_class.__name__))
                                        raise ObjectUtilException(msg)

def get_gpb_class_from_type_id(typeid):
    """
    Get a callable google.protobuf.message.Message subclass with the given MessageTypeIdentifier enum id.
    @param id The type id object
    @retval msg_class The class for the given id.
    @throws ObjectUtilException
    """
    try:
        if isinstance(typeid, int):
            return gpb_id_to_class[typeid]
        else:
            return gpb_id_to_class[typeid.object_id]
    except AttributeError, ex:
        raise ObjectUtilException('The type argument is not a valid type identifier object: "%s, type: %s "' % (str(typeid), type(typeid)))
    except KeyError, ex:
        raise ObjectUtilException('No Protocol Buffer Message class found for id "%s"' % (str(typeid)))

type_name_cache = None
def find_type_ids(query):
    """
    Fuzzy search for a GPB-defined type with the given text in the name.
    Assumes that build_gpb_lookup() was called on package init.
    """
    import difflib

    global type_name_cache
    if type_name_cache is None:
        type_name_cache = dict((cls.__name__.lower(), cls) for cls in gpb_id_to_class.itervalues())

    matches = difflib.get_close_matches(query.lower(), type_name_cache.iterkeys(), cutoff=0.5)
    clses = (type_name_cache[match] for match in matches)
    return dict((cls._ID, cls) for cls in clses)

def return_proto_file(typeid):
    """
    Similar to open_proto, but instead return the contents of the proto file as a
    simple string.
    """
    gpb_root = os.path.join('..', 'ion-object-definitions', 'net')
    cls = get_gpb_class_from_type_id(typeid)
    proto_pieces = cls.__module__.split('.')[1:]
    filename = proto_pieces.pop()
    proto = '%s.proto' % (filename.replace('_pb2', ''))
    proto_dir = os.sep.join(proto_pieces)
    proto_path = os.path.join(gpb_root, proto_dir, proto)
    if os.path.exists(proto_path):
        return open(proto_path).read()
    else:
        py_dir = __import__(cls.__module__).__path__[0]
        py_file = '%s.py' % (filename)
        py_path = os.path.join(py_dir, proto_dir, py_file)
        return open(py_path).read()
    
def open_proto(typeid):
    """ Developer utility to open either the .proto if found, or the generated .py, for a GPB typeid. """

    def launch(path):
        import platform

        if platform.system() == 'Windows':
            os.system('notepad %s' % path)
        else:
            os.system('open -t %s' % path)


    gpb_root = os.path.join('..', 'ion-object-definitions', 'net')
    cls = get_gpb_class_from_type_id(typeid)
    proto_pieces = cls.__module__.split('.')[1:]
    filename = proto_pieces.pop()
    proto = '%s.proto' % (filename.replace('_pb2', ''))
    proto_dir = os.sep.join(proto_pieces)
    proto_path = os.path.join(gpb_root, proto_dir, proto)
    if os.path.exists(proto_path):
        launch(proto_path)
    else:
        py_dir = __import__(cls.__module__).__path__[0]
        py_file = '%s.py' % (filename)
        py_path = os.path.join(py_dir, proto_dir, py_file)
        launch(py_path)



def _gpb_source(func):

    def call_func(self, *args, **kwargs):

        func_name = func.__name__
        '''
        print 'GPB SOURCE'
        print 'func name', func_name, func
        print 'args', args
        print 'kwargs', kwargs
        '''
        source = self._source
        if source._invalid:
            log.error(source.Debug())
            raise OOIObjectError('Can not access Invalidated Object in function "%s"' % func_name)

        return func(source, *args, **kwargs)

    return call_func

def _gpb_source_root(func):

        def call_func(self, *args, **kwargs):

            func_name = func.__name__

            '''
            print 'GPB SOURCE ROOT'

            print 'func name', func_name, func
            print 'args', args
            print 'kwargs', kwargs
            '''
            source = self._source
            if source._invalid:
                log.error(source.Debug())
                raise OOIObjectError('Can not access Invalidated Object in function "%s"' % func_name)

            source_root = source._root

            return func(source_root, *args, **kwargs)


        return call_func




# Build the lookup table on first import
build_gpb_lookup('net')

# Build the CDM TYPES for import 
CDM_GROUP_TYPE = create_type_identifier(object_id=10020, version=1)
CDM_DATASET_TYPE = create_type_identifier(object_id=10001, version=1)
CDM_VARIABLE_TYPE = create_type_identifier(object_id=10024, version=1)
CDM_DIMENSION_TYPE = create_type_identifier(object_id=10018, version=1)
CDM_ATTRIBUTE_TYPE = create_type_identifier(object_id=10017, version=1)
ARRAY_STRUCTURE_TYPE = create_type_identifier(object_id=10025, version=1)
CDM_ARRAY_INT32_TYPE = create_type_identifier(object_id=10009, version=1)
CDM_ARRAY_UINT32_TYPE = create_type_identifier(object_id=10010, version=1)
CDM_ARRAY_INT64_TYPE = create_type_identifier(object_id=10011, version=1)
CDM_ARRAY_UINT64_TYPE = create_type_identifier(object_id=10012, version=1)
CDM_ARRAY_FLOAT32_TYPE = create_type_identifier(object_id=10013, version=1)
CDM_ARRAY_FLOAT64_TYPE = create_type_identifier(object_id=10014, version=1)
CDM_ARRAY_STRING_TYPE = create_type_identifier(object_id=10015, version=1)
CDM_ARRAY_OPAQUE_TYPE = create_type_identifier(object_id=10016, version=1)