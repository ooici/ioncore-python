#!/usr/bin/env python
"""
@file ion/core/object/object_utils.py
@brief Tools and utilities for object management 
@author David Stuebe
"""

from ion.util import procutils as pu

from net.ooici.core.type import type_pb2

import hashlib
from google.protobuf import message

# Globals
gpb_id_to_class = {}

class ObjectUtilException(Exception):
    """ Exceptions specific to Object Utilities. """
    pass

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
    """
    Move to object utilities module
    
    Operates on instances and classes of gpb messages!
    """
    gpbtype = type_pb2.GPBType()
    
    # Take just the file name
    gpbtype.protofile = obj.DESCRIPTOR.file.name.split('/')[-1]
    # Get rid of the .proto
    gpbtype.protofile = gpbtype.protofile.split('.')[0]

    gpbtype.package = obj.DESCRIPTOR.file.package
    gpbtype.cls = obj.DESCRIPTOR.name
    
    return gpbtype



def create_type_identifier(package='', protofile='', cls=''):
    """
    This returns an unwrapped GPB object to the application level
    """        
    gpbtype = type_pb2.GPBType()
        
    gpbtype.protofile = protofile
    gpbtype.package = package
    gpbtype.cls = cls
        
    return gpbtype

def build_gpb_lookup(rootpath):
    """
    To be called once on package initialization.
    @param rootpath The full path of the package to import the Protocol Buffers classes from.
    """

    ENUM_NAME = '_MessageTypeIdentifier'
    ENUM_ID_NAME = '_ID'

    global gpb_id_to_class
    gpb_id_to_class = {}

    root = __import__(rootpath)

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
                                    gpb_id_to_class[val.number] = msg_class

def get_gpb_class_from_id(id):
    """
    Get a callable google.protobuf.message.Message subclass with the given MessageTypeIdentifier enum id.
    @param id The integer id.
    @retval msg_class The class for the given id.
    @throws ObjectUtilException
    """
    try:
        id = int(id)
        return gpb_id_to_class[id]
    except ValueError, ex:
        raise ObjectUtilException('Protocol Buffer Message ids must be integers: "%s"' % (str(id)))
    except KeyError, ex:
        raise ObjectUtilException('No Protocol Buffer Message class found for id "%d"' % (id))

# Build the lookup table on first import
build_gpb_lookup('net')
