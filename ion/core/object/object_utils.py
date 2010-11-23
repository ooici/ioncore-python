#!/usr/bin/env python
"""
@file ion/core/object/object_utils.py
@Brief Tools and utilities for object management 
@author David Stuebe
"""

from ion.util import procutils as pu

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