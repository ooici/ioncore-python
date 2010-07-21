"""
@file ion/services/coi/hostsensor/encoders.py
@author Brian Fox
@brief (En)decodes complex python objects for safe XMLRPC transmission
"""

import re

def encodeJSONToXMLRPC(object):
    """
    Encodes a JSON-like object made up of list, dicts, ints, and so
    on to a compatible XMLRPC representation.  For now, this means
    converting larger than 32-bit signed integers into a string
    representation.
    
    See http://docs.python.org/library/json.html Section 18.2.2
    for the details of a "JSON-like object"
    
    """
    if hasattr(object, '__iter__'):
        
        # recurse through iterables
        
        copy = type(object)()

        if isinstance(object,dict):
            for key in object:
                copy[key] = encodeJSONToXMLRPC(object[key])
            return copy

        elif isinstance(object,list):
            for next in object:
                copy.append(encodeJSONToXMLRPC(next))
            return copy

        elif isinstance(object,tuple):
            l = list(object)
            copy = []
            for next in l:
                copy.append(encodeJSONToXMLRPC(next))
            return tuple(copy)
        
        else:
            raise Exception(
                            'Cannot encode object:'
                            ' unsupported iterable %s = %s'
                            %(type(object), str(object))
                            )
    else:
        if not isinstance(object,unicode)       \
            and not isinstance(object, str)     \
            and not isinstance(object, int)     \
            and not isinstance(object, long)    \
            and not isinstance(object, float)   \
            and object != True                  \
            and object != False                 \
            and object != None:
            raise Exception(
                            'Cannot encode object:'
                            ' unsupported type %s = %s'
                            %(type(object), str(object))
                            )
            
        # secret sauce - we want to turn large ints to strings    
        if isinstance(object,long) or isinstance(object,int):
            if object > 2**31 - 1:
                return 'long:' + hex(object)
        
        return object


    
def decodeXMLRPCToJSON(object):
    """
    Inverse of encodeJSONToXMLRPC
    """
    if hasattr(object, '__iter__'):
        
        # recurse through iterables
        
        copy = type(object)()

        if isinstance(object,dict):
            for key in object:
                copy[key] = decodeXMLRPCToJSON(object[key])
            return copy

        elif isinstance(object,list):
            for next in object:
                copy.append(decodeXMLRPCToJSON(next))
            return copy

        elif isinstance(object,tuple):
            l = list(object)
            copy = []
            for next in l:
                copy.append(decodeXMLRPCToJSON(next))
            return tuple(copy)
        
        else:
            raise Exception(
                            'Cannot decode object:'
                            ' unsupported iterable %s = %s'
                            %(type(object), str(object))
                            )
    else:
        if not isinstance(object,unicode)       \
            and not isinstance(object, str)     \
            and not isinstance(object, int)     \
            and not isinstance(object, long)    \
            and not isinstance(object, float)   \
            and object != True                  \
            and object != False                 \
            and object != None:
            raise Exception(
                            'Cannot encode object:'
                            ' unsupported type %s = %s'
                            %(type(object), str(object))
                            )
            
        # secret sauce - we want to turn large ints to strings    
        if isinstance(object, str) or isinstance(object, unicode):
            if re.match("^long\:0x[0-9A-Fa-f]+", object):
                return int(object[5:], 16)
        return object            



