#!/usr/bin/env python
"""
@file ion/data/resource.py
@author David Stuebe
@author Dorian Raymer
@brief Resource object which can be encoded, decoded and stored!

@todo 
"""

import re
import hashlib
import struct
import logging

from zope.interface import Interface
from zope.interface import implements
from zope.interface import Attribute 

from twisted.internet import defer

from ion.data.datastore import cas


NULL_CHR = "\x00"


class RdfReference(str):
    """
    @NOTE This is a temperoray addition to play with making a reference
    """
    def __init__(self,value):
        str.__init__(value)

class TypedAttribute(object):

    def __init__(self, type, default=None):
        self.name = None
        self.type = type
        self.default = default if default else type()
        self.cache = None

    def __get__(self, inst, cls):
        #inst.getEvent(self.name)
        value = getattr(inst, self.name, self.default)
        return value

    def __set__(self, inst, value):
        #inst.setEvent(self.name, value)
        if not isinstance(value, self.type):
            raise TypeError("Must be a %s" % self.type)
        setattr(inst, self.name, value)

    @classmethod
    def decode(cls, value):
        stype, default = value.split(NULL_CHR)
        
        #print 'default',default
        type = eval(str(stype))
#        return cls(type, type(str(default)))

        if type == list:
            return cls(type, eval(str(default)))
        else:
           return cls(type, type(str(default)))

class DataObject(type):

    def __new__(cls, name, bases, dict):
        slots = []
        for key, value in dict.items():
            if isinstance(value, TypedAttribute):
                value.name = '_' + key
                slots.append(value.name)
        dict['__slots__'] = slots
        return type.__new__(cls, name, bases, dict)

class BaseResource(object):
    """
    """
    __metaclass__ = DataObject

    def __init__(self, **kwargs):
        """
        """
        for key, value in kwargs.items():
            setattr(self, key, value)


    def setEvent(self, name, value):
        """
        """

    def getEvent(self, name):
        """
        """

    def __eq__(self,other):
        
        if not isinstance(other, BaseResource):
            return False
        
        for name in self.attributes:
            self_value = getattr(self,name)
            
            other_value = getattr(other,name)
            if other_value != self_value:
                return False
        return True

    def __str__(self):
        head = '='*10
        strng  = """\n%s Resource Type: %s %s\n""" % (head, str(self.__class__.__name__), head)
        for name in self.attributes:
            value = getattr(self,name)
            strng += """= '%s':'%s'\n""" % (name,value)
        strng += head*2
        return strng

    @property
    def attributes(self):
        names = []
        for key in self.__slots__:
            names.append(key[1:])
        return names


    def encode(self):
        """
        """
        encoded = []
        for name in self.attributes:
            value = getattr(self, name)
            
            # Attempt to handle nested Resources
            if not isinstance(value, BaseResource):
                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, str(value),)))
            else:
                value = value.encode()
                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, str(value),)))
        return encoded

    @classmethod
    def decode(cls, baseType, attrs):
        """
        decode resource object[s]
        """
        #d = dict([(str(name), TypedAttribute.decode(value)) for name, value in attrs])
        d={}
        for name, value in attrs:
            #print 'name',name
            #print 'value',value
            d[str(name)] = TypedAttribute.decode(value)       
        return type(str(baseType), (cls,), d)


class IdentityResource(BaseResource):

    name = TypedAttribute(str)
    email = TypedAttribute(str)

class DeviceResource(BaseResource):

    mfg = TypedAttribute(str)
    serial = TypedAttribute(int)
    voltage = TypedAttribute(float)






