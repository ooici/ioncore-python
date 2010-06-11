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

import datetime
import time
import os
import uuid

try:
    import json
except:
    import simplejson as json

sha1hex = cas.sha1hex
sha1bin = cas.sha1bin
sha1_to_hex = cas.sha1_to_hex

def sha1(val, bin=True):
    if isinstance(val, ResourceObject):
        val = val.value
    if bin:
        return sha1bin(val)
    return sha1hex(val)

NULL_CHR = "\x00"


types={}

class IResourceObject(Interface):
    """
    Interface for objects stored in CAStore.
    """

    type = Attribute("""@param type Type of Resource object. This should be
            set as a class attribute for each type implementation class""")

    def value():
        """
        @brief Bytes to write into store.
        """

    def encode():
        """
        @brief Full encoding (header + body) of storable content. This
        computes the header portion, and prepends that to the body.
        @retval Storable, hashable value representing an object.
        """

    def decode(value, types):
        """
        @brief Decode value into an instance of a StoreObject class
        """
        
    def __str__():
        """
        @brief Pretty print object as string for inspection
        """
        
        
class ResourceObject(object):
    """Base object of Resource
    Instances of these objects can be encoded and decoded, stored and retrieved.
    """

    implements(IResourceObject)

    type = None

    @classmethod
    def get_type(cls):
        """@note was considering a scheme where the type is taken as the 
        name of the class. 
        """
        return cls.__name__.lower()

    @property
    def value(self):
        """
        @brief Bytes that actually go into the store (i.e. content
        addressable key/value store).
        @todo cache encoding() result to avoid re-computing the same thing.
        """
        return self.encode()

    @property
    def hash(self):
        return sha1(self.value, bin=False)

    def encode(self):
        """
        @brief Encode this instance.
        """
        body = self._encode_body()
        header = self._encode_header(body)
        encoded = "%s%s" % (header, body,)
        return encoded

    def __eq__(self,other):
        if isinstance(other, ResourceObject):
            return self.encode() == other.encode()
        return False


    @staticmethod
    def decode(value, types):
        """
        @brief Decode an encoded object. This is a general entry-point
        that starts off the decoding process using the definitive
        decode_header implementation. Once the header is decoded, the type
        name is known (type being Storable Object Type, implemented as a class
        that extends BaseObject) and the actual type (class) is retrieved
        (from the provided types dict) to which the rest of the decoding is
        delegated.
        @param value An encoded storable object.
        @param types A dictionary of type_name:type_class where type_class
        is a derived class of BaseObject (Blob, Tree, Commit, ...).
        @retval A new instance of the encoded object
        """

        type, body = ResourceObject._decode_header(value)
        obj = types[type]._decode_body(body)
        return obj

    @classmethod
    def decode_full(cls, encoded_obj):
        """
        @brief Decoded known object type. This makes it so you can test
        decoding specific object types with out passing a dict of types (as
        in the encode method).
        """
        type, body = ResourceObject._decode_header(encoded_obj)
        assert type == cls.type
        obj = cls._decode_body(body)
        return obj

    def _encode_header(self, body):
        """
        @brief method all derived classes use this to compute header.
        @note Header format:
            [type][space][content-length][null-char]
        """
        length = len(body)
        header = "%s %d%s" % (self.type, length, NULL_CHR,)
        return header

    @staticmethod
    def _decode_header(encoded_obj):
        """
        @brief extract the header from an encoded value
        """
        sep_index = encoded_obj.find(NULL_CHR)
        head = encoded_obj[:sep_index]
        type, content_length = head.split()
        body = encoded_obj[sep_index+1:]
        #Implement an Exception class to raise here
        assert len(body) == int(content_length)
        return type, body

    def _encode_body(self):
        """
        @brief Implement for each object type
        """
        pass

    @classmethod
    def _decode_body(cls, encoded_body):
        """
        @brief implement for each object type
        """
        pass


class TimeStampResource(ResourceObject):
    
    type = 'timestamp'
    os.environ['TZ']='UTC'
    
    def __init__(self, timestamp=time.time()):
        self.content=timestamp

    def _encode_body(self):
        return json.dumps(self.content)

    def __str__(self):
        head = '='*10
        strng  = """\n%s Resource Type: %s %s\n""" % (head, str(self.get_type()), head)
        strng += """= Content: "%s"\n""" % str(self.content)
        strng += """= DateTime: %s UTC \n""" % str(datetime.datetime.fromtimestamp(self.content))
        strng += head*2
        return strng

    @classmethod
    def _decode_body(cls, encoded_body):
        return cls(timestamp=json.loads(encoded_body))

types['timestamp']=TimeStampResource

class UniqueResource(ResourceObject):
    type = 'unique'

    def __init__(self, unique=None):
        if unique == None:
            unique = str(uuid.uuid4())
        self.content=unique

    def _encode_body(self):
        return self.content

    def __str__(self):
        head = '='*10
        strng  = """\n%s Resource Type: %s %s\n""" % (head, str(self.get_type()), head)
        strng += """= UUID: "%s"\n""" % self.content
        strng += head*2
        return strng

    @classmethod
    def _decode_body(cls, encoded_body):
        return cls(unique=encoded_body)

types['unique']=UniqueResource


class IdentityResource(ResourceObject):
    type = 'identity'

    def __init__(self, username=None, email=None, firstname=None, lastname=None, uniqueid=None):
        self.username = username
        self.email = email
        self.firstname = firstname
        self.lastname = lastname
        if uniqueid == None:
            uniqueid=UniqueResource()
        self.uniqueid = uniqueid
        
    def _encode_body(self):
        d= self.__dict__
        d['uniqueid']=d['uniqueid'].encode()
        
        return json.dumps(d)
        
    def __str__(self):
        head = '='*10
        strng  = """\n%s Store Type: %s %s\n""" % (head, str(self.get_type()), head)
        strng += """= User Name: "%s"\n""" % self.username
        strng += """= Email: "%s"\n""" % self.email
        strng += """= Name(last,first): "%s,%s"\n""" % (self.lastname,self.firstname)
        strng += """= Unique ID: "%s"\n""" % self.uniqueid.value
        strng += head*2
        return strng
        
    @classmethod
    def _decode_body(cls, encoded_body):
        
        d = json.loads(encoded_body)
        
        e={}
        for item in d:
            e[str(item)]=str(d[item])

        e['uniqueid']=ResourceObject.decode(e['uniqueid'],types)

        return cls(**e)
        
types['identity']=IdentityResource


class TypedAttribute(object):

    def __init__(self, name, type, default=None):
        self.name = '_' + name
        self.type = type
        self.value = default if default else type()

    def __get__(self, inst, cls):
        return getattr(inst, self.name, self.value)

    def __set__(self, inst, value):
        self.value = value
        setattr(inst, self.name, value)
        setattr(inst, '_' + self.name, self.encode())

    def encode(self):
        """
        encded:
        "str email\x00handle@domain.tld"
        """
        encoded = "%s %s%s%s" % (self.type.__name__, self.name, NULL_CHR, self.value,)
        return encoded

class BaseResource(object):
    """
    """

class IdentityResource(BaseResource):

    name = TypedAttribute('name', Str, 'Dorian Raymer')
    email = TypedAttribute('email', str)
    birth = TypedAttribute('birth', str)
    weight = TypedAttribute('w', int)

    def encode(self, thing):
        """
        encded:
        "str email\x00handle@domain.tld"
        """
        encoded = "%s %s%s%s" % (thing.type, thing.name, NULL_CHR, thing,)
        return encoded


class IdentityResource(dict):

    def __init__(self, name, email='a@b.com', birth=0):
        """
        """
        dict.__init__(self, (('name', name), ('email', email), ('birth', birth)))
        self.name = name

    def encode(self):
        encoded = []
        for k in self:
            v = self[k]
            encoded.append("%s %s%s%s" % (k, NULL_CHR, type(v).__name__, v,))
        return encoded




class BaseResource(object):
    """
    """
    resource = TypedProperty(str)
    lcs = TypedProperty(LifeCycleState)
    uuid = TypedProperty(str)
    
    def stage(self):
        """
        """
        blobs=[]
        trees=[]
        atts=self.__dict__

        for k, v in atts.items():
            b=cas.Blob(v)
            blobs.append()
            trees.append(cas.Tree(k,b))
            blobs.append(b)
    
        return blobs, trees
    
    
    
class IdentityResource(BaseResource):
    """
    """
    
    uname = TypedProperty(str)
    email = TypedProperty(str)
    first = TypedProperty(str)
    last = TypedProperty(str)
    

    




