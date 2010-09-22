"""
@file ion/data/datastore/cas.py
@author Dorian Raymer
@author Michael Meisinger
@author David Stuebe

A simpler, more general cas re-factor. 
Instead of trees and blobs, there are Nodes; structured nodes (general tree)
and unstructured data nodes (blob). The structure quality is in the context
of the ca store format, and has nothing to do with the actual data it
represents. These are storage objects or content objects. 

An object (external data object) has it's own structure. The elements in
that structure have a type. Every type needs an encoder. Encoders can be
implemented as specific mechanisms, per type, or as a monolithic encoder
for a set of types. The type may defined independently from its encoding. 
The actual pure encoded data is stored in an leaf node (unformatted).
The meta data describing an encoded type and a pointer to the leaf node 
is stored in a structured node.
The meta data indicates what type encoder was used. The ca store interface
can then use those encoders as plugins, and maintain it's own simple header
structure separately. 

Create a container (for full serialization) using a proto buffer with a
repeated field of type bytes.

The mutable object description store also uses nodes. There is one node per
external object (data object). The node has a key (UUID) in the ['semantic']
namespace. This 'semantic' key is an invariant, and points to the root
storage object (commit node, where commit is a particular state of the data
beneath the root node). This pointer changes overtime while the uuid key
stays constant to the outer world. The description/reference node also has
other attributes that may or may not mutate, including owner id (pointer to
identity object (type: semantic uuid key)), time stamp[?], branch pointers
(some collection of pointers that point to alternate root nodes). 

This mutable structure is encoded into one serialized object or encoded
using the cassandra data model, and stored under the uuid key. The
description/reference store interface is a regular key/value interface.  

@note This file is almost Twisted independent; the only exception is in
CAStore -- ion.data.store.IStore specifies methods that return
twisted.internet.defer.Deferred objects.


-----
This implementation combines the Node object definitions and their
encoding and serialization.
Maybe the encoding and serialization should be decoupled...
"""

import hashlib
import struct
import logging

from zope.interface import Interface
from zope.interface import implements
from zope.interface import Attribute 


NULL_CHR = "\x00"

def sha1hex(val):
    return hashlib.sha1(val).hexdigest()

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

class CAStoreError(Exception):
    """
    Exception class for CAStore
    """

class Element(tuple):
    """
    Represents a child element of a tree object. Not an object itself, but
    a convenience container for the format of an element of a tree.  A
    tuple is immutable, so this is a safe way to carry around the elements
    of a Tree.

    @note Want flexibility on what obj is: Tree is encoded with the obj's
    sha1 hash (bin version).
    """

    def __init__(self, name, obj, mode=None):
        """
        @param name something to associate with obj
        @param obj a storable object or it's id.
        @brief Only store obj if it is really an instance of BaseObject.
        @note Tree uses this to represent it's child elements. Tree treats
        this like a tuple. This extension of tuple enforces the order of
        name, obj, mode.
        """
        if not isinstance(obj, BaseObject):
            obj = None
        self.obj = obj

    def __new__(cls, name, obj, mode='100644'):
        """
        @note By overriding __new__, we can instantiate a tuple with these
        3 specific arguments (normally, tuple takes only one argument)
        """
        if isinstance(obj, BaseObject):
            obj_id = sha1(obj)
        else:
            #can only assume it is the sha1(obj) id
            obj_id = obj
        return tuple.__new__(cls, [name, obj_id, mode])

    def __str__(self):
        head = "="*10
        strng ="""\n%s Entity %s\n""" % (head, head)
        strng+="""= tuple:: name:"%s" id:"%s" mode:"%s"\n""" % (self[0],sha1_to_hex(self[1]),self[2])
        s=False
        if self.obj:
            s=True
        strng+="""= tuple obj present=%s \n""" % s
        strng+=head*2
        return strng



class ICAStoreNode(Interface):
    """
    Interface of immutable content objects stored in CAStore.
    """

    type = Attribute("""@param type Type of storable object. This should be
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

class BaseNode(object):
    """Base object of content addressable value store
    Instances of these objects are immutable.
    """

    implements(ICAStoreNode)

    type = None
    _value_cache = None
    _encoded_cache = None

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
        @note Do we need way to get the value? Or just encode and decode.
        Really, there is not an encoding option, this IS the encoding, so
        maybe encode and decode aren't needed..only something like 'value'.
        """
        if not self._value_cache:
            self._value_cache = self.encode()
        return self._value_cache

    def encode(self):
        """
        @brief Encode this instance.
        @note This is where a cache should be. BaseObject.value is DEPRECATED.
        @todo Use cStringIO buffer instead of python str.
        """
        if not self._encoded_cache:
            body = self._encode_body()
            header = self._encode_header(body)
            self._encoded_cache = "%s%s" % (header, body,)
        return self._encoded_cache

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
        type, body = BaseObject._decode_header(value)
        obj = types[type]._decode_body(body)
        return obj

    @classmethod
    def decode_full(cls, encoded_obj):
        """
        @brief Decoded known object type. This makes it so you can test
        decoding specific object types with out passing a dict of types (as
        in the encode method).
        """
        type, body = BaseObject._decode_header(encoded_obj)
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


class Blob(BaseNode):
    """
    Blob is a container for blob of bytes (string, or serialized object).
    """
    type = 'blob'

    def __init__(self, content):
        """
        @param content serializable blob (str or bytes)
        @note once content is set, it should not change
        """
        self.content = content
    
    def _encode_body(self):
        """
        @brief Content held by blob should already be in storable
        (serialized) form.
        """
        return self.content

    def __str__(self):
        head = '='*10
        strng  = """\n%s Store Type: %s %s\n""" % (head, self.type, head,)
        strng += """= Key: "%s"\n""" % sha1hex(self.value)
        strng += """= Content: "%s"\n""" % str(self.content)
        strng += head*2
        return strng


    @classmethod
    def _decode_body(cls, encoded_body):
        """
        @brief Decoding an encoded object body is the same as creating a new
        instance with the context contained in encoded_body.
        @note The Blob object type decoding is trivial, as any higher-level
        encoding is ignored here (by virtue of being a blob).
        @retval New instance of Blob.
        """
        return cls(encoded_body)

class Tree(BaseNode):
    """
    Tree Object

    @todo implement __iter__
    """
    type = 'tree'

    elementFactory = Element

    def __init__(self, *children):
        """
        @param children (name, obj_hash, mode)

        @note XXX For organizational convenience, child objects could be
        represented by an Element class...a container for the object, name,
        and mode (state bit map). The element would be completely abstract
        (arbitrary) in the context of the CAStore, but it might be part of
        the data model in a higher-level application.
        """
        entities = []
        names = {}
        for child in children:
            if not isinstance(child, self.elementFactory):
                child = self.elementFactory(*child)
            entities.append(child)
            names[child[0]] = child
        self.children = entities
        self._names = names

    def __getitem__(self, key):
        return self._names[key].obj
        
    def _encode_body(self):
        """
        format for each child in body of tree object
        [6 bytes][space][name][null char][hash]
        @note should hash be string or binary of sha1 hexdigest?
        @todo Use the 20 byte digest instead.
        """
        body = "" #@todo use buffer
        for (name, obj_hash, mode) in self.children:
            assert len(obj_hash) == 20 #bin sha1 (not hex)
            body += "%s %s\x00%s" % (mode, name, obj_hash,)
        return body

    def __str__(self):
        head = "="*10
        strng ="""\n%s Store Type: %s %s\n""" % (head, self.type, head,)
        strng+="""= Key: "%s"\n""" % sha1hex(self.value)
        for element in self.children:  
            strng+="""= name: "%s", id: "%s"\n""" % (element[0], sha1_to_hex(element[1]),)
        strng+=head*2
        return strng

    @classmethod
    def _decode_body(cls, encoded_body):
        """
        @brief Parse encoded Tree object.
        @param encoded_body Storable (serialized) representation of Tree
        object.
        @retval New instance of Tree.
        """
        #No longer using this parser (this one works on trees that encode a
        #40 char hex sha1
        #children = cls._decode_body_re(encoded_body)

        #This one works on trees that encode the 20 byte binary sha1
        children = cls._decode_body_parser(encoded_body)
        entities = [cls.child(*c) for c in children]
        return cls(*entities)

    @staticmethod
    def _decode_body_parser(raw):
        """
        @brief Parse encoded Tree using a two part processing loop. First
        look for a null character; the bytes before the null are the mode +
        name seperated by a space character. The 20 bytes that follow make
        up the sha1 hash.
        """
        raw = list(raw)
        def read_to_null(raw):
            buf = ''
            while raw:
                char = raw.pop(0)
                if char == NULL_CHR:
                    break
                buf += char
            return buf, raw

        def read_sha1(raw):
            hash, raw = ''.join(raw[0:20]), raw[20:]
            return hash, raw

        children = []
        while True:
            mode_name, raw = read_to_null(raw)
            mode, name = mode_name.split()
            hash, raw = read_sha1(raw)
            children.append((name, hash, mode))
            if not raw:
                break
        return children

    @classmethod
    def child(cls, name, obj, mode=None):
        """
        @brief A factory for creating child entities for a Tree.
        """
        return cls.elementFactory(name, obj, mode)


class Commit(BaseNode):
    """
    Commit Node
    Context for a root node. A commit is a particular state of a root node.
    """
    type = 'commit'

    def __init__(self, tree, parents=[], log="", **other):
        """
        @brief Context wrapper around a root node (formerly a tree).
        @param tree hash or object
        @param parent commit hash or object. Sha1 hash in hex form.
        @param log Record of commit reason/context/change/etc.
        """
        if isinstance(tree, BaseObject):
            tree_obj, tree = tree, sha1hex(tree)
        else:
            tree_obj = None
        self.tree = tree
        self.tree_obj = tree_obj
        self.parents = parents
        self.log = str(log) #or unicode? or what?
        self.other = other

    def __str__(self):
        head = "="*10
        strng ="""\n%s Store Type: %s %s\n""" % (head, self.type, head,)
        strng+="""= Key: "%s"\n""" % sha1hex(self.value)
        strng+="""= Tree: "%s"\n""" % self.tree
        if self.parents:
            strng+="""= Parent: "%s"\n""" % self.parents[0]
        strng+="""= Log: "%s"\n""" % self.log
        strng+=head*2
        return strng

    def _encode_body(self):
        """
        @brief encoded store-able format 
        General format:
        [type][ ][hash][\n]
        [\n]
        [log]

        Example:
        tree [tree_obj_hash]\n
        parent [parent_hash]\n\n
        \n
        
        """
        body = ""
        body += "%s %s\n" % ('tree', self.tree,)
        for parent in self.parents:
            body += "%s %s\n" % ('parent', parent,)
        body += "\n%s" % self.log
        return body

    @classmethod
    def _decode_body(cls, encoded_body):
        """
        @brief Parse encoded commit object.
        @note Split raw into list (on new-line). Process until first blank line
        is encountered.
        @retval New instance of Commit.
        """
        raw = encoded_body
        tree = None
        parents = []
        other = {}
        log = ''
        parts = raw.split('\n')
        while True:
            part = parts.pop(0)
            if part:
                space = part.find(' ')
                type, rest = part[:space], part[space+1:]
                if type == 'tree':
                    tree = rest
                elif type == 'parent':
                    parents.append(rest)
                else:
                    other[type] = rest
            else:
                #First blank line indicates we are now at the log.
                #Join the remaining parts back together with \n
                #Make sure what went in is what comes out!
                #Verify with sha1 hash
                log = '\n'.join(parts)
                break
        return cls(tree, parents, log=log, **other)


class ICAStore(Interface):
    """
    @brief Content addressable value store. Stores ICAStoreObject instances
    in a persistent storage via an object providing ion.data.store.IStore. 
    @todo Add a delete/remove method to interface?
    """

    TYPES = Attribute("""@param TYPES Dict providing map of ICAStoreObject
        type names to ICAStoreObject content object implementation class.""")

    def get(id):
        """
        @param id key of content object.
        @retval defer.Deferred that fires with an object that provides
        ICAStoreObject.
        """

    def put(obj):
        """
        @brief The key the object is stored at is determined by taking the
        hash of the content. This ensures the immutability of all content as
        the keys cannot be directly specified for writing (assuming sha1
        hash algorithm is used and treating it as collision free.) 
        @param obj instance of object providing ICAStoreObject
        @retval defer.Deferred that fires with the obj id.
        """

class StoreContextWrapper(object):
    """
    Context wrapper around backend store.
    """

    def __init__(self, backend, prefix):
        """
        @param backend instance that provides ion.data.store.IStore
        interface.
        @param prefix segment of namespace (the context).
        """
        self.backend = backend
        self.prefix = prefix

    def _key(self, id):
        return self.prefix + id

    def get(self, id):
        return self.backend.get(self._key(id))

    def put(self, id, val):
        return self.backend.put(self._key(id), val)

    def remove(self, id):
        return self.backend.remove(self._key(id))

    def query(self, regex):
        pattern = "%s%s" % (self.prefix, regex,)
        return self.backend.query(pattern)

class CAStore(object):
    """
    Content Addressable Store
    """

    BaseStorableType = BaseObject

    TYPES = {
            Blob.type:Blob,
            Tree.type:Tree,
            Commit.type:Commit,
            }

    def __init__(self, backend, namespace='', compression=None):
        """
        @param backend instance that provides the ion.data.store.IStore
        interface.
        @param namespace root prefix qualifying context for this CAS with in the
        general space of the backend store.
        """
        self.backend = backend
        self.namespace = namespace
        self.objs = StoreContextWrapper(backend, namespace + '.objs.')

    def decode(self, encoded_obj):
        """
        @brief decode raw object read from backend store
        @param encoded_obj encoded object of one of the type in self.TYPES
        """
        obj = self.BaseStorableType.decode(encoded_obj, self.TYPES)
        return obj

    def hash_object(self, obj):
        """
        @brief Compute the hash of an object (which is used as a key)
        """
        hash = sha1(obj)
        return hash

    def put(self, obj):
        """
        @param obj hashable object to store
        @note The mechanism for hashing a storable object should not be
        part of the object. It should be functionality provided and
        controlled by the store. The objects know how to encode and decode
        themselves, and they also know about inter-store-object
        relationships.
        If knowledge of an objects hash is only obtainable here, then it
        can always be assumed that a hash corresponds to an object in the
        store.
        """
        data = obj.encode() #compress arg
        hash = sha1(data)
        id = sha1_to_hex(hash)
        d = self.objs.put(id, data)
        d.addCallback(lambda _: id)
        return d

    def get(self, id):
        """
        @param id key where an object is stored (object hash)
        @retval Instance of store object.
        @todo flexible handling of id (bin sha1 or hex sha1)
        """
        if len(id) == 20:
            id = sha1_to_hex(id)
        d = self.objs.get(id)
        def _decode_cb(data):
            if not data:
                raise CAStoreError("Object with id: %s not found" % id)
            obj = self.decode(data)
            # assure integrity 
            if not id == sha1(obj, bin=False):
                raise CAStoreError("Object Integrity Error!")
            return obj
        d.addCallback(_decode_cb)
        # d.addErrback
        return d

    def _obj_exists(self, id):
        """Store (backend) interface does not have an 'exists' method; this
        has to be implemented by trying to get the whole object, which
        could be just as inefficient as writing over the existing object.
        """


