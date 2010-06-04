#!/usr/bin/env python
"""
@file ion/data/objstore.py
@author Dorian Raymer
@author Michael Meisinger
@author David Stuebe
@brief storing immutable values (blobs, trees, commit) and storing structured
        mutable objects mapped to graphs of immutable values

@todo Decide if Objects(BaseObject) pass around object instances, or their
hashes.
"""

import re
import hashlib
import struct
import logging

from zope.interface import Interface, Attribute, implements

from twisted.internet import defer

from ion.data.dataobject import DataObject

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


class Entity(tuple):
    """
    Represents a child element of a tree object. Not an object itself, but
    a convenience container for the format of an element of a tree.

    @note Want flexibility on what obj is: Tree is encoded with the obj's
    sha1 hash (bin version).
    """

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

class ICAStoreObject(Interface):
    """
    Interface for objects stored in CAStore.
    """

    type = Attribute("""@param type Type of storable object. This should be
            set as a class attribute for each type implementation class""")

    def value():
        """
        @brief Bytes to write into store.
        """

    def hash():
        """
        @brief Hash (sha1) of storable value
        @todo Decide if this should really be part of the interface
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

class BaseObject(object):
    """Base object of content addressable value store
    Instances of these objects are immutable.
    """

    implements(ICAStoreObject)

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

class Blob(BaseObject):
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

class Tree(BaseObject):
    """
    Tree Object

    @todo implement __iter__
    """
    type = 'tree'

    entityFactory = Entity

    def __init__(self, *children):
        """
        @param children (name, obj_hash, mode)
        @note mode is 6 bytes. Git uses this for the file mode

        @note XXX For organizational convenience, child objects could be
        represented by an Entity class...a container for the object, name,
        and mode (state bit map). The entity would be completely abstract
        (arbitrary) in the context of the CAStore, but it might be part of
        the data model in a higher-level application.
        """
        entities = []
        for child in children:
            if not isinstance(child, self.entityFactory):
                child = self.entityFactory(*child)
            entities.append(child)
        self.children = entities
        
    def _encode_body(self):
        """
        format for each child in body of tree object
        [6 bytes][space][name][null char][hash]
        @note should hash be string or binary of sha1 hexdigest?
        """
        body = "" #@todo use buffer
        for (name, obj_hash, mode) in self.children:
            assert len(obj_hash) == 20 #bin sha1 (not hex)
            body += "%s %s\x00%s" % (mode, name, obj_hash,)
        return body

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
        return cls(*children)

    @staticmethod
    def _decode_body_re(raw):
        """
        @brief Parse encoded Tree using regular expression. This is an easy
        way to decode Tree objects that use hex string sha1 format (as
        opposed to bin sha1 format).
        @note as long as the name of a tree element is not anything weird,
        this should work...but it's hard to ensure it will always work!
        Alternative parser could be implemented without re.
        """
        #          [mode]     [name]    [hash(str)]
        pattern = "([0-9]*)[ ]([\S]*)\x00(\w{40})" #40char string sha1 version
        matches = re.findall(pattern, raw)
        smatches = [(name, hash, mode) for mode, name, hash in matches]
        return smatches

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
        return cls.childFactory(name, obj, mode)


class Commit(BaseObject):
    """
    Commit Object
    """
    type = 'commit'

    def __init__(self, tree, parents=[], log="", **other):
        """
        @param tree hash or object
        @param parent commit hash or object. Sha1 hash in hex form.
        @param log Record of commit reason/context/change/etc.
        """
        #if tree isinstance(Tree):
        #    tree = tree.hash
        self.tree = tree
        self.parents = parents
        self.log = str(log) #or unicode? or what?
        self.other = other

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
    Interface for a content addressable value store
    @todo determine appropriate interface methods.
    """

class StoreContextWrapper(object):
    """
    Context wrapper around backend store.
    """

    def __init__(self, backend, prefix):
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
        pattern = "%s(%s)" % (self.prefix, regex,)
        return self.backend.query(pattern)

class CAStore(object):
    """
    Content Addressable Store
    Manages and provides organizational utilities for a set of objects
    (blobs, trees, commits, etc.)
    """
    TYPES = {
            Blob.type:Blob,
            Tree.type:Tree,
            Commit.type:Commit,
            }

    def __init__(self, backend, namespace='', compression=None):
        """
        @param namespace root prefix qualifying context for this CAS with in the
        general space of the backend store.
        @param backend storage interface
        """
        self._backend = backend
        self.root_namespace = namespace
        self.objstore = StoreContextWrapper(backend, namespace + '.objects.')
        self.refstore = StoreContextWrapper(backend, namespace + '.refs.')

    def decode(self, encoded_obj):
        """
        @brief decode raw object read from backend store
        @param encoded_obj encoded object of one of the type in self.TYPES
        """
        obj = BaseObject.decode(encoded_obj, self.TYPES)
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
        value = obj.value #compress arg
        hash = sha1(value)
        id = sha1_to_hex(hash)
        d = self.objstore.put(id, value)
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
        d = self.objstore.get(id)
        def _decode_cb(raw):
            return self.decode(raw)
        d.addCallback(_decode_cb)
        # d.addErrback
        return d

    def _obj_exists(self, id):
        """Store (backend) interface does not have an 'exists' method; this
        has to be implemented by trying to get the whole object, which
        could be just as inefficient as writing over the existing object.
        """

class EntityProxy(Entity):
    """
    @brief Used for reading from the store
    """

    def __init__(self, backend, name, hash, mode=None):
        self.backend = backend
        self.hash = hash
        self._obj = None

    def get_obj(self):
        """
        @brief Get object from backend store, cache result.
        @retval Deferred that fires with obj
        """
        if not self._obj:
            d = self.backend.get(self.hash)
            def set_obj(obj):
                self._obj = obj
                return obj
            d.addCallback(set_obj)
            return d
        return defer.succeed(self._obj)

    def __new__(cls, backend, name, hash, mode=None):
        return tuple.__new__(cls, [name, hash, mode])

class BlobProxy(object):
    """
    Inverse of regular Blob object.
    Start with the object id (hash), fetching the content when needed
    """

    def __init__(self, backend, id):
        """
        @param backend (or objstore) active backend to read from
        @param id the object hash
        """
        self.objstore = backend
        self.id = id
        self._content = None

    def get_content(self):
        """
        @retval a Deferred that will fire with the content
        """
        if not self._content:
            d = self.objstore.get(self.id)
            def store_result(content):
                self._content = content
                return content
            d.addCallback(store_result)
        else:
            d = defer.succeed(self._content)
        return d

class TreeProxy(object):
    """
    Live tree of real objects
    """

    def __init__(self, backend, *child):
        """
        @param child An element of the tree (a child entity).
        """
        self.backend = backend
        self.children = child

class WorkingTree(object):
    """
    Tree of objects that may or may not be written to backend store
    """

    def __init__(self, backend, name=''):
        """
        @param backend Instance of CAStore.
        @param name of working tree; could have more than one.
        """
        self.backend = backend
        self.name = name
        self.entitys = {}

    def add(self, entity):
        """
        @param entity Instance of Entity or EntityProxy.
        """

    def update(self, entity):
        """
        """

    def remove(self, entity):
        """
        """

class Frontend(CAStore):
    """
    """

    def __init__(self, backend, namespace=''):
        CAStore.__init__(self, backend, namespace)

    def _get_symbolic_ref(self, name='HEAD'):
        self.backend.get(name)

    def _set_symbolic_ref(self, name='HEAD', ref=''):
        """
        """

    def _checkout(self):
        """
        Checkout is used to establish a context for the present. The
        default is to get the HEAD reference; the HEAD reference is the id
        of a commit object designated as the latest commit to use.
        """

    def _get_named_entity(self, name):
        """
        @brief Get object by name.
        @param name represents an object in a tree
        """

    def _put_raw_data_value(self, name, value):
        """
        @brief Write a piece of raw data
        """
        b = Blob(value)
        t = Tree(Entity(name, b.hash))
        d = self.put(t)
        #d.addCallback(

    def get_info(self, id):
        """
        @brief get an objects type
        """



