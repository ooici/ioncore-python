"""
@brief Interface definitions for core data store components and entities.

The set of primitive types (and structures/DataObject?) can be thought of
as an invariant constant. 
"""

from zope.interface import Interface
from zope.interface import Attribute

class IDataObject(Interface):
    """
    In general, this is a struct -- a container that holds typed
    attributes, which can be other Data Objects, and references (links) to
    other Data Objects. 

    The behavior of this object is minimal -- it is limited to accessors,
    structure immutability, and attribute type enforcement. 
    Architecturally, it is a basic pure data structure.
    
    It is possible Data Object might not be the correct name for this object, 
    in light of recent design discussion.
    """

    def set(key, value):
        """
        """

    def get(key):
        """
        """

    def get_keys():
        """
        @retval Deferred that returns list of keys.
        """

class IDataObjectNode(Interface):
    """
    """

class IEncoder(Interface):
    """
    An encoder has functionality for encoding/decoding the primitive types
    of the data store.

    * A binding between messaging and encoding
        o There could be a 'partial' encoder that, for a data object
        structure, completes the final serialization process.
        Separating full-serialization (for sending/storing a structure)
        from the encoding of the types provides flexibility. 
            - Some structures need to remain decomposed when
              sent/stored (pass by reference and ca store).
            - Some structures should always be fully serialized and
              sent/stored as opaque byte strings/blobs.
    """

    def encode(obj):
        """
        @param obj DataObject instance or datastore type
        @retval encoded/serialized version of object
        @note Could return list of messages, or packed set of content
        """

    def decode(data):
        """
        @param data encoded/serialized data object
        @retval DataObject instance.
        @note Could accept list of encoded content, or one
        packed/serialized thing...
        """

class ISerialization(Interface):
    """
    Uniform, common "goto" interface for serializing/encoding and
    de-serializing/decoding a DataObject 

    * Registry/common interface for all different encoders

    """

    def register():
        """
        """

    def encode(obj, encoding=None):
        """
        """

    def decode(data, encoding=None):
        """
        """

class IStore(Interface):
    """
    Basic key/value store interface.
    (or IKeyValueStore ?)

    This is the common low level key value store interface on top of which,
    other store types can add/constrain behavior. 

    Question, does this represent a partition of storage name space? or
    does it support partitioning?
    For example, the object node store and the content(value) node store
    ought to use separate name spaces to ensure the immutable nature of
    content nodes (no overwriting of content keys by identically named mutable
    nodes)

    Partitions (or namespaces) can be organized by adapting interface (with
    out IStore needing to know what those might be). 

    A convention could be encoded into key names which could map on to
    technology specific organization (cassandra columns, redis sets, etc.)

    Think about how this supports usage of cassandra data structures.

    This is more of a protocol interface than a programming interface in
    the sense that this is a correspondence between a backend technology
    and a frontend application. It is brought into existence by some
    factory mechanism that knows about the specific backend.
    """

    def get(key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for value associated with key, or None if not existing.
        """

    def put(key, value):
        """
        @param key  an immutable key to be associated with a value
        @param value  an object to be associated with the key. The caller must
                not modify this object after it was
        @retval Deferred, for success of this operation
        """

    def getput(key, value, oldvalue):
        """
        should this be part of the interface?
        """

    def remove(key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
        """

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


class ICAStore(Interface):
    """
    @brief Content addressable value store. Stores ICAStoreObject instances
    in a persistent storage via an object providing ion.data.store.IStore. 
    @todo Add a delete/remove method to interface?

    This adapts an IStore provider 
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


class IDataStore(Interface):
    """
    This is the entry point interface for initiating access to
    (create/delete) data. The modify/read ability is only possible after this
    initiation. 

    gets and puts data object nodes

    has key/value store access
    """

    def clone():
        """
        open/get / create (clone structure)

        clone (or open and get node entry)  

        @retval an IDataNodeStore provider
        """
    
    def push():
        """
        or sync
        atomic transaction which mutates data object Node
        """


class IDataNodeStore(Interface):
    """
    Specific access behavior around a general key/value interface.
   
    gets and puts content nodes

    has key/value store access
    """

    def get(id):
        """
        get mutable data node
        """

    def put(id, newnode, lastnode):
        """
        put data node.
        This can only succeed if the provided lastnode is infact the
        current node (test and set)
        """

    def checkout():
        """
        read certain (default, latest) state
        """







