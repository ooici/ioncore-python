"""
@brief Interface definitions for core data store components and entities.

The set of primitive types (and structures/DataObject?) can be thought of
as an invariant constant. 
"""

from zope.interface import Interface

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

class IEncoder(Interface):
    """
    An encoder has functionality for encoding/decoding the primitive types
    of the data store.
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

class ISerializer(Interface):
    """
    Uniform, common "goto" interface for serializing/encoding and
    de-serializing/decoding a DataObject 

    @note The Serializer concept could have a few purposes
        * Registry/common interface for all different encoders
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

    def remove(key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
        """
