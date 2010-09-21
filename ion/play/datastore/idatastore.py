"""
@brief Interface definitions for core data store components and entities.
"""

from zope.interface import Interface

class IDataObject(Interface):
    """
    implementation to inherit Structure?
    """

    def set(key, value):
        """
        """

    def get(key):
        """
        """

    def get_keys():
        """
        """

class IEncoder(Interface):
    """
    Has a implementation independent set of Types it should be able to
    encode/decode 

    """

    def encode(obj):
        """
        """

    def decode(data):
        """
        """

class ISerializer(Interface):
    """
    Uniform, common "goto" interface for serializing/encoding and
    de-serializing/decoding a DataObject 
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
    Interface for all store backend implementations.
    All operations are returning deferreds and operate asynchronously.
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
