"""
@file ion/data/backend/redis.py
@author Dorian Raymer
"""

from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator

from txredis import protocol

from ion.data import store

class Store(store.IStore):
    """
    """
    def __init__(self, client):
        """
        @brief Initializes store instance
        @param kwargs arbitrary keyword arguments interpreted by the subclass
        """
        self.kvs = client

    @classmethod
    def create_store(cls, host='localhost', port=6379, **kwargs):
        """
        @brief Factory method to create an instance of the store.
        @param kwargs arbitrary keyword arguments interpreted by the subclass to
                configure the store.
        @retval Deferred, for IStore instance.
        """
        client_creator = ClientCreator(reactor, protocol.Redis)
        d = client_creator.connectTCP(host, port)
        d.addCallback(cls)
        return d

    def get(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for value associated with key, or None if not existing.
        """
        return self.kvs.get(key)

    def put(self, key, value):
        """
        @param key  an immutable key to be associated with a value
        @param value  an object to be associated with the key. The caller must
                not modify this object after it was
        @retval Deferred, for success of this operation
        """
        return self.kvs.set(key, value)

    def query(self, regex):
        """
        @param regex  regular expression matching zero or more keys
        @retval Deferred, for list of values for keys matching the regex
        """
        return self.kvs.keys(regex)

    def remove(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
        """
        return self.kvs.delete(key)
