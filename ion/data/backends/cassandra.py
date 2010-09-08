#!/usr/bin/env python
"""
@file ion/data/backends/cassandra.py
@author Paul Hubbard
@author Michael Meisinger
@author Paul Hubbard
@author Dorian Raymer
@brief Implementation of ion.data.store.IStore using pycassa to interface a
        Cassandra datastore backend
@Note Test cases for the cassandra backend are now in ion.data.test.test_store
"""

import re
import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator

#import pycassa
from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory, ManagedThriftClientProtocol

from ion.core import ioninit
from ion.data.store import IStore

import uuid

CONF = ioninit.config(__name__)
CF_default_keyspace = CONF['default_keyspace']
CF_default_colfamily = CONF['default_colfamily']
CF_default_cf_super = CONF['default_cf_super']
CF_default_namespace = CONF['default_namespace']
CF_default_key = CONF['default_key']


class CassandraStore(IStore):
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    @Note Default behavior is to use a random super column name space!
    """
    def __init__(self, **kwargs):
        self.kvs = None
        self.cass_host_list = None
        self.keyspace = None
        self.colfamily = None
        self.cf_super = True
        self.namespace = None
        self.key=None

    @classmethod
    def create_store(cls, **kwargs):
        """
        @brief Factory method to create an instance of the cassandra store.
        @param kwargs keyword arguments to configure the store.
        @param cass_host_list List of hostname:ports for cassandra host or cluster
        @retval Deferred, for IStore instance.
        """
        inst = cls(**kwargs)
        inst.kwargs = kwargs
        inst.cass_host_list = kwargs.get('cass_host_list', None)
        inst.keyspace = kwargs.get('keyspace', CF_default_keyspace)
        inst.colfamily = kwargs.get('colfamily', CF_default_colfamily)
        inst.cf_super = kwargs.get('cf_super', CF_default_cf_super)
        inst.key = kwargs.get('key', CF_default_key)
        
        if not inst.key:
            inst.key = str(uuid.uuid4())
        
        if inst.cf_super:
            inst.namespace = kwargs.get('namespace', CF_default_namespace)
            if inst.namespace == None:
                # Must change behavior to set a random namespace so that test don't interfere!
                inst.namespace = ':'
        else:
            if inst.namespace:
                logging.info('Ignoring namespace argument in non super column cassandra store')
            inst.namespace=None

        if not inst.cass_host_list:
            logging.info('Connecting to Cassandra on localhost...')
        else:
            logging.info('Connecting to Cassandra ks:cf=%s:%s at %s ...' %
                         (inst.keyspace, inst.colfamily, inst.cass_host_list))
        #inst.client = pycassa.connect(inst.cass_host_list)
        #inst.kvs = pycassa.ColumnFamily(inst.client, inst.keyspace,
        #                                inst.colfamily, super=inst.cf_super)
        inst.manager = ManagedCassandraClientFactory()
        inst.client = CassandraClient(inst.manager, inst.keyspace)
        logging.info('connected to Cassandra... OK.')
        logging.info('cass_host_list: '+str(inst.cass_host_list))
        logging.info('keyspace: '+str(inst.keyspace))
        logging.info('colfamily: '+str(inst.colfamily))
        logging.info('cf_super: '+str(inst.cf_super))
        logging.info('namespace: '+str(inst.namespace))
        client_creator = ClientCreator(reactor, ManagedThriftClientProtocol)
        port = 9160
        host = 'amoeba.ucsd.edu'
        d = client_creator.connectTCP(host, port)
        d.addCallback(cls)
        return d
        

    @defer.inlineCallbacks
    def clear_store(self):
        """
        @brief Delete the super column namespace. Do not touch default namespace!
        @note This is complicated by the persistence across many 
        """
        if self.cf_super:
            yield self.client.remove(self.key, self.colfamily, super_column=self.namespace)
        else:
            logging.info('Can not clear root of persistent store!')
        defer.returnValue(None)
        
    @defer.inlineCallbacks
    def get(self, col):
        """
        @brief Return a value corresponding to a given key
        @param col Cassandra column
        @retval Deferred, for value from the ion dictionary, or None
        """
        value = None
        #try:
        if self.cf_super:
            value = yield self.client.get(self.key, self.colfamily, column=[col], super_column=self.namespace)
        else:
            value = yield self.client.get(self.key, self.colfamily, column=[col])
                #logging.debug('Key "%s":"%s"' % (key, val))
                #this could fail if insert did it wrong
        value=value.get(col)
        #except pycassa.NotFoundException:
            #logging.debug('Key "%s" not found' % key)
        #pass
        defer.returnValue(value)

    @defer.inlineCallbacks
    def put(self, col, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value
        @note Value is composed into OOI dictionary under keyname 'value'
        @retval Deferred for success
        """
        #logging.debug('writing key %s value %s' % (key, value))
        if self.cf_super:
            yield self.client.insert(self.key, self.colfamily, {self.namespace:{col:value}})
        else:
            yield self.client.insert(self.key, self.colfamily, {col:value})
        
        defer.returnValue(None)

    @defer.inlineCallbacks
    def query(self, regex):
        """
        @brief Search by regular expression
        @param regex Regular expression to match against the keys
        @retval Deferred, for list, possibly empty, of keys that match.
        @note Uses get_range generator of unknown efficiency.
        """
        #@todo This implementation is very inefficient. Do smarter, but how?
        matched_list = []
        if self.cf_super:
            klist = yield self.client.get(self.key, self.colfamily, super_column=self.namespace)
        else:
            klist = yield self.client.get(self.key, self.colfamily)

        for x in klist.keys():
            #m = re.search(regex, x[0])
            m = re.findall(regex, x)
            if m:
                matched_list.extend(m)

        defer.returnValue(matched_list)

    @defer.inlineCallbacks
    def remove(self, col):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval Deferred, for success of operation
        @note Deletes are lazy, so key may still be visible for some time.
        """
        if self.cf_super:
            yield self.client.remove(self.key, self.colfamily, columns=[col], super_column=self.namespace)
        else:
            yield self.client.remove(self.key, self.colfamily, columns=[col])
        defer.returnValue(None)
