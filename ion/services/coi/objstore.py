"""
@file ion/services/coi/objstore.py
@author Dorian Raymer
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from zope import interface

from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.exception import ReceivedError
from ion.services.dm.preservation import store
from ion.services.dm.preservation import cas
from ion.data.datastore import objstore

class Serializer(object):

    def encode(self, obj, encoder=None):
        """
        @brief Operates on sendable objects
        """
        return obj.encode()

    def decode(self, bytes, decoder=None):
        """
        @brief Operates on encoded sendable objects
        """
        return

class ObjectChassis(object):
    """
    Distributed implementation of the Object Store Object Chassis
    object class meta
    object instance refs

    The chassis presents the revision control functional interface.
    The chassis provides access to the data of any storable data object.

        This interface relies solely on object/cas store interface methods.
        This means that all information stored/retrieved from the backend
        IStore should be apart of the standard DataObject, or be part of
        another meta model.



    The object chassis is the working version of any data object. Data
    object content is always extracted from the data store via it's commit
    object. In this way, the context of a data object (wrt
    history/ancestry change) is always determinable.

    """

    interface.implements(objstore.IObjectChassis)

    def __init__(self, objstore):
        """
        @note Not using the 'keyspace' arg of the original implementation.
        All ref information should come from one description object.
        @param objstore provider of objstore.IObjectStore
        """


class ObjectStoreService(ServiceProcess):
    """
    The service end of Distributed ObjectStore that mediates between the message based
    interface and the actual implementation.

    """

    declare = ServiceProcess.service_declare(name='objstore', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        """
        @brief setup creation of ObjectStore instance
        decide on which backend to use based on option

        It would be nice to separate the existence/creation of the
        ObjectStores IStore backend from the instantiation of ObjectStore.
        That way, if IStore is an in memory dict, it can be used by
        different IObjectStores; if IStore is a Redis client, it's
        connection can be managed elsewhere....actually, that is a good
        question, how is that connection managed?
        """
        # Need to make this an option:
        # IStore interface to local persistent store.
        backend = yield store.Store.create_store()
        self.objstore = yield objstore.ObjectStore(backend)


    def op_clone(self, content, headers, msg):
        """
        """

    @defer.inlineCallbacks
    def op_create(self, content, headers, msg):
        """
        XXX how to build object chassis?

        """
        try:
            name = content[0]
            encoded_baseClass = content[1]
            baseClass = dataobject.DEncoder().decode(encoded_baseClass)
            obj_chas = yield self.objstore.create(name, baseClass)
            yield self.reply_ok(msg, "??")
        except objstore.ObjectStoreError:
            yield self.reply_err(msg, None)

    def op_update(self, content, headers, msg):
        """
        @note A data object id is a topic that can be subscribed
        to/conversation. There needs to be a way to enforce an affinity
        (with in some messaging domain) between object operations and
        IObjectStore service process instances.
        """

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        @brief The content should be a 'cas content' id
        @note The backend decodes right before we re-encode. Is this
        necessary?
        Ans: cas.BaseObject has a cache for this encoding step
        """
        id = content
        try:
            obj = yield self.objstore.get(id)
            #data = serializer.encode(obj)
            data = obj.encode()
            yield self.reply_ok(msg, data)
        except cas.CAStoreError, e:
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks
    def op_put(self, content, headers, msg):
        """
        """
        id = content['id']
        data = content['data']
        try:
            obj = self.objstore.decode(data)
            id_local = yield self.objstore.put(obj)
            if not id == id_local:
                yield self.reply_err(msg, 'Content inconsistency')
            yield self.reply_ok(msg, id)
        except cas.CAStoreError, e:
            yield self.reply_err(msg, e)




class ObjectStoreClient(ServiceClient, cas.CAStore):
    """
    The client end of Distributed ObjectStore that presents the same
    interface as the actual ObjectStore.

    The main variable is the name and sys-name of the service. All else
    should be invariant.

    This is where a caching/mirroring-backend layer can exist.
    """

    interface.implements(objstore.IObjectStore, cas.ICAStore)

    objectChassis = objstore.ObjectChassis

    def ObjectChassis(self):
        """
        @brief Factory for creating an active object chassis without
        explicitly specifying its context (object name/id).
        @retval objectChassis instance that uses this ObjectStore instance.

        @note clone and create are the only IObjectStore methods that
        should be used directly. get and put are use by IObjectChassis...
        """
        return self.objectChassis(self)

    @defer.inlineCallbacks
    def create(self, name, baseClass):
        """
        @brief Create a new DataObject with the structure of baseClass.
        @param name Unique identifier of data object.
        @param baseClass DataObject class.
        @retval defer.Deferred that succeeds with an instance of
        objectChassis.
        @todo Change name to id
        """
        encoded_baseClass = dataobject.DEncoder().encode(baseClass())

        (content, headers, msg) = yield self.rpc_send('create', [name, encoded_baseClass])
        #obj = content['value']
        obj = content
        defer.returnValue(obj)


    @defer.inlineCallbacks
    def clone(self, name):
        """
        @brief Pull data object out of distributed store into local store.
        @param name Unique identifier of data object.
        @retval defer.Deferred that succeeds with an instance of
        objectChassis.
        @todo Change name to id.
        """
        (content, headers, msg) = yield self.rpc_send('clone', name)
        #obj = content['value']
        obj = content
        defer.returnValue(obj)


    def get(self, id):
        """
        @brief get content object.
        @param id of content object.
        @retval defer.Deferred that fires with an object that provides
        cas.ICAStoreObject.
        """
        d = self.rpc_send('get', id)
        d.addCallback(self._get_callback, id)
        d.addErrback(self._get_errback)
        return d

    def _get_callback(self, (content, headers, msg), id):
        """
        """
        #data = content['value'] # This should not be content['value']
        data = content # This should not be content['value']
        """@todo make 'ok' a bool instead
        """
        obj = self.decode(data)
        if not id == cas.sha1(obj, bin=False):
            raise cas.CAStoreError("Object Integrity Error!")
        return obj

    def _get_errback(self, reason):
        try:
            reason.raiseException()
        except ReceivedError, re:
            raise cas.CAStoreError("not found")
        return reason

    def put(self, obj):
        """
        @brief Write a content object to the store.
        @param obj instance providing cas.ICAStoreObject.
        @retval defer.Deferred that fires with the obj id.
        """
        data = obj.encode()
        id = cas.sha1(data, bin=False)
        d = self.rpc_send('put', {'id':id, 'data':data})
        d.addCallback(self._put_callback)
        #d.addErrback
        return d


    def _put_callback(self, (content, headers, msg)):
        """
        """
        #id = content['value']
        id = content
        return id

factory = ProcessFactory(ObjectStoreService)
