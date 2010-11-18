"""
Prototype and example service using new Cassandra based Store backend.

To make a generic Store service (an ion service that provides the IStore
interface) the choosing and configuring of the backend client must:
    a) accomplished without editing this file 
    b) the client must be triggered to start using a Connect or Listen
    reactor method as exposed through the ion.core.process.process.Process
    class

A specific service definition probably shouldn't be part of a library
"""
from twisted.internet import defer

from ion.core.process import service_process
from ion.data import store
from ion.data.backends import cassandra

class KeyValueStoreService(service_process.ServiceProcess):
    """
    An Ion Service that provides the IStore interface.

    All ion service processes have a service life cycle..
    This service relies on a backend client that also has a life cycle.
    The backend client requires an initialization step, and an activation
    step. The initialization is where appropriate configuration parameters
    are provided. The parameters can vary depending on the backend
    technology/implementation.
    The general pattern regardless of backend are the two steps.
    """

    storeFactory = cassandra.CassandraFactory

    declare = service_process.ServiceProcess.service_declare(name='keyvaluestore',
                                                            version='0',
                                                            dependencies=[])

    def slc_init(self):
        """
        use spawn args to initialize cassandra Store client

        Here, the service needs to ensure the connection to the local
        cassandra server is usable when the slc_init phase is complete.

        The telephus client uses a reconnecting factory. It will be
        important to know when the client is in a error state and not just
        in a perpetual state of reconnecting.
        """
        host = self.spawn_args["host"]
        port = self.spawn_args["port"]
        namespace = self.spawn_args["namespace"]
        process = self
        store_factory = self.storeFactory(host, port, namespace)
        # The buildStore method uses connectTCP, and therefore is not a
        # deferred function. This means there is not a guarantee the store
        # will be connected by the time messages arrive to the service.
        self.store = store_factory.buildStore(process) #Not sure if this is the best way
                                          #pass in the process instance;
                                          #trying it for now. Comments
                                          #welcome
    def slc_terminate(self):
        """hack to accommodate cassandra reconnecting connection manager
        """
        self.store.manager.shutdown()

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        key = content['key']

        try:
            value = yield self.store.get(key)
        except KeyError, e:
            self.reply_err(msg, 'KeyError')
        # Service Process is handeling one message at a time, so you dont
        # need the msg object for reply. the process can infer who to reply
        # to becasue it knows the current context, and it knows the
        # convention that the reply to address is in the header of the
        # current msg (else, reply errors). 
        self.reply_ok(msg, value)

    @defer.inlineCallbacks
    def op_put(self, content, headers, msg):
        key, value = content['key'], content['value']

        yield self.store.put(key, value)
        # shouldn't need to reply for a simple put
        # Does an error here propogate to the next handler?

@defer.inlineCallbacks
def get(key):
    request = {'key':key}
    value = yield rpc_send('keyvaluestore', request)
    defer.returnValue(value)
    

class KeyValueStoreClient(service_process.ServiceClient):

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'keyvaluestore'
        service_process.ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def get(self, key):
        request = {'key':key}
        (content, headers, msg) = yield self.rpc_send('get', request)
        value = content['value']
        defer.returnValue(value)
    
    @defer.inlineCallbacks
    def put(self, key, value):
        request = {'key':key, 'value':value}
        yield self.send('put', request)
