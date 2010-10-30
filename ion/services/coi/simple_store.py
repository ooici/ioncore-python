
from twisted.internet import defer

from ion.core.process import service_process
from ion.data import store
from ion.data.backends import cassandra

class KeyValueStoreService(service_process.ServiceProcess):
    declare = service_process.ServiceProcess.service_declare(name='keyvaluestore',
                                                            version='0',
                                                            dependencies=[])

    def slc_init(self):
        """
        """
        

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
