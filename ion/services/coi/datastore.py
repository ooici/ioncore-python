#!/usr/bin/env python

"""
@file ion/services/coi/datastore.py
@author David Stuebe

@TODO
use persistent key:value store in work bench to persist push and get pull!
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.services.dm.preservation import store


class DataStoreService(ServiceProcess):
    """
    The data store is not yet persistent. At the moment all its stored objects
    are kept in a python dictionary, part of the work bench. This service will
    be modified to use a persistent store - a set of cache instances to which
    it will dump data from push ops and retrieve data for pull and fetch ops.
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='datastore',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        #assert isinstance(backend, store.IStore)
        #self.backend = backend
        ServiceProcess.__init__(self, *args, **kwargs)
        
        self.op_push = self.workbench.op_push
        self.op_pull = self.workbench.op_pull
        #self.op_clone = self.workbench.op_clone
        self.op_fetch_linked_objects = self.workbench.op_fetch_linked_objects

        self.push = self.workbench.push
        self.pull = self.workbench.pull
        #self.clone = self.workbench.clone
        self.fetch_linked_objects = self.workbench.fetch_linked_objects

        log.info('DataStoreService.__init__()')
        

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

#
#
#class DataStoreServiceClient(ServiceClient):
#    """
#    This is an exemplar service client that calls the hello service. It
#    makes service calls RPC style.
#    """
#    def __init__(self, proc=None, **kwargs):
#        if not 'targetname' in kwargs:
#            kwargs['targetname'] = "datastore"
#        ServiceClient.__init__(self, proc, **kwargs)
#
#    @defer.inlineCallbacks
#    def hello(self, text='Hi there'):
#        yield self._check_init()
#        (content, headers, msg) = yield self.rpc_send('hello', text)
#        log.info('Service reply: '+str(content))
#        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProcessFactory(DataStoreService)



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
