#!/usr/bin/env python

"""
@file ion/play/rdf_store/rdf_service.py
@package ion.play.rdf_store.rdf_service 
@author David Stuebe
@brief A service that provides git symantics for push pull commit and diff to
the rdf workspace composed of associations and objects.
The associations can be walked to find content.
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class DataStoreService(BaseService):
    """
    Example service interface
    """
    # Declaration of service
    declare = BaseService.service_declare(name='DataStoreService',
                                          version='0.1.0',
                                          dependencies=[])

    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        #self.rdfs=RdfStore()
        logging.info('DataStoreService.__init__()')

#    @defer.inlineCallbacks
    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        #yield self.rdfs.init()
        pass

    @defer.inlineCallbacks
    def op_push(self, content, headers, msg):
        logging.info('op_push: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})

    @defer.inlineCallbacks
    def op_pull(self, content, headers, msg):
        logging.info('op_pull: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})



class DataStoreServiceClient(BaseServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "DataStoreService"
        BaseServiceClient.__init__(self, proc, **kwargs)
        #self.rdfs=RdfStore()
        logging.info('DataStoreServiceClient.__init__()')


#    @defer.inlineCallbacks
    def slc_init(self):
#        yield self.rdfs.init()
        pass

    @defer.inlineCallbacks
    def push(self, repo_key):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('push', repo_key)
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def pull(self, repo_key):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('pull', repo_key)
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))



# Spawn of the process using the module name
factory = ProtocolFactory(DataStoreService)


