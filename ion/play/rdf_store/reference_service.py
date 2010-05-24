#!/usr/bin/env python

"""
@file ion/play/rdf_store/reference_service.py
@author David Stuebe
@brief  RDF Store: reference service
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.data.dataobject import DataObject
from ion.data.store import Store
from ion.data.objstore import ValueObject

class ReferenceService(BaseService):
    """Example service implementation
    """

    # Declaration of service
    declare = BaseService.service_declare(name='references', version='0.1.0', dependencies=[])

#    def __init__(self, receiver, spawnArgs=None):
#        BaseService.__init__(self, receiver, spawnArgs)
#        logging.info('HelloService.__init__()')

    def slc_init(self):
        self.kss={}

    @defer.inlineCallbacks
    def op_add_reference(self, content, headers, msg):
        '''
        '''
        logging.info('op_add_reference: '+str(content))        
        key = content['key']
        reference = content['reference']
        
        if key in self.kss:
            self.kss[key].add(reference)
        else:
            self.kss[key]=set([reference])

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', {'Referenced Key':key}, {})

    @defer.inlineCallbacks
    def op_get_references(self, content, headers, msg):
        '''
        '''
        logging.info('op_get_references: '+str(content)) 
        key = content['key']
        ref=self.kss[key]
        ref=list(ref)
        yield self.reply(msg, 'reply', {'references':ref}, {})

    @defer.inlineCallbacks
    def op_del_reference(self, content, headers, msg):
        '''
 
        '''
        key = content['key']
        reference = content['reference']
        
        self.kss[key].discard(reference)
        
        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', {'result':'success'}, {})


class ReferenceServiceClient(BaseServiceClient):
    """
    This is an exemplar service class that calls the hello service. It
    applies the RPC pattern.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "references"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def add_reference(self, key,reference):
        '''
        @param reference A DataObject reference to be stored
        '''
        yield self._check_init()

        kd={'key':key,'reference':reference} 

        (content, headers, msg) = yield self.rpc_send('add_reference', kd)
        logging.info('reference Servie Client: put_reference: '+str(content))
        defer.returnValue(content['Referenced Key'])
        
    @defer.inlineCallbacks
    def get_references(self, key):
        '''
        @param key, A key for a stored reference
        '''
        yield self._check_init()
        
        # assert ?
        kd={'key':key} 
        (content, headers, msg) = yield self.rpc_send('get_references', kd)
        logging.info('reference Servie Client: get_references: '+str(content))
        defer.returnValue(content['references'])


    @defer.inlineCallbacks
    def del_reference(self, key,reference):
        '''
        @param key, A key for a stored reference
        '''
        yield self._check_init()
        
        # assert ?
        kd={'key':key,'reference':reference} 
        (content, headers, msg) = yield self.rpc_send('del_reference', kd)
        logging.info('reference Servie Client: del_reference: '+str(content))
        defer.returnValue(content['result'])        

# Spawn of the process using the module name
factory = ProtocolFactory(ReferenceService)



