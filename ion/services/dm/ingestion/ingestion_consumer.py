#!/usr/bin/env python



"""
@file ion/services/dm/distribution/consumers/forwarding_consumer.py
@author David Stuebe
@brief The ingestions consumer examines metadata from a stream and updates
the data registry with that content.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.services.dm.distribution import base_consumer
from ion.core.process.process import ProcessFactory
from ion.services.dm.inventory import data_registry
from ion.data import dataobject
try:
    import json
except:
    import simplejson as json


class IngestionConsumer(base_consumer.BaseConsumer):
    """
    ingest data from a stream and update registry
    """
    @defer.inlineCallbacks
    def customize_consumer(self):
        log.info('Setting up Ingestion Consumer')
        
        ref_str = self.params.get('data_resource_ref')
        enc_ref = json.loads(ref_str)
        
        self.params['data_resource_ref'] = dataobject.DataObject.decode(enc_ref)
        self.datareg = yield data_registry.DataRegistryClient(proc=self)

    #@defer.inlineCallbacks
    def ondata(self, data, notification, timestamp, data_resource_ref):
        '''
        examine data
        extract meta data
        get data resource
        put updated data resource
        '''
        pass
    
    
    

# Spawn of the process using the module name
factory = ProcessFactory(IngestionConsumer)



