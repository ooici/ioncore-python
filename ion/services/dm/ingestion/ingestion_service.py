#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/ingestion_service.py
@author Michael Meisinger
@brief service for ingesting information into DM
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.dm.ingestion import ingestion_registry
from ion.services.dm.distribution import pubsub_service
from ion.services.dm.preservation import preservation_service
from ion.services.dm.inventory import data_registry

from ion.data import dataobject

from ion.resources.dm_resource_descriptions import IngestionStreamResource
from ion.resources.dm_resource_descriptions import PubSubTopicResource
from ion.resources.dm_resource_descriptions import DMDataResource
from ion.resources.dm_resource_descriptions import ArchiveResource

class IngestionService(BaseService):
    """
    Ingestion service interface
    @note Needs work - Should create a subscription to ingest a data source
    @todo What should the service interface look like for this?
    """

    # Declaration of service
    declare = BaseService.service_declare(name='ingestion_service',
                                          version='0.1.0',
                                          dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        self.reg = yield ingestion_registry.IngestionRegistryClient(proc=self)
        self.pubsub = yield pubsub_service.DataPubsubClient(proc=self)
        self.preserv = yield preservation_service.PreservationClient(proc=self)
        self.datareg = yield data_registry.DataRegistryClient(proc=self)

    @defer.inlineCallbacks
    def op_create_ingestion_datastream(self, content, headers, msg):
        """Service operation: declare new named datastream for ingestion

        0) Decode content to Igestion Stream Resource
        1) create inbound topic
        2) create ingested topic
        3) Register new data
        3) start preservation of ingested
        4) start preservation of inbound
        5) start ingestion workflow

        Register progress and set lcstate along the way

        return the ingestiondatastream resource

        """
        log.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' +  str(headers))
        isr = dataobject.DataObject.decode(content)
        log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', Ingestion Stream: \n' + str(publisher))

        # Register the intended feed...
        isr = yield self.reg.define_ingestion_stream(isr)

        inbnd = irs.name + '.inbound'
        topic = PubSubTopicResource.create(name=inbnd,keywords='input') # Place holder - what to put, NOT RAW!
        topic = yield self.pubsub.define_topic(topic)
        isr.input_topic = topic.reference(heat=True)

        ingested = irs.name + '.ingested'
        topic = PubSubTopicResource.create(name=ingested,keywords='ingested')
        topic = yield self.pubsub.define_topic(topic)
        isr.ingested_topic = topic.reference(heat=True)

        # Register the feed topics...
        isr = yield self.reg.define_ingestion_stream(isr)

        #Create a DM Data Resource for this new feed.
        dmdr = DMDataResource.create_new_resource()
        dmdr.input_topic = isr.input_topic
        dmdr.ingested_topic = isr.ingested_topic

        # Register the data
        dmdr = yield self.datareg.define_data(dmdr)


        #Call the preservation service for the input stream
        arc = ArchiveResource.create_new_resource()
        arc.dmdataresource = dmdr.reference(head=true)
        arc.topic = isr.input_topic

        yield self.preserv.create_archive(arc)
        yield self.preserv.activate_persister

        # Register the data
        dmdr.input_archive = arc.reference(head=True)
        dmdr = yield self.datareg.define_data(dmdr)


        #Call the preservation service for the ingested stream
        arc = ArchiveResource.create_new_resource()
        arc.dmdataresource = dmdr.reference(head=true)
        arc.topic = isr.ingested_topic

        yield self.preserv.create_archive(arc)
        yield self.preserv.activate_persister

        # Register the data
        dmdr.ingested_archive = arc.reference(head=True)
        dmdr = yield self.datareg.define_data(dmdr)

        # Register the persisters
        isr.persisting_inout = True
        isr.persisting_ingested = True
        isr = yield self.reg.define_ingestion_stream(isr)

        #activate ingestion
        isr = yield self._activate_ingestion(isr, dmdr)
        isr = yield self.reg.define_ingestion_stream(isr)

        yield self.reply_ok(isr.encode())

    @defer.inlineCallbacks
    def _activate_ingestion(isr, data_resource):
        subscription = SubscriptionResource()
        subscription.topic1 = t_search

        # More to do - tell the ingester what to do with results - what dmdr to update etc...

        subscription.workflow = {'consumer1':{'module':'path.to.module','consumerclass':'<ConsumerClassName>',\
            'attach':'topic1',\
            'process parameters':{'param1':'my parameter'}}}

        #subscription = yield self.pubsub.create_consumer_args(subscription)

        isr.ingesting = True

        defer.returnValue(isr)


# Spawn of the process using the module name
factory = ProtocolFactory(IngestionService)



class IngestionClient(BaseServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'ingestion_service'
        BaseServiceClient.__init__(self, proc, **kwargs)

    def create_ingestion_datastream(self,isr):
        '''
        @brief create a new ingestion datastream
        @param name a string naming the new ingestion stream
        @return IngestionDataStreamResource object
        '''
