#!/usr/bin/env python

"""
@file ion/services/cei/provisioner.py
@author Michael Meisinger
@author Alex Clemesha
@author David LaBissoniere
@brief Starts, stops, and tracks instance and context state.
"""

import os
import logging
from twisted.internet import defer, reactor

from ion.services.base_service import BaseService
from ion.core import base_process
from ion.core.base_process import ProtocolFactory
from ion.services.cei.provisioner_store import ProvisionerStore
from ion.services.cei.provisioner_core import ProvisionerCore
from ion.services.cei import states

class ProvisionerService(BaseService):
    """Provisioner service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='provisioner', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.store = ProvisionerStore()
        self.notifier = ProvisionerNotifier(self)
        self.core = ProvisionerCore(self.store, self.notifier)
    
    @defer.inlineCallbacks
    def op_provision(self, content, headers, msg):
        """Service operation: Provision a taskable resource
        """
        logging.info("op_provision content:"+str(content))
        
        launch, nodes = yield self.core.expand_request(content)
        
        self.store.put_record(launch, states.Requested)
        self.store.put_records(nodes, states.Requested)

        subscribers = launch['subscribers']
        if subscribers:
            self.notifier.send_records(nodes, subscribers)

        # now we can ACK the request as it is safe in datastore

        # set up a callLater to fulfill the request after the ack. Would be
        # cleaner to have explicit ack control.
        reactor.callLater(0, self.core.fulfill_launch, launch, nodes)

    def op_terminate(self, content, headers, msg):
        """Service operation: Terminate a taskable resource
        """
        pass

    def op_query(self, content, headers, msg):
        """Service operation: query IaaS  and send updates to subscribers.
        """
        # immediate ACK is desired
        reactor.callLater(0, self.core.query_nodes, content)
    
    def _get_iaas_info(self, iaas_service):
        """
        Get information from 'iaas_service' about
        the state of all currently ownded VM instances.

        @todo: Actually communicate wth IAAS service. 
        """
        #XXX replace below with "real structure" of message to Sensor Aggregator.
        return {"iaas_service":iaas_service, "x":1, "y":2, "z":3}

    @defer.inlineCallbacks
    def send_iaas_notification(self):
        """Sends out IAAS information.

        (Currently only intended to send IAAS info to SensorAggregator)
        """
        iaas_info = self._get_iaas_info("ec2")
        sa = yield base_process.procRegistry.get("sensor_aggregator")
        result = yield self.rpc_send(sa, "sensor_aggregator_info", iaas_info, {})
        content, headers, msg = result

    def op_receipt_taken(self, content, headers, msg):
        logging.info("Receipt has been taken.  content:"+str(content))

    @defer.inlineCallbacks
    def op_cei_test(self, content, headers, msg):
        sa = yield self.get_scoped_name('system', 'sensor_aggregator')
        yield self.send(sa, 'cei_test', content)
        

class ProvisionerNotifier(object):
    """Abstraction for sending node updates to subscribers.
    """
    def __init__(self, process):
        self.process = process

    @defer.inlineCallbacks
    def send_record(self, record, subscribers, operation='node_update'):
        """Send a single node record to all subscribers.
        """
        logging.debug('Sending status record about node %s to %s', 
                record['node_id'], repr(subscribers))
        for sub in subscribers:
            yield self.process.send(sub, operation, record)

    @defer.inlineCallbacks
    def send_records(self, records, subscribers, operation='node_update'):
        """Send a set of node records to all subscribers.
        """
        for rec in records:
            yield self.send_record(rec, subscribers, operation)

# Spawn of the process using the module name
factory = ProtocolFactory(ProvisionerService)
