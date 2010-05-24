#!/usr/bin/env python

"""
@file ion/services/cei/provisioner.py
@author Michael Meisinger
@author Alex Clemesha
@brief Starts, stops, and tracks instance and context state.
"""

import logging
import uuid
from twisted.internet import defer, reactor

from ion.services.base_service import BaseService
from ion.core import base_process
from ion.core.base_process import ProtocolFactory
from ion.services.cei.dtrs import DeployableTypeRegistryClient
from ion.services.cei.provisioner_store import ProvisionerStore
from ion.services.cei import states

class ProvisionerService(BaseService):
    """Provisioner service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='provisioner', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.store = ProvisionerStore()
    
    @defer.inlineCallbacks
    def op_provision(self, content, headers, msg):
        """Service operation: Provision a taskable resource
        """
        logging.info("op_provision content:"+str(content))
        
        document, launch_record, node_records = yield self._expand_request(content)
        
        #eventually should have some kind of bulk insert
        self.store.put_state(launch_record['launch_id'], 
                launch_record['state'], launch_record)
        for record in node_records:
            self.store.put_state(record['node_id'], record['state'], record)

        # now we can ACK the request as it is safe in datastore

        # set up a callLater to fulfill the request after the ack. Would be
        # cleaner to have explicit ack control.
        reactor.callLater(0, self._fulfill_launch, document, launch_record, node_records)

    def _fulfill_launch(self, document, launch, nodes):
        logging.info('Time to fulfill this request, kids.')
        
    @defer.inlineCallbacks
    def _expand_request(self, request):
        """Validates request and transforms it into a document and node records.

        Returns a tuple (document, launch record, node records).
        """
        try:
            deployable_type = request['deployable_type']
            nodes = request['nodes']
            launch_id = request['launch_id']
            subscribers = request['subscribers'] #what will this look like?
        except KeyError:
            logging.error('Request was malformed')
            #TODO error handling, what?
            yield defer.fail()

        #TODO how to do this lookup once for the service?
        dtrs_id = yield base_process.procRegistry.get("dtrs")
        dtrs = DeployableTypeRegistryClient(pid=dtrs_id)

        dt = yield dtrs.lookup(deployable_type, nodes)

        doc = dt['document']
        node_groups = dt['nodes']

        launch_record = _one_launch_record(launch_id, deployable_type,
                subscribers)

        node_records = []
        for (group_name, group) in node_groups.iteritems():
            node_ids = group['id']
            for node_id in node_ids:
                node_records.append(_one_node_record(node_id, group, 
                    group_name, launch_record))
        result = (doc, launch_record, node_records)
        yield defer.returnValue(result)

    def op_terminate(self, content, headers, msg):
        """Service operation: Terminate a taskable resource
        """
        pass

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
         

def _one_launch_record(launch_id, dt, subscribers, state=states.Requested):
    return {'launch_id' : launch_id,
            'deployable_type' : dt,
            'subscribers' : subscribers,
            'state' : state,
            }

def _one_node_record(node_id, group, group_name, launch, state=states.Requested):
    return {'launch_id' : launch['launch_id'],
            'node_id' : node_id,
            'state' : state,
            'state_desc' : None,
            'site' : group['site'],
            'allocation' : group['allocation'],
            'ctx-name' : group_name,
            }
        
# Spawn of the process using the module name
factory = ProtocolFactory(ProvisionerService)
