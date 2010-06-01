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
import uuid
from itertools import izip
from twisted.internet import defer, reactor, threads

from nimboss.node import NimbusNodeDriver
from nimboss.ctx import ContextClient
from nimboss.cluster import ClusterDriver
from nimboss.nimbus import NimbusClusterDocument
from libcloud.types import NodeState as NimbossNodeState

from ion.services.base_service import BaseService
from ion.core import base_process
from ion.core.base_process import ProtocolFactory
from ion.services.cei.dtrs import DeployableTypeRegistryClient
from ion.services.cei.provisioner_store import ProvisionerStore, group_records
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

    @defer.inlineCallbacks
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
         
_NIMBOSS_STATE_MAP = {
        #TODO when broker is polled, these states will end at Started
        NimbossNodeState.RUNNING : states.Running, 
        NimbossNodeState.REBOOTING : states.Running, #TODO hmm
        NimbossNodeState.PENDING : states.Pending,
        NimbossNodeState.TERMINATED : states.Terminated,
        NimbossNodeState.UNKNOWN : states.ErrorRetrying}

class ProvisionerCore(object):
    """Provisioner functionality that is not specific to the service.
    """


    def __init__(self, store, notifier):
        self.store = store
        self.notifier = notifier

        #TODO how about a config file
        nimbus_key = os.environ['NIMBUS_KEY']
        nimbus_secret = os.environ['NIMBUS_SECRET']
        nimbus_test_driver = NimbusNodeDriver(nimbus_key, secret=nimbus_secret,
                host='nimbus.ci.uchicago.edu', port=8444)

        self.node_drivers = {'nimbus-test' : nimbus_test_driver}
        
        self.ctx_client = ContextClient(
                'https://nimbus.ci.uchicago.edu:8888/ContextBroker/ctx/', 
                nimbus_key, nimbus_secret)
        self.cluster_driver = ClusterDriver(self.ctx_client)

    @defer.inlineCallbacks
    def expand_request(self, request):
        """Validates request and transforms it into launch and node records.

        Returns a tuple (launch record, node records).
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

        launch_record = _one_launch_record(launch_id, doc, deployable_type,
                subscribers)

        node_records = []
        for (group_name, group) in node_groups.iteritems():
            node_ids = group['id']
            for node_id in node_ids:
                node_records.append(_one_node_record(node_id, group, 
                    group_name, launch_record))
        result = (launch_record, node_records)
        yield defer.returnValue(result)
    
    @defer.inlineCallbacks
    def fulfill_launch(self, launch, nodes):
        """Follows through on bringing a launch to the Pending state
        """

        subscribers = launch['subscribers']
        docstr = launch['document']
        doc = NimbusClusterDocument(docstr)
        
        launch_groups = group_records(nodes, 'ctx_name')
        
        context = None
        if doc.needs_contextualization:
            context = yield threads.deferToThread(self._create_context)
            logging.info('Created new context: ' + context.uri)
            
            launch['context'] = context
            #could have special launch states?
            yield self.store.put_record(launch, states.Pending)
        
        cluster = self.cluster_driver.new_bare_cluster(context.uri)
        specs = doc.build_specs(context)

        #assumption here is that a launch group does not span sites or
        #allocations. That may be a feature for later.

        for spec in specs:
            launch_group = launch_groups[spec.name]
            site = launch_group[0]['site']
            driver = self.node_drivers[site]

            logging.info('Launching group '+spec.name)
            
            iaas_nodes = yield threads.deferToThread(
                    self.cluster_driver.launch_node_spec, spec, driver)
            
            # TODO so many failure cases missing
            
            cluster.add_node(iaas_nodes)

            # underlying node driver may return a list or an object
            if not hasattr(iaas_nodes, '__iter__'):
                iaas_nodes = [iaas_nodes]
                
            for node_rec, iaas_node in izip(launch_group, iaas_nodes):
                node_rec['iaas_id'] = iaas_node.id
                node_rec['public_ip'] = iaas_node.public_ip
                node_rec['private_ip'] = iaas_node.private_ip
                node_rec['extra'] = iaas_node.extra.copy()
                node_rec['state'] = states.Pending
            
            yield self.store_and_notify(launch_group, subscribers)
    
    @defer.inlineCallbacks
    def store_and_notify(self, records, subscribers, newstate=None):
        """Convenience method to store records and notify subscribers.
        """
        yield self.store.put_records(records, newstate)
        yield self.notifier.send_records(records, subscribers)

    @defer.inlineCallbacks
    def query_nodes(self, request):
        """Performs querys of IaaS and broker, sends updates to subscribers.
        """
        # Right now we just query everything. Could be made more granular later

        for site in self.node_drivers.iterkeys():
            nodes = yield self.store.get_site_nodes(site, 
                    before_state=states.Terminated)
            if nodes:
                yield self.query_one_site(site, nodes)


    @defer.inlineCallbacks
    def query_one_site(self, site, nodes):
        node_driver = self.node_drivers[site]

        logging.info('Querying site "%s"', site)
        nimboss_nodes = yield threads.deferToThread(node_driver.list_nodes)
        nimboss_nodes = dict((node.id, node) for node in nimboss_nodes)

        # note we are walking the nodes from datastore, NOT from nimboss
        for node in nodes:
            state = node['state']
            if state < states.Pending or state > states.Terminated:
                continue
            
            nimboss_id = node['iaas_id']
            nimboss_node = nimboss_nodes.pop(nimboss_id, None)
            if not nimboss_node:
                # this state is unknown to underlying IaaS. What could have
                # happened? IaaS error? Recovery from loss of net to IaaS?

                logging.warn('node %s: in data store but unknown to IaaS. '+
                        'Marking as terminated.', node['node_id'])

                # we must mark it as terminated TODO should this be failed?
                node['state'] = states.Terminated
                #TODO should make state_desc more formal?
                node['state_desc'] = 'node disappeared'

                launch = yield self.store.get_launch(node['launch_id'])
                yield self.store_and_notify([node], launch['subscribers'])
            nimboss_state = _NIMBOSS_STATE_MAP[nimboss_node.state]
            if nimboss_state > node['state']:
                #TODO nimboss could go backwards in state.
                node['state'] = nimboss_state
                launch = yield self.store.get_launch(node['launch_id'])
                yield self.store_and_notify([node], launch['subscribers'])
        #TODO nimboss_nodes now contains any other running instances that
        # are unknown to the datastore (or were started after the query)
        # Could do some analysis of these nodes

    def _create_context(self):
        """Synchronous call to context broker.
        """
        return self.ctx_client.create_context()



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

def _one_launch_record(launch_id, document, dt, subscribers, 
        state=states.Requested):
    return {'launch_id' : launch_id,
            'document' : document,
            'deployable_type' : dt,
            'subscribers' : subscribers,
            'state' : state,
            }

def _one_node_record(node_id, group, group_name, launch, 
        state=states.Requested):
    return {'launch_id' : launch['launch_id'],
            'node_id' : node_id,
            'state' : state,
            'state_desc' : None,
            'site' : group['site'],
            'allocation' : group['allocation'],
            'ctx_name' : group_name,
            }
        
# Spawn of the process using the module name
factory = ProtocolFactory(ProvisionerService)
