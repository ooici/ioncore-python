#!/usr/bin/env python

"""
@file ion/services/cei/provisioner_core.py
@author David LaBissoniere
@brief Starts, stops, and tracks instance and context state.
"""

import os
import logging
logging = logging.getLogger(__name__)
from itertools import izip
from twisted.internet import defer, threads

from nimboss.node import NimbusNodeDriver
from nimboss.ctx import ContextClient
from nimboss.cluster import ClusterDriver
from nimboss.nimbus import NimbusClusterDocument
from libcloud.types import NodeState as NimbossNodeState
from libcloud.base import Node as NimbossNode
from libcloud.drivers.ec2 import EC2NodeDriver
from ion.core import base_process
from ion.services.cei.dtrs import DeployableTypeRegistryClient
from ion.services.cei.provisioner_store import group_records
from ion.services.cei import states

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
        nimbus_uc_driver = NimbusNodeDriver(nimbus_key, secret=nimbus_secret,
                host='tp-vm1.ci.uchicago.edu', port=8445)
       
        ec2_key = os.environ['AWS_ACCESS_KEY_ID']
        ec2_secret = os.environ['AWS_SECRET_ACCESS_KEY']
        ec2_east_driver = EC2NodeDriver(ec2_key, ec2_secret)

        self.node_drivers = {
                'nimbus-test' : nimbus_test_driver,
                'nimbus-uc' : nimbus_uc_driver,
                'ec2-east' : ec2_east_driver,
                }
        
        self.ctx_client = ContextClient(
                'https://nimbus.ci.uchicago.edu:8888/ContextBroker/ctx/', 
                nimbus_key, nimbus_secret)
        self.cluster_driver = ClusterDriver(self.ctx_client)

    @defer.inlineCallbacks
    def expand_provision_request(self, request):
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

            logging.info('Launching group %s - %s nodes', spec.name, spec.count)
            
            iaas_nodes = yield threads.deferToThread(
                    self.cluster_driver.launch_node_spec, spec, driver, 
                    ex_keyname='ooi')
            
            # TODO so many failure cases missing
            
            cluster.add_node(iaas_nodes)

            # underlying node driver may return a list or an object
            if not hasattr(iaas_nodes, '__iter__'):
                iaas_nodes = [iaas_nodes]

            if len(iaas_nodes) != len(launch_group):
                logging.error('%s nodes from iaas but %s from datastore', 
                        len(iaas_nodes), len(launch_group))
                
            for node_rec, iaas_node in izip(launch_group, iaas_nodes):
                node_rec['iaas_id'] = iaas_node.id
                # for some reason, ec2 libcloud driver places IP in a list
                #TODO if we support drivers that actually have multiple
                #public and private IPs, we will need to revist this
                public_ip = iaas_node.public_ip
                if isinstance(public_ip, list):
                    public_ip = public_ip[0]
                private_ip = iaas_node.private_ip
                if isinstance(private_ip, list):
                    private_ip = private_ip[0]
                node_rec['public_ip'] = public_ip
                node_rec['private_ip'] = private_ip
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

        yield self.query_contexts()

    @defer.inlineCallbacks
    def query_one_site(self, site, nodes):
        node_driver = self.node_drivers[site]

        logging.info('Querying site "%s"', site)
        nimboss_nodes = yield threads.deferToThread(node_driver.list_nodes)
        nimboss_nodes = dict((node.id, node) for node in nimboss_nodes)

        # note we are walking the nodes from datastore, NOT from nimboss
        for node in nodes:
            state = node['state']
            if state < states.Pending or state >= states.Terminated:
                continue
            
            nimboss_id = node.get('iaas_id')
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
            else:
                nimboss_state = _NIMBOSS_STATE_MAP[nimboss_node.state]
                if nimboss_state > node['state']:
                    #TODO nimboss could go backwards in state.
                    node['state'] = nimboss_state
                    launch = yield self.store.get_launch(node['launch_id'])
                    yield self.store_and_notify([node], launch['subscribers'])
        #TODO nimboss_nodes now contains any other running instances that
        # are unknown to the datastore (or were started after the query)
        # Could do some analysis of these nodes

    @defer.inlineCallbacks
    def query_contexts(self):
        """Queries all open launch contexts and sends node updates.
        """
        #grab all the launches in the pending state
        launches = yield self.store.get_launches(state=states.Pending)

        for launch in launches:
            context = launch.get('context')
            launch_id = launch['launch_id']
            if not context:
                logging.warn('Launch %s is in %s state but it has no context!',
                        launch['launch_id'], launch['state'])
                continue
            
            ctx_uri = context['uri']
            logging.debug('Querying context ' + ctx_uri)
            context_status = yield threads.deferToThread(self._query_context,
                    ctx_uri)
            nodes = yield self.store.get_launch_nodes(launch_id)
            by_ip = group_records(nodes, 'public_ip')
            #TODO this matching could probably be more robust
            updated_nodes = []
            for ctx_node in context_status.nodes:
                for id in ctx_node.identities:
                    node = by_ip.get(id.ip)
                    if node and _update_node_from_ctx(node, ctx_node, id):
                        updated_nodes.append(node)
                        break
            if updated_nodes:
                yield self.store_and_notify(updated_nodes)
            
            if context_status.complete:
                logging.info('Launch %s context is complete!', launch_id)
                # update the launch record so this context won't be re-queried
                launch['state'] = states.Running
                yield self.store.put_record(launch)
            else:
                logging.debug('Launch %s context is incomplete: %s of %s nodes',
                        launch_id, len(context_status.nodes), 
                        context_status.expected_count)
    
    @defer.inlineCallbacks
    def mark_launch_terminating(self, launch_id):
        """Mark a launch as Terminating in data store.
        """
        launch = yield self.store.get_launch(launch_id)
        nodes = yield self.store.get_launch_nodes(launch_id)
        yield self.store_and_notify(nodes, launch['subscribers'], 
                states.Terminating)
    
    @defer.inlineCallbacks
    def terminate_launch(self, launch_id):
        """Destroy all nodes in a launch and mark as terminated in store.
        """
        launch = yield self.store.get_launch(launch_id)
        nodes = yield self.store.get_launch_nodes(launch_id)

        for node in nodes:
            state = node['state']
            if state < states.Pending or state >= states.Terminated:
                continue
            #would be nice to do this as a batch operation
            nimboss_node = self._to_nimboss_node(node)
            driver = self.node_drivers[node['site']]
            yield threads.deferToThread(driver.destroy_node, nimboss_node)
        
            yield self.store_and_notify([node], launch['subscribers'], 
                    states.Terminated)

    @defer.inlineCallbacks
    def terminate_launches(self, launch_ids):
        """Destroy all node in a set of launches.
        """
        for launch in launch_ids:
            yield self.terminate_launch(launch)

    def _create_context(self):
        """Synchronous call to context broker to create a new context.
        """
        return self.ctx_client.create_context()

    def _query_context(self, resource):
        """Synchronous call to context broker to query an existing context
        """
        return self.ctx_client.get_status(resource)
    
    def _to_nimboss_node(self, node):
        """Nimboss drivers need a Node object for termination.
        """
        #TODO this is unfortunately tightly coupled with EC2 libcloud driver
        # right now. We are building a fake Node object that only has the
        # attribute needed for termination (id). Would need to be fleshed out
        # to work with other drivers.
        return NimbossNode(id=node['iaas_id'], name=None, state=None,
                public_ip=None, private_ip=None, 
                driver=self.node_drivers[node['site']])

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

def _update_node_from_ctx(self, node, ctx_node, identity):
    node_done = ctx_node.ok_occurred or ctx_node.error_occurred
    if not node_done or node['state'] >= states.Running:
        return False
    if ctx_node.ok_occurred:
        node['state'] = states.Running
        node['pubkey'] = identity.pubkey
    else:
        node['state'] = states.Failed
        node['error_code'] = ctx_node.error_code
        node['error_message'] = ctx_node.error_message
    return True

