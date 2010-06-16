#!/usr/bin/env python
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer #, reactor

from ion.services.base_service import BaseService, BaseServiceClient
from ion.core.base_process import ProtocolFactory
from ion.services.cei.provisioner_store import ProvisionerStore
from ion.services.cei.provisioner_core import ProvisionerCore
from ion.services.cei.dtrs import DeployableTypeRegistryClient
from ion.services.cei import states

class ProvisionerService(BaseService):
    """Provisioner service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='provisioner', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.store = ProvisionerStore()
        self.notifier = ProvisionerNotifier(self)
        self.dtrs = DeployableTypeRegistryClient(self)
        self.core = ProvisionerCore(self.store, self.notifier, self.dtrs)
    
    @defer.inlineCallbacks
    def op_provision(self, content, headers, msg):
        """Service operation: Provision a taskable resource
        """
        logging.debug("op_provision content:"+str(content))
        
        launch, nodes = yield self.core.expand_provision_request(content)
        
        self.store.put_record(launch, states.Requested)
        self.store.put_records(nodes, states.Requested)

        subscribers = launch['subscribers']
        if subscribers:
            self.notifier.send_records(nodes, subscribers)

        # now we can ACK the request as it is safe in datastore

        # set up a callLater to fulfill the request after the ack. Would be
        # cleaner to have explicit ack control.
        #reactor.callLater(0, self.core.fulfill_launch, launch, nodes)
        yield self.core.fulfill_launch(launch, nodes)
    
    @defer.inlineCallbacks
    def op_terminate_nodes(self, content, headers, msg):
        """Service operation: Terminate one or more nodes
        """
        logging.debug('op_terminate_nodess content:'+str(content))

        #expecting one or more node IDs
        if not isinstance(content, list):
            content = [content]

        #TODO yield self.core.mark_nodes_terminating(content)

        #reactor.callLater(0, self.core.terminate_nodes, content)
        yield self.core.terminate_nodes(content)

    @defer.inlineCallbacks
    def op_terminate_launches(self, content, headers, msg):
        """Service operation: Terminate one or more launches
        """
        logging.debug('op_terminate_launches content:'+str(content))

        #expecting one or more launch IDs
        if not isinstance(content, list):
            content = [content]

        for launch in content:
            yield self.core.mark_launch_terminating(launch)

        #reactor.callLater(0, self.core.terminate_launches, content)
        yield self.core.terminate_launches(content)

    @defer.inlineCallbacks
    def op_query(self, content, headers, msg):
        """Service operation: query IaaS  and send updates to subscribers.
        """
        # immediate ACK is desired
        #reactor.callLater(0, self.core.query_nodes, content)
        yield self.core.query_nodes(content)
    
        
class ProvisionerClient(BaseServiceClient):
    """
    Client for provisioning deployable types
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "provisioner"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def provision(self, launch_id, deployable_type, launch_description):
        """Provisions a deployable type
        """
        yield self._check_init()

        nodes = {}
        for nodename, item in launch_description.iteritems():
            nodes[nodename] = {'id' : item.instance_ids,
                    'site' : item.site,
                    'allocation' : item.allocation_id,
                    'data' : item.data}
        sa = yield self.proc.get_scoped_name('system', 'sensor_aggregator')
        request = {'deployable_type' : deployable_type,
                'launch_id' : launch_id,
                'nodes' : nodes,
                'subscribers' : [sa]}
        logging.debug('Sending provision request: ' + str(request))
        yield self.send('provision', request)
        
    @defer.inlineCallbacks
    def query(self):
        """Triggers a query operation in the provisioner. Node updates
        are not sent in reply, but are instead sent to subscribers 
        (most likely a sensor aggregator).
        """
        yield self._check_init()
        logging.debug('Sending query request to provisioner')
        yield self.send('query', None)

    @defer.inlineCallbacks
    def terminate_launches(self, launches):
        """Terminates one or more launches
        """
        yield self._check_init()
        logging.debug('Sending terminate_launches request to provisioner')
        yield self.send('terminate_launches', launches)

    @defer.inlineCallbacks
    def terminate_nodes(self, nodes):
        """Terminates one or more nodes
        """
        yield self._check_init()
        logging.debug('Sending terminate_nodes request to provisioner')
        yield self.send('terminate_nodes', nodes)

class ProvisionerNotifier(object):
    """Abstraction for sending node updates to subscribers.
    """
    def __init__(self, process):
        self.process = process

    @defer.inlineCallbacks
    def send_record(self, record, subscribers, operation='node_status'):
        """Send a single node record to all subscribers.
        """
        logging.debug('Sending status record about node %s to %s', 
                record['node_id'], repr(subscribers))
        for sub in subscribers:
            yield self.process.send(sub, operation, record)

    @defer.inlineCallbacks
    def send_records(self, records, subscribers, operation='node_status'):
        """Send a set of node records to all subscribers.
        """
        for rec in records:
            yield self.send_record(rec, subscribers, operation)

# Spawn of the process using the module name
factory = ProtocolFactory(ProvisionerService)
