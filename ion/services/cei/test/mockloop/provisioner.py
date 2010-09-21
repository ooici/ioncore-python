#!/usr/bin/env python

"""
@brief Starts, stops, and tracks instance and context state for fake instances.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from twisted.internet.task import LoopingCall
from magnet.spawnable import Receiver

from ion.services.base_service import BaseService
from ion.core import base_process
from ion.core.base_process import ProtocolFactory

STATES = {0:"requesting",
          1:"requested",
          2:"pending",
          3:"started",
          4:"running",
          5:"shutting-down",
          6:"terminated"}

class MockLoopProvisionerService(BaseService):
    """Provisioner service for "mockloop" setup
    """

    # Declaration of service
    declare = BaseService.service_declare(name='provisioner', version='0.1.0', dependencies=[])

    def slc_init(self):
        
        # tracker is dict of dicts:
        # launch_id --> dict of {instance: state}
        self.tracker = {}
        
        self.looping_call = LoopingCall(self._move_states)
        self.looping_call.start(2.0)

    def op_provision(self, content, headers, msg):
        """Service operation: Provision a taskable resource
        """
        # skip any validation of message content
        launch_id = content['launch_id']
        instances = {}
        for key in content['instances'].keys():
            log.info("request to provision %s" % key)
            id_list = content['instances'][key]['id']
            for instance_id in id_list:
                log.info(" - %s" % instance_id)
                instances[instance_id] = 0
                
        self.tracker[launch_id] = instances
        
    def op_terminate(self, content, headers, msg):
        """Service operation: Terminate a taskable resource
        """
        log.error("cannot terminate yet")
        
    def _move_states(self):
        # in future will be per-instance, provided by provisioner client
        sa_instance = "sensor_aggregator"
        
        for (launch_id, instance_dict) in self.tracker.iteritems():
            for instance_id in instance_dict.keys():
                if instance_dict[instance_id] == 6:
                    log.info("'%s' instance: '%s'" % (instance_id, STATES[instance_dict[instance_id]]))
                    # moving to 7 means it won't show up again in logging
                    instance_dict[instance_id] += 1
                    
                elif instance_dict[instance_id] < 6:
                    current = STATES[instance_dict[instance_id]]
                    instance_dict[instance_id] += 1
                    newstate = STATES[instance_dict[instance_id]]
                    log.info("'%s' instance: state was '%s', moved to '%s'" % (instance_id, current, newstate))
                    
                    iaas_info = self._construct_iaas_info(launch_id, instance_id, instance_dict[instance_id])
                    self._send_iaas_notification(iaas_info, sa_instance)

    def _construct_iaas_info(self, launch_id, instance_id, state_int):
        return {
                 'launch_id' : launch_id,
                 'instance_id' : instance_id,
                 'state' : STATES[state_int],
                 'state_desc' : None,
                 'state_change_time' : 'sometime',
                 'start_time' : 'atime',
                 'termination_time' : 'anothertime',
                 'site' :  'ec2-west',
                 'allocation' : 'small',
                 'ctx-name' : 'head-node',
                 'ip_address' : '10.0.0.3',
                 'hostname' :  'ec2-foo-bar.123...',
                 'extras' : {
                      'ami-name' : 'ami-12f4wg5',
                      'ec2-instance-id' : 'i-f43fad',
                      'ctx-id':'c9c579be-6b8c-4a0f-8be7-78e21cf78782'
                  }
            }


    @defer.inlineCallbacks
    def _send_iaas_notification(self, iaas_info, sa_id):
        """Sends out IAAS information about an instance.
        """
        
        sa = yield base_process.procRegistry.get(sa_id)
        (content, headers, msg) = yield self.rpc_send(sa, "sensor_aggregator_info", iaas_info, {})
        
        
# Spawn of the process using the module name
factory = ProtocolFactory(MockLoopProvisionerService)
