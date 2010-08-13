#!/usr/bin/env python

"""
@file ion/services/sa/data_acquisition.py
@author Michael Meisinger
@brief service for data acquisition
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.data.dataobject import DataObject, ResourceReference
from ion.services.base_service import BaseService, BaseServiceClient

from ion.resources.coi_resource_descriptions import AgentInstance
from ion.resources.sa_resource_descriptions import InstrumentResource, DataProductResource
from ion.services.sa.instrument_registry import InstrumentRegistryClient
from ion.services.sa.data_product_registry import DataProductRegistryClient
from ion.services.coi.agent_registry import AgentRegistryClient

class InstrumentManagementService(BaseService):
    """
    Instrument management service interface.
    This service provides overall coordination for instrument management within
    an observatory context. In particular it coordinates the access to the
    instrument and data product registries and the interaction with instrument
    agents.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='instrument_management',
                                          version='0.1.0',
                                          dependencies=[])

    def slc_init(self):
        self.irc = InstrumentRegistryClient(proc=self)
        self.dprc = DataProductRegistryClient(proc=self)
        self.arc = AgentRegistryClient(proc=self)

    @defer.inlineCallbacks
    def op_create_new_instrument(self, content, headers, msg):
        """
        Service operation: Accepts a dictionary containing user inputs and updates the instrument
        registry.
        """
        userInput = content['userInput']

        newinstrument = InstrumentResource.create_new_resource()

        if 'direct_access' in userInput:
            newinstrument.direct_access = str(userInput['direct_access'])

        if 'manufacturer' in userInput:
            newinstrument.manufacturer = str(userInput['manufacturer'])

        if 'model' in userInput:
            newinstrument.model = str(userInput['model'])

        if 'serial_num' in userInput:
            newinstrument.serial_num = str(userInput['serial_num'])

        if 'fw_version' in userInput:
            newinstrument.fw_version = str(userInput['fw_version'])

        instrument_res = yield self.irc.register_instrument_instance(newinstrument)

        yield self.reply_ok(msg, instrument_res.encode())

    @defer.inlineCallbacks
    def op_create_new_data_product(self, content, headers, msg):
        """
        Service operation: Accepts a dictionary containing user inputs and
        updates the data product registry.
        """
        dataProductInput = content['dataProductInput']

        newdp = DataProductResource.create_new_resource()
        if 'instrumentID' in dataProductInput:
            inst_id = str(dataProductInput['instrumentID'])
            int_ref = ResourceReference(RegistryIdentity=inst_id, RegistryBranch='master')
            newdp.instrument_ref = int_ref

        if 'dataformat' in dataProductInput:
            newdp.dataformat = str(dataProductInput['dataformat'])

        # Step: Create a data stream

        res = yield self.dprc.register_data_product(newdp)
        ref = res.reference(head=True)

        yield self.reply_ok(msg, res.encode())

    @defer.inlineCallbacks
    def op_execute_command(self, content, headers, msg):
        """
        Service operation: Execute a command on an instrument.
        """

        # Step 1: Extract the arguments from the UI generated message content
        commandInput = content['commandInput']

        if 'instrumentID' in commandInput:
            inst_id = str(commandInput['instrumentID'])
        else:
            raise ValueError("Input for instrumentID not present")

        command_op = str(commandInput['command'])

        command_args = []
        arg_idx = 0
        while True:
            argname = 'cmdArg'+str(arg_idx)
            if argname in commandInput:
                command_args.append(str(commandInput['argname']))
            else:
                break

        command = {command_op: command_args}

        # Step 2: Find the agent id for the given instrument id
        agent_pid  = yield get_agent_pid_for_instrument(inst_id)

        # Step 3: Interact with the agent to execute the command
        iaclient = InstrumentAgentClient(proc=self, target=agent_pid)
        cmd_result = yield iaclient.execute_instrument(command)

        yield self.reply_ok(msg, cmd_result)

    @defer.inlineCallbacks
    def op_get_instrument_state(self, content, headers, msg):
        """
        Service operation: .
        """
        # Step 1: Extract the arguments from the UI generated message content
        commandInput = content['commandInput']

        if 'instrumentID' in commandInput:
            inst_id = str(commandInput['instrumentID'])
        else:
            yield reply_error(msg, "Input for instrumentID not present")
            return

        agent_pid = yield get_agent_pid_for_instrument(inst_id)

        iaclient = InstrumentAgentClient(proc=self, target=agent_pid)

        yield self.reply_ok(msg, "")

    @defer.inlineCallbacks
    def op_start_direct_access(self, content, headers, msg):
        """
        Service operation: Starts direct access mode.
        """
        yield self.reply_err(msg, "Not yet implemented")

    @defer.inlineCallbacks
    def op_stop_direct_access(self, content, headers, msg):
        """
        Service operation: Stops direct access mode.
        """
        yield self.reply_err(msg, "Not yet implemented")

    @defer.inlineCallbacks
    def get_agent_for_instrument(self, instrument_id):
        int_ref = ResourceReference(RegistryIdentity=instrument_id, RegistryBranch='master')
        agent_query = AgentInstance()
        agent_query.instrument_ref = int_ref
        res_list = yield self.arc.find_registered_agent_instance_from_description(agent_query)
        agents = res_list.resources
        logging.debug("Found %s agent instances for instrument id %s" % (len(agents), instrument_id))
        agent_res = None
        if len(agents) > 0:
            agents[0]
        defer.returnValue(agent_res)

    @defer.inlineCallbacks
    def get_agent_pid_for_instrument(self, instrument_id):
        agent_res = yield get_agent_for_instrument(instrument_id)
        agent_pid = agent_res.process_name
        logging.debug("Agent process id for instrument id %s is: %s" % (instrument_id, agent_pid))
        defer.returnValue(agent_pid)

class InstrumentManagementClient(BaseServiceClient):
    """
    Class for the client accessing the instrument management service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_management"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def create_new_instrument(self, userInput):
        reqcont = {}
        reqcont['userInput'] = userInput

        (content, headers, message) = yield self.rpc_send('create_new_instrument',
                                                          reqcont)
        defer.returnValue(DataObject.decode(content['value']))

    @defer.inlineCallbacks
    def create_new_data_product(self, dataProductInput):
        reqcont = {}
        reqcont['dataProductInput'] = dataProductInput

        (content, headers, message) = yield self.rpc_send('create_new_data_product',
                                                          reqcont)
        defer.returnValue(DataObject.decode(content['value']))

# Spawn of the process using the module name
factory = ProtocolFactory(InstrumentManagementService)
