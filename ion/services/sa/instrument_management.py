#!/usr/bin/env python

"""
@file ion/services/sa/data_acquisition.py
@author Michael Meisinger
@brief service for data acquisition
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator
from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.core.process.process import ProcessFactory, ProcessDesc

from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.resource_registry.association_client import AssociationClient
from ion.services.coi.resource_registry.association_client import AssociationClientError
import ion.util.procutils as pu
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.services.dm.distribution.events import InfoLoggingEventSubscriber
from ion.services.dm.distribution.events import DataEventSubscriber

import ion.agents.instrumentagents.instrument_agent as instrument_agent

from ion.core.process.process import Process
from ion.core.process.process import ProcessDesc
from ion.core import bootstrap


from ion.core.object import object_utils

from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID

INSTRUMENT_TYPE = object_utils.create_type_identifier(object_id=4301, version=1)
INSTRUMENT_AGENT_TYPE = object_utils.create_type_identifier(object_id=4302, version=1)
IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)

INSTRUMENTDATA_EVENT_ID = 5001


class InstrumentDataEventSubscriber(DataEventSubscriber):
    """
    Event Notification Subscriber for Instrument Data.

    The "origin" parameter in this class' initializer should be the process' exchagne name (TODO: correct?)
    """
    event_id = INSTRUMENTDATA_EVENT_ID

 # Setup a subscriber to an event topic
class SBE37DataEventSubscriber(InstrumentDataEventSubscriber):
    def __init__(self, *args, **kwargs):
        self.msgs = []
        DataEventSubscriber.__init__(self, *args, **kwargs)

    def ondata(self, data):
        log.info("IMSSRVC !!!!!!! SBE37DataEventSubscriber received a message with content: %s",
                data['content'])

        msg = data['content'];
        log.info("IMSSRVC !!!!!!! SBE37DataEventSubscriber additional info: %s", msg.additional_data.data_block)

        self.msgs.append(data)


        
class InstrumentManagementService(ServiceProcess):
    """
    Instrument management service interface.
    This service provides overall coordination for instrument management within
    an observatory context. In particular it coordinates the access to the
    instrument and data product registries and the interaction with instrument
    agents.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='instrument_management',
                                          version='0.1.0',
                                          dependencies=[])

    def slc_init(self):
        #self.irc = InstrumentRegistryClient(proc=self)
        #self.dprc = DataProductRegistryClient(proc=self)
        #self.arc = AgentRegistryClient(proc=self)
        #self.dpsc = DataPubsubClient(proc=self)
        self.rc = ResourceClient(proc=self)
        self.ac    = AssociationClient(proc=self)


    @defer.inlineCallbacks
    def op_create_new_instrument(self, content, headers, msg):
        """
        Service operation: Accepts a dictionary containing user inputs.
        Updates the instrument registry.
        """
        userInput = content['userInput']

        resource = yield self.rc.create_instance(INSTRUMENT_TYPE, ResourceName='Test Instrument Resource', ResourceDescription='A test instrument resource')

        log.info("IMSSRVC op_create_new_instrument created resource")

        # Set the attributes
        if 'name' in userInput:
            resource.name = str(userInput['name'])

        if 'description' in userInput:
            resource.description = str(userInput['description'])

        if 'manufacturer' in userInput:
            resource.manufacturer = str(userInput['manufacturer'])

        if 'model' in userInput:
            resource.model = str(userInput['model'])

        if 'serial_num' in userInput:
            resource.serial_num = str(userInput['serial_num'])

        if 'fw_version' in userInput:
            resource.fw_version = str(userInput['fw_version'])

        yield self.rc.put_instance(resource, 'Save instrument resource')
        res_id = resource.ResourceIdentity
        log.info("IMSSRVC op_create_new_instrument stored resource. identity: %s ", res_id)

        res_value = {'instrument_id':res_id }

        yield self.reply_ok(msg, res_value)

    @defer.inlineCallbacks
    def op_create_new_data_product(self, content, headers, msg):
        """
        Service operation: Accepts a dictionary containing user inputs.
        Updates the data product registry. Also sets up an ingestion pipeline
        for an instrument
        """
        dataProductInput = content['dataProductInput']

        """
        newdp = DataProductResource.create_new_resource()
        if 'instrumentID' in dataProductInput:
            inst_id = str(dataProductInput['instrumentID'])
            int_ref = ResourceReference(RegistryIdentity=inst_id, RegistryBranch='master')
            newdp.instrument_ref = int_ref

        if 'name' in dataProductInput:
            newdp.name = str(dataProductInput['name'])

        if 'description' in dataProductInput:
            newdp.description = str(dataProductInput['description'])

        if 'dataformat' in dataProductInput:
            newdp.dataformat = str(dataProductInput['dataformat'])

        # Step: Create a data stream
        ## Instantiate a pubsubclient
        #self.dpsc = DataPubsubClient(proc=self)
        #
        ## Create and Register a topic
        #self.topic = PubSubTopicResource.create('SBE49 Topic',"oceans, oil spill")
        #self.topic = yield self.dpsc.define_topic(self.topic)
        #log.debug('DHE: Defined Topic')
        #
        #self.publisher = PublisherResource.create('Test Publisher', self, self.topic, 'DataObject')
        #self.publisher = yield self.dpsc.define_publisher(self.publisher)


        res = yield self.dprc.register_data_product(newdp)
        ref = res.reference(head=True)

        yield self.reply_ok(msg, res.encode())
        """

    @defer.inlineCallbacks
    def op_execute_command(self, content, headers, msg):
        """
        Service operation: Execute a command on an instrument.
        """

        """
        # Step 1: Extract the arguments from the UI generated message content
        commandInput = content['commandInput']

        if 'instrumentID' in commandInput:
            inst_id = str(commandInput['instrumentID'])
        else:
            raise ValueError("Input for instrumentID not present")

        command = []
        if 'command' in commandInput:
            command_op = str(commandInput['command'])
        else:
            raise ValueError("Input for command not present")

        command.append(command_op)

        arg_idx = 0
        while True:
            argname = 'cmdArg'+str(arg_idx)
            arg_idx += 1
            if argname in commandInput:
                command.append(str(commandInput[argname]))
            else:
                break

        # Step 2: Find the agent id for the given instrument id
        agent_pid  = yield self.get_agent_pid_for_instrument(inst_id)
        if not agent_pid:
            yield self.reply_err(msg, "No agent found for instrument "+str(inst_id))
            defer.returnValue(None)

        # Step 3: Interact with the agent to execute the command
        iaclient = InstrumentAgentClient(proc=self, target=agent_pid)
        commandlist = [command,]
        log.info("Sending command to IA: "+str(commandlist))
        cmd_result = yield iaclient.execute_instrument(commandlist)
        """

        yield self.reply_ok(msg, cmd_result)

    @defer.inlineCallbacks
    def op_get_instrument_state(self, content, headers, msg):
        """
        Service operation: .
        """
        # Step 1: Extract the arguments from the UI generated message content
        commandInput = content['commandInput']

        """
        if 'instrumentID' in commandInput:
            inst_id = str(commandInput['instrumentID'])
        else:
            raise ValueError("Input for instrumentID not present")

        agent_pid = yield self.get_agent_pid_for_instrument(inst_id)
        if not agent_pid:
            raise StandardError("No agent found for instrument "+str(inst_id))

        iaclient = InstrumentAgentClient(proc=self, target=agent_pid)
        inst_cap = yield iaclient.get_capabilities()
        if not inst_cap:
            raise StandardError("No capabilities available for instrument "+str(inst_id))

        ci_commands = inst_cap['ci_commands']
        instrument_commands = inst_cap['instrument_commands']
        instrument_parameters = inst_cap['instrument_parameters']
        ci_parameters = inst_cap['ci_parameters']

        values = yield iaclient.get_from_instrument(instrument_parameters)
        resvalues = {}
        if values:
            resvalues = values
        """

        yield self.reply_ok(msg, resvalues)


    @defer.inlineCallbacks
    def op_start_instrument_agent(self, content, headers, msg):
        """
        Service operation: Starts an instrument agent for a type of
        instrument.
        """

        log.info("IMSSRVC op_start_instrument_agent start")
        if 'instrumentID' in content:
            inst_id = str(content['instrumentID'])
        else:
            raise ValueError("Input for instrumentID not present")

        if 'instrumentResourceID' in content:
            inst_resource_id = str(content['instrumentResourceID'])
        else:
            raise ValueError("Input for instrumentResourceID not present")

        if 'model' in content:
            model = str(content['model'])
        else:
            raise ValueError("Input for model not present")

        if model != 'SBE37':
            raise ValueError("Only SBE37 supported!")
        log.info("IMSSRVC op_start_instrument_agent good input")



        #yield self._start_container()

        # Driver and agent configuration. Configuration data will ultimatly be
        # accessed via some persistance mechanism: platform filesystem
        # or a device registry. For now, we pass all configuration data
        # that would be read this way as process arguments.
        sbe_host = '137.110.112.119'
        sbe_port = 4001
        driver_config = {
            'ipport':sbe_port,
            'ipaddr':sbe_host
        }
        agent_config = {}

        # Process description for the SBE37 driver.
        driver_desc = {
            'name':'SBE37_driver',
            'module':'ion.agents.instrumentagents.SBE37_driver',
            'class':'SBE37Driver',
            'spawnargs':{'config':driver_config}
        }

        # Process description for the SBE37 driver client.
        driver_client_desc = {
            'name':'SBE37_client',
            'module':'ion.agents.instrumentagents.SBE37_driver',
            'class':'SBE37DriverClient',
            'spawnargs':{}
        }

        # Spawnargs for the instrument agent.
        spawnargs = {
            'driver-desc':driver_desc,
            'client-desc':driver_client_desc,
            'driver-config':driver_config,
            'agent-config':agent_config
        }

        # Process description for the instrument agent.
        agent_desc = {
            'name':'instrument_agent',
            'module':'ion.agents.instrumentagents.instrument_agent',
            'class':'InstrumentAgent',
            'spawnargs':spawnargs
        }

        # Processes for the tests.
        processes = [
            agent_desc
        ]

        # Spawn agent and driver, create agent client.
        log.info("IMSSRVC op_start_instrument_agent spawn")
        #self.sup1 = yield bootstrap.create_supervisor()
        proc1 = ProcessDesc(**agent_desc)
        self.svc_id = yield self.spawn_child(proc1)
        log.info("IMSSRVC op_start_instrument_agent spawned process id: %s", self.svc_id)
        self.ia_client = instrument_agent.InstrumentAgentClient(proc=self, target=self.svc_id)
        log.info("IMSSRVC op_start_instrument_agent get ia_client")

        #self.ia_client.register_resource(content['instrumentResourceID'])

        """
        reply_1 = yield self.ia_client.start_transaction(0)
        log.info("IMSSRVC op_start_instrument_agent start trans %s", reply_1)
        transaction_id_1 = reply_1['transaction_id']
        reply_3 = yield self.ia_client.end_transaction(transaction_id_1)
        log.info("IMSSRVC op_start_instrument_agent end trans %s", reply_3)
        """

        log.info("IMSSRVC op_start_instrument_agent register resource")
        #store the new instrument agent in the resource registry
        instrumentAgentResource = yield self.rc.create_instance(INSTRUMENT_AGENT_TYPE, ResourceName='Test Instrument Agent Resource', ResourceDescription='A test instrument resource')

        # Set the attributes
        instrumentAgentResource.name = content['model']
        instrumentAgentResource.description = content['model']
        instrumentAgentResource.class_name = 'SBE37InstrumentAgent'
        instrumentAgentResource.module = 'ion.agents.instrumentagents.SBE37_IA'
        instrumentAgentResource.process_id = str(self.svc_id)

        #Store the resource in the registry
        yield self.rc.put_instance(instrumentAgentResource, 'Save agent resource')
        inst_agnt_id = instrumentAgentResource.ResourceIdentity
        log.info("IMSSRVC op_start_instrument_agent stored agent resource. identity: %s ", inst_agnt_id)

        #Associate this agent to the instrument
        instrument_resource = yield self.rc.get_instance(inst_resource_id)
        association = yield self.ac.create_association(instrument_resource, HAS_A_ID, instrumentAgentResource)
        # Put the association and the resources in the datastore
        yield self.rc.put_resource_transaction([instrument_resource, instrumentAgentResource])
        log.info("IMSSRVC op_start_instrument_agent created association %s", association)

        #https://github.com/ooici/ioncore-python/blob/r1lca/ion/services/dm/presentation/web_viz_consumer.py
        #https://github.com/ooici/ioncore-python/blob/r1lca/ion/services/dm/distribution/consumers/timeseries_consumer.py

        log.info("IMSSRVC op_start_instrument_agent spawn listerner")
        subproc = Process()
        yield subproc.spawn()

        dataEventSubscrbr = SBE37DataEventSubscriber(origin=inst_agnt_id, process=subproc)
        log.info('IMSSRVC op_start_instrument_agent set handler for DataEventSubscriber')
        yield dataEventSubscrbr.initialize()
        yield dataEventSubscrbr.activate()
        log.info('IMSSRVC op_start_instrument_agent DataEvent activation complete')

        res_value = {'instrument_agent_id':inst_agnt_id }
        yield self.reply_ok(msg, res_value)

    @defer.inlineCallbacks
    def op_stop_instrument_agent(self, content, headers, msg):
        """
        Service operation: Starts direct access mode.
        """
        yield self.reply_err(msg, "Not yet implemented")


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
    def get_agent_desc_for_instrument(self, instrument_id):
        log.info("get_agent_desc_for_instrument() instrumentID="+str(instrument_id))
        """
        int_ref = ResourceReference(RegistryIdentity=instrument_id, RegistryBranch='master')
        agent_query = InstrumentAgentResourceInstance()
        agent_query.instrument_ref = int_ref


        if not agent_res:
            defer.returnValue(None)
        agent_pid = agent_res.proc_id
        log.info("Agent process id for instrument id %s is: %s" % (instrument_id, agent_pid))
        defer.returnValue(agent_pid)
        """

    @defer.inlineCallbacks
    def get_agent_for_instrument(self, inst_resource_id):

        result = None
        instrument_resource = yield self.rc.get_instance(inst_resource_id)
        try:
            results = yield self.ac.find_associations(subject=instrument_resource, predicate_or_predicates=HAS_A_ID)

        except AssociationClientError:
            log.error('AssociationError')
            defer.returnValue(result)

        for association in results:
            log.info('Associated Source for Instrument: ' + \
                      association.ObjectReference.key + \
                      ' is: ' + association.SubjectReference.key)

        instrument_agent_resource = yield self.rc.get_instance(association.ObjectReference.key)


        """
        log.info("get_agent_for_instrument() instrumentID="+str(instrument_id))
        int_ref = ResourceReference(RegistryIdentity=instrument_id, RegistryBranch='master')
        agent_query = InstrumentAgentResourceInstance()
        agent_query.instrument_ref = int_ref
        # @todo Need to list the LC state here. WHY???
        agent_query.lifecycle = LCStates.developed
        agents = yield self.arc.find_registered_agent_instance_from_description(agent_query, regex=False)
        log.info("Found %s agent instances for instrument id %s" % (len(agents), instrument_id))
        agent_res = None
        if len(agents) > 0:
            agent_res = agents[0]
        defer.returnValue(agent_res)
        """

    @defer.inlineCallbacks
    def get_agent_pid_for_instrument(self, instrument_id):
        """
        agent_res = yield self.get_agent_for_instrument(instrument_id)
        if not agent_res:
            defer.returnValue(None)
        agent_pid = agent_res.proc_id
        log.info("Agent process id for instrument id %s is: %s" % (instrument_id, agent_pid))
        defer.returnValue(agent_pid)
        """

class InstrumentManagementClient(ServiceClient):
    """
    Class for the client accessing the instrument management service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_management"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def create_new_instrument(self, userInput):
        reqcont = {}
        reqcont['userInput'] = userInput

        (cont, hdrs, msg) = yield self.rpc_send('create_new_instrument', reqcont)
        defer.returnValue(cont)

    @defer.inlineCallbacks
    def create_new_data_product(self, dataProductInput):
        reqcont = {}
        reqcont['dataProductInput'] = dataProductInput

        result = yield self.rpc_send('create_new_data_product', reqcont)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def start_instrument_agent(self, instrumentID, instrumentResourceID, model):
        reqcont = {}
        reqcont['instrumentID'] = instrumentID
        reqcont['instrumentResourceID'] = instrumentResourceID
        reqcont['model'] = model
        #result = yield self._base_command('start_instrument_agent', reqcont)
        (cont, hdrs, msg)  = yield self.rpc_send('start_instrument_agent', reqcont)
        defer.returnValue(cont)

    @defer.inlineCallbacks
    def stop_instrument_agent(self, instrumentID):
        reqcont = {}
        reqcont['instrumentID'] = instrumentID
        result = yield self._base_command('stop_instrument_agent', reqcont)
        defer.returnValue(result)


    @defer.inlineCallbacks
    def get_instrument_state(self, instrumentID):
        reqcont = {}
        commandInput = {}
        commandInput['instrumentID'] = instrumentID
        reqcont['commandInput'] = commandInput

        result = yield self._base_command('get_instrument_state', reqcont)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def execute_command(self, instrumentID, command, arglist):
        reqcont = {}
        commandInput = {}
        commandInput['instrumentID'] = instrumentID
        commandInput['command'] = command
        if arglist:
            argnum = 0
            for arg in arglist:
                commandInput['cmdArg'+str(argnum)] = arg
                argnum += 1
        reqcont['commandInput'] = commandInput

        result = yield self._base_command('execute_command', reqcont)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _base_command(self, op, content):
        (cont, hdrs, msg) = yield self.rpc_send(op, content)
        defer.returnValue(cont)

# Spawn of the process using the module name
factory = ProcessFactory(InstrumentManagementService)
