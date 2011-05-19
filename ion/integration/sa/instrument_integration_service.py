#!/usr/bin/env python

"""
@file ion/integration/sa/instrument_integration_service.py
@author Maurice Manning
@brief service to provide instrument workflows to UI and external access
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
from ion.core.messaging.message_client import MessageClient

from ion.services.dm.inventory.association_service import AssociationServiceClient, ASSOCIATION_QUERY_MSG_TYPE
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE, PREDICATE_REFERENCE_TYPE

import ion.util.procutils as pu
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.services.dm.distribution.events import InfoLoggingEventSubscriber
from ion.services.dm.distribution.events import DataEventSubscriber

import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.agents.instrumentagents.instrument_constants import AgentCommand
from ion.agents.instrumentagents.instrument_constants import AgentEvent
from ion.agents.instrumentagents.instrument_constants import AgentStatus
from ion.agents.instrumentagents.instrument_constants import AgentState
from ion.agents.instrumentagents.instrument_constants import DriverChannel
from ion.agents.instrumentagents.instrument_constants import DriverCommand
from ion.agents.instrumentagents.instrument_constants import InstErrorCode

from ion.services.coi.datastore_bootstrap.ion_preload_config import INSTRUMENT_RES_TYPE_ID, TYPE_OF_ID

from ion.core.process.process import Process
from ion.core.process.process import ProcessDesc
from ion.core import bootstrap


from ion.core.object import object_utils
import gviz_api

from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID

INSTRUMENT_TYPE = object_utils.create_type_identifier(object_id=4301, version=1)
INSTRUMENT_AGENT_TYPE = object_utils.create_type_identifier(object_id=4302, version=1)
#IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)

AIS_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9001, version=1)
AIS_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9002, version=1)
AIS_RESPONSE_ERROR_TYPE = object_utils.create_type_identifier(object_id=9003, version=1)
CREATE_INSTRUMENT_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9301, version=1)
CREATE_INSTRUMENT_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9302, version=1)
START_INSTRUMENT_AGENT_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9303, version=1)
START_INSTRUMENT_AGENT_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9304, version=1)
START_INSTRUMENT_SAMPLING_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9305, version=1)
START_INSTRUMENT_SAMPLING_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9306, version=1)
STOP_INSTRUMENT_SAMPLING_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9307, version=1)
STOP_INSTRUMENT_SAMPLING_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9308, version=1)
GET_INSTRUMENT_STATE_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9309, version=1)
GET_INSTRUMENT_STATE_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9310, version=1)
SET_INSTRUMENT_STATE_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9311, version=1)
SET_INSTRUMENT_STATE_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9312, version=1)
GET_INSTRUMENT_LIST_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9313, version=1)
GET_INSTRUMENT_LIST_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9314, version=1)


"""
class InstrumentDataEventSubscriber(DataEventSubscriber):

    #Event Notification Subscriber for Instrument Data.

    #The "origin" parameter in this class' initializer should be the process' exchagne name (TODO: correct?)

    event_id = INSTRUMENTDATA_EVENT_ID

 # Setup a subscriber to an event topic
class SBE37DataEventSubscriber(InstrumentDataEventSubscriber):
    def __init__(self, *args, **kwargs):
        self.msgs = []
        self.pdata=[]
        DataEventSubscriber.__init__(self, *args, **kwargs)
        self.max_points=50

    def ondata(self, data):
        msg = data['content'];
        log.info("IIService SBE37DataEventSubscriber additional info: %s", msg.additional_data.data_block)

        self.msgs.append(data)
        #convert the incoming string into a dict
"""


class InstrumentIntegrationService(ServiceProcess):
    """
    Instrument integration service interface.
    This service provides overall coordination for instrument workflows within
    an observatory context. In particular it coordinates the access to the
    instrument and interaction with instrument agents.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='instrument_integration_service',
                                          version='0.1.0',
                                          dependencies=[])

    def slc_init(self):
        self.rc = ResourceClient(proc=self)
        self.ac = AssociationClient(proc=self)
        self.asc = AssociationServiceClient(proc=self)
        self.mc = MessageClient(proc = self)
        log.info('Finishing slc_init')

        
    @defer.inlineCallbacks
    def op_prepInstrument(self, instrument_agent_resource_id):

        self.ia_client = instrument_agent.InstrumentAgentClient(proc=self, target=instrument_agent_resource_id.process_id)
        log.info("IIService op_prepInstrument ia_client retrieved")

        # Begin an explicit transaciton.
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        trans_id = reply['transaction_id']
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(success, "Unable to transition instrument state")
            return

        # Initialize the agent to bring up the driver and client.
        cmd = [AgentCommand.TRANSITION,AgentEvent.INITIALIZE]
        reply = yield self.ia_client.execute_observatory(cmd,trans_id)
        success = reply['success']
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(success, "Unable to transition instrument state")
            return

        # Connect to the driver.
        cmd = [AgentCommand.TRANSITION,AgentEvent.GO_ACTIVE]
        reply = yield self.ia_client.execute_observatory(cmd,trans_id)
        success = reply['success']
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(success, "Unable to transition instrument state")
            return

        # Enter observatory mode.
        cmd = [AgentCommand.TRANSITION,AgentEvent.RUN]
        reply = yield self.ia_client.execute_observatory(cmd,trans_id)
        success = reply['success']
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(success, "Unable to transition instrument state")
            return

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,trans_id)
        #self.assert_(agent_state == AgentState.OBSERVATORY_MODE)
        success = reply['success']
        result = reply['result']
        log.info("IIService prep_instrument state: %s", result)
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(success, "Unable to transition instrument state")
            return

        defer.returnValue(trans_id)
        
    @defer.inlineCallbacks
    def op_cleanupInstrument(self, trans_id):
        # Reset the agent to disconnect and bring down the driver and client.
        cmd = [AgentCommand.TRANSITION,AgentEvent.RESET]
        reply = yield self.ia_client.execute_observatory(cmd,trans_id)
        success = reply['success']
        result = reply['result']
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(success, "Unable to transition instrument state")
            return

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,trans_id)
        success = reply['success']
        result = reply['result']
        log.info("IIService op_cleanupInstrument state: %s", result)
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(success, "Unable to transition instrument state")
            return     

        # End the transaction.
        reply = yield self.ia_client.end_transaction(trans_id)
        success = reply['success']
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(success, "Unable to transition instrument state")
            return

    @defer.inlineCallbacks
    def op_getInstrumentList(self, content, headers, msg):
        """
        Service operation: Returns all instrument resource ids in the resource registry.
        """
        log.info("IIService op_getInstrumentList enter")

        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # Set the Object search term
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = INSTRUMENT_RES_TYPE_ID
        pair.object = type_ref

        log.info("IIService op_getInstrumentList call assoc service")

        result = yield self.asc.get_subjects(request)

        log.info("IIService op_getInstrumentList size: %s", str(len(result.idrefs)))

        key_list = []
        for idref in result.idrefs:
            log.info("IIService op_getInstrumentList list: %s", idref)
            key_list.append(idref.key)
        
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(GET_INSTRUMENT_LIST_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference[0].result = str(key_list)
        log.info("IIService op_getInstrumentList key_list %s"%str(key_list))
        yield self.reply_ok(msg, rspMsg)
        

    @defer.inlineCallbacks
    def op_createNewInstrument(self, content, headers, msg):
        """
        Service operation: Accepts a dictionary containing user inputs.
        Updates the instrument registry.
        """
        log.info('In createNewInstrument')
        name = content.message_parameters_reference.name
        log.info('name: %s'%name)
        description = content.message_parameters_reference.description
        log.info('description: %s'%description)
        manufacturer = content.message_parameters_reference.manufacturer
        log.info('manufacturer: %s'%manufacturer)
        model = content.message_parameters_reference.model
        log.info('model: %s'%model)
        serial_num = content.message_parameters_reference.serial_num
        log.info('serial_num: %s'%serial_num)
        fw_version = content.message_parameters_reference.fw_version
        log.info('fw_version: %s'%fw_version)

        resource = yield self.rc.create_instance(INSTRUMENT_TYPE, ResourceName='Test Instrument Resource', ResourceDescription='A test instrument resource')

        log.info("IIService op_create_new_instrument created resource")

        # Set the attributes
        resource.name = name
        resource.description = description
        resource.manufacturer = manufacturer
        resource.model = model
        resource.serial_num = serial_num
        resource.fw_version = fw_version

        yield self.rc.put_instance(resource, 'Save instrument resource')
        res_id = resource.ResourceIdentity
        log.info("IIService op_create_new_instrument stored resource. identity: %s ", res_id)

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(CREATE_INSTRUMENT_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference[0].instrument_resource_id = res_id

        log.info('Replying')
        yield self.reply_ok(msg, rspMsg)


    @defer.inlineCallbacks
    def op_startAutoSampling(self, content, headers, msg):
        """
        Service operation: Execute a command on an instrument.

        """
        log.info("IIService op_startAutoSampling")
        # Step 1: Extract the arguments from the UI generated message content
        commandInput = content.message_parameters_reference.instrument_resource_id

        # get the agent resource for this instrument
        agent_resource_id = yield self.getAgentForInstrument(commandInput)
        log.info("IIService op_startAutoSampling agent resource: %s", agent_resource_id)

        instrument_agent_resource = yield self.rc.get_instance(agent_resource_id)

        # Put the instrument in a state to accept commands
        transaction_id = yield self.op_prepInstrument(instrument_agent_resource)

        # Start autosampling.
        chans = [DriverChannel.INSTRUMENT]
        cmd = [DriverCommand.START_AUTO_SAMPLING]
        reply = yield self.ia_client.execute_device(chans,cmd,transaction_id)
        success = reply['success']
        result = reply['result']
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(msg, "Unable to transition instrument state")
            return

       # Put the instrument back into passive mode
        reply = yield self.op_cleanupInstrument(transaction_id)

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(START_INSTRUMENT_SAMPLING_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference[0].result = result

        log.info('Replying')
        yield self.reply_ok(msg, rspMsg)

    @defer.inlineCallbacks
    def op_stopAutoSampling(self, content, headers, msg):
        """
        Service operation: Execute a command on an instrument.

        """
        log.info("IIService op_startAutoSampling")
        # Step 1: Extract the arguments from the UI generated message content
        commandInput = content.message_parameters_reference.instrument_resource_id

        # get the agent resource for this instrument
        agent_resource_id = yield self.getAgentForInstrument(commandInput)
        log.info("IIService op_startAutoSampling agent resource: %s", agent_resource_id)

        instrument_agent_resource = yield self.rc.get_instance(agent_resource_id)

        # Put the instrument in a state to accept commands
        transaction_id = yield self.op_prepInstrument(instrument_agent_resource)


        # Stop autosampling.
        chans = [DriverChannel.INSTRUMENT]
        cmd = [DriverCommand.STOP_AUTO_SAMPLING,'GETDATA']
        while True:
            reply = yield self.ia_client.execute_device(chans,cmd,transaction_id)
            success = reply['success']
            result = reply['result']

            if InstErrorCode.is_ok(success):
                break

            #elif success == InstErrorCode.TIMEOUT:
            elif InstErrorCode.is_equal(success,InstErrorCode.TIMEOUT):
                pass

            else:
                log.info("IIService Unable to transition instrument state")
                yield self.reply_err(msg, "Unable to transition instrument state")
                return
                
        # Put the instrument back into passive mode
        reply = yield self.op_cleanupInstrument(transaction_id)

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(STOP_INSTRUMENT_SAMPLING_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference[0].result = result

        log.info('Replying')
        yield self.reply_ok(msg, rspMsg)

    @defer.inlineCallbacks
    def op_getInstrumentState(self, content, headers, msg):
        """
        Service operation: Retrieve the state of the instrument
        """
        log.info("IIService op_getInstrumentState")
        # Step 1: Extract the arguments from the UI generated message content
        commandInput = content.message_parameters_reference.instrument_resource_id

        # get the agent resource for this instrument
        agent_resource_id = yield self.getAgentForInstrument(commandInput)
        log.info("IIService op_getInstrumentState agent resource: %s", agent_resource_id)

        instrument_agent_resource = yield self.rc.get_instance(agent_resource_id)

        # Put the instrument in a state to accept commands
        transaction_id = yield self.op_prepInstrument(instrument_agent_resource)
        #transaction_id = reply['tid']

        # Get driver parameters.
        params = [('all','all')]
        reply = yield self.ia_client.get_device(params, transaction_id)
        success = reply['success']
        result = reply['result']
        log.info("IIService op_getInstrumentState state: %s", result)
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(msg, "Unable to transition instrument state")
            return

        #  Put the instrument back into passive mode
        reply = yield self.op_cleanupInstrument(transaction_id)

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(GET_INSTRUMENT_STATE_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference[0].result = result

        log.info('Replying')
        yield self.reply_ok(msg, rspMsg)

    @defer.inlineCallbacks
    def op_setInstrumentState(self, content, headers, msg):
        """
        Service operation:
        """
        log.info("IIService op_setInstrumentState")
        # Step 1: Extract the arguments from the UI generated message content
        instrument_id = content.message_parameters_reference.instrument_resource_id
        parameters = content.message_parameters_reference.parameters

        log.info("IIService op_setInstrumentState  inst id: %s   parameters: %s", instrument_id, parameters)

        # get the agent resource for this instrument
        agent_resource_id = yield self.getAgentForInstrument(instrument_id)
        log.info("IIService op_setInstrumentState agent resource: %s", agent_resource_id)
        instrument_agent_resource = yield self.rc.get_instance(agent_resource_id)

        # Put the instrument in a state to accept commands
        transaction_id = yield self.op_prepInstrument(instrument_agent_resource)

        # Set driver parameters.
        reply = yield self.ia_client.set_device(parameters,transaction_id)
        success = reply['success']
        result = reply['result']
        log.info("IIService op_setInstrumentState state: %s", result)
        if not success:
            log.info("IIService Unable to transition instrument state")
            yield self.reply_err(msg, "Unable to transition instrument state")
            return

       #  Put the instrument back into passive mode
        reply = yield self.op_cleanupInstrument(transaction_id)

#        res_value = {'result':result }
#        yield self.reply_ok(msg, res_value)

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(SET_INSTRUMENT_STATE_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference[0].result = result

        log.info('Replying')
        yield self.reply_ok(msg, rspMsg)

    @defer.inlineCallbacks
    def op_startInstrumentAgent(self, content, headers, msg):
        """
        Service operation: Starts an instrument agent for a type of
        instrument.
        """
        log.info('In startInstrumentAgent')
#        userInput = content['userInput']
        inst_id = content.message_parameters_reference.name
        log.info('name: %s'%inst_id)
        model = content.message_parameters_reference.model
        log.info('model: %s'%model)
        inst_resource_id = content.message_parameters_reference.instrument_resource_id
        log.info('instrument_resource_id: %s'%inst_resource_id)

#        log.info("IIService op_start_instrument_agent start")
#        if 'instrument_id' in content:
#            inst_id = str(content['instrument_id'])
#        else:
#            raise ValueError("Input for instrument_id not present")
#
#        if 'instrumentResourceID' in content:
#            inst_resource_id = str(content['instrumentResourceID'])
#        else:
#            raise ValueError("Input for instrumentResourceID not present")
#
#        if 'model' in content:
#            model = str(content['model'])
#        else:
#            raise ValueError("Input for model not present")

        if model != 'SBE37':
            raise ValueError("Only SBE37 supported!")
        log.info("IIService op_start_instrument_agent good input")



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
        log.info("IIService op_start_instrument_agent spawn")
        #self.sup1 = yield bootstrap.create_supervisor()
        proc1 = ProcessDesc(**agent_desc)
        self.svc_id = yield self.spawn_child(proc1)
        log.info("IIService op_start_instrument_agent spawned process id: %s", self.svc_id)
        self.ia_client = instrument_agent.InstrumentAgentClient(proc=self, target=self.svc_id)
        log.info("IIService op_start_instrument_agent get ia_client")

        #self.ia_client.register_resource(content['instrumentResourceID'])

        """
        reply_1 = yield self.ia_client.start_transaction(0)
        log.info("IIService op_start_instrument_agent start trans %s", reply_1)
        transaction_id_1 = reply_1['transaction_id']
        reply_3 = yield self.ia_client.end_transaction(transaction_id_1)
        log.info("IIService op_start_instrument_agent end trans %s", reply_3)
        """

        log.info("IIService op_start_instrument_agent register resource")
        #store the new instrument agent in the resource registry
        instrumentAgentResource = yield self.rc.create_instance(INSTRUMENT_AGENT_TYPE, ResourceName='Test Instrument Agent Resource', ResourceDescription='A test instrument resource')

        # Set the attributes
#        instrumentAgentResource.name = content['model']
#        instrumentAgentResource.description = content['model']
        instrumentAgentResource.name = model
        instrumentAgentResource.description = model
        instrumentAgentResource.class_name = 'SBE37InstrumentAgent'
        instrumentAgentResource.module = 'ion.agents.instrumentagents.SBE37_IA'
        instrumentAgentResource.process_id = str(self.svc_id)

        #Store the resource in the registry
        yield self.rc.put_instance(instrumentAgentResource, 'Save agent resource')
        inst_agnt_id = instrumentAgentResource.ResourceIdentity
        log.info("IIService op_start_instrument_agent stored agent resource. identity: %s ", inst_agnt_id)

        #Associate this agent to the instrument
        instrument_resource = yield self.rc.get_instance(inst_resource_id)
        association = yield self.ac.create_association(instrument_resource, HAS_A_ID, instrumentAgentResource)
        # Put the association and the resources in the datastore
        yield self.rc.put_resource_transaction([instrument_resource, instrumentAgentResource])
        log.info("IIService op_start_instrument_agent created association %s", association)

        #https://github.com/ooici/ioncore-python/blob/r1lca/ion/services/dm/presentation/web_viz_consumer.py
        #https://github.com/ooici/ioncore-python/blob/r1lca/ion/services/dm/distribution/consumers/timeseries_consumer.py

        """
        log.info("IIService op_start_instrument_agent spawn listerner")
        subproc = Process()
        yield subproc.spawn()

        dataEventSubscrbr = SBE37DataEventSubscriber(origin=inst_agnt_id, process=subproc)
        log.info('IIService op_start_instrument_agent set handler for DataEventSubscriber')
        yield dataEventSubscrbr.initialize()
        yield dataEventSubscrbr.activate()
        log.info('IIService op_start_instrument_agent DataEvent activation complete')
        """

#        res_value = {'instrument_agent_resource_id':inst_agnt_id, 'instrument_agent_process_id':str(self.svc_id)}
#        yield self.reply_ok(msg, res_value)

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(START_INSTRUMENT_AGENT_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference[0].instrument_agent_resource_id = inst_agnt_id
        rspMsg.message_parameters_reference[0].instrument_agent_process_id = str(self.svc_id)

    @defer.inlineCallbacks
    def op_stopInstrumentAgent(self, content, headers, msg):
        """
        Service operation: Starts direct access mode.
        """
        yield self.reply_err(msg, "Not yet implemented")


    @defer.inlineCallbacks
    def op_startDirectAccess(self, content, headers, msg):
        """
        Service operation: Starts direct access mode.
        """
        yield self.reply_err(msg, "Not yet implemented")

    @defer.inlineCallbacks
    def op_stopDirectAccess(self, content, headers, msg):
        """
        Service operation: Stops direct access mode.
        """
        yield self.reply_err(msg, "Not yet implemented")

    @defer.inlineCallbacks
    def getAgentDescForInstrument(self, instrument_id):
        log.info("get_agent_desc_for_instrument() instrument_id="+str(instrument_id))
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
    def getAgentForInstrument(self, inst_resource_id):

        result = None
        instrument_resource = yield self.rc.get_instance(inst_resource_id)
        try:
            results = yield self.ac.find_associations(subject=instrument_resource, predicate_or_predicates=HAS_A_ID)

        except AssociationClientError:
            log.error('AssociationError')
            log.info('IIService getAgentForInstrument error retrieving association')
            defer.returnValue(result)

        if len(results)  != 1 :
            log.error('IIService  Instrument Agent association not found')
            defer.returnValue(result)

        for association in results:
            log.info('IIService Associated Source for Instrument: ' + \
                      association.ObjectReference.key + \
                      ' is: ' + association.SubjectReference.key)

        defer.returnValue(association.ObjectReference.key)



    @defer.inlineCallbacks
    def getAgentPidForInstrument(self, instrument_id):
        """
        agent_res = yield self.get_agent_for_instrument(instrument_id)
        if not agent_res:
            defer.returnValue(None)
        agent_pid = agent_res.proc_id
        log.info("Agent process id for instrument id %s is: %s" % (instrument_id, agent_pid))
        defer.returnValue(agent_pid)
        """

class InstrumentIntegrationClient(ServiceClient):
    """
    Class for the client accessing the instrument integration service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_integration_service"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def createNewInstrument(self, userInput):
#        reqcont = {}
#        reqcont['userInput'] = userInput

#        (cont, hdrs, msg) = yield self.rpc_send('createNewInstrument', reqcont)
        (cont, hdrs, msg) = yield self.rpc_send('createNewInstrument', userInput)
        defer.returnValue(cont)


    @defer.inlineCallbacks
    def startInstrumentAgent(self, instrument_id, instrumentResourceID, model):
        reqcont = {}
        reqcont['instrument_id'] = instrument_id
        reqcont['instrumentResourceID'] = instrumentResourceID
        reqcont['model'] = model
        #result = yield self._base_command('start_instrument_agent', reqcont)
        (cont, hdrs, msg)  = yield self.rpc_send('startInstrumentAgent', reqcont)
        defer.returnValue(cont)

    @defer.inlineCallbacks
    def stopInstrumentAgent(self, instrument_id):
        reqcont = {}
        reqcont['instrument_id'] = instrument_id
        result = yield self._base_command('stopInstrumentAgent', reqcont)
        defer.returnValue(result)


    @defer.inlineCallbacks
    def getInstrumentState(self, instrument_id):
        reqcont = {}
        reqcont['instrument_id'] = instrument_id
        (cont, hdrs, msg)  = yield self.rpc_send('getInstrumentState', reqcont)
        defer.returnValue(cont)

    @defer.inlineCallbacks
    def setInstrumentState(self, instrument_id, parameters):
        reqcont = {}
        reqcont['instrument_id'] = instrument_id
        reqcont['parameters'] = parameters

        result = yield self._base_command('setInstrumentState', reqcont)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def getInstrumentList(self):
        reqcont = {}
        (cont, hdrs, msg)  = yield self.rpc_send('getInstrumentList', reqcont)
        defer.returnValue(cont)

    @defer.inlineCallbacks
    def startAutoSampling(self, instrument_id):
        reqcont = {}
        reqcont['instrument_id'] = instrument_id

        (cont, hdrs, msg)  = yield self.rpc_send('startAutoSampling', reqcont)
        defer.returnValue(cont)

    @defer.inlineCallbacks
    def stopAutoSampling(self, instrument_id):
        reqcont = {}
        reqcont['instrument_id'] = instrument_id

        (cont, hdrs, msg)  = yield self.rpc_send('stopAutoSampling', reqcont)
        defer.returnValue(cont)

    @defer.inlineCallbacks
    def _base_command(self, op, content):
        (cont, hdrs, msg) = yield self.rpc_send(op, content)
        defer.returnValue(cont)

# Spawn of the process using the module name
factory = ProcessFactory(InstrumentIntegrationService)
