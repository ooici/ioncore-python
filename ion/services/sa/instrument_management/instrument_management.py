#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/instrument_management.py
@author
@brief Services related to the orchastration of instrument activities
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
#from twisted.python import reflect


from ion.core.process.process import Process, ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.services.dm.distribution.events import BusinessStateChangeSubscriber

import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.agents.instrumentagents.instrument_constants import AgentParameter, \
    AgentConnectionState, AgentState, driver_client, \
    DriverAnnouncement, InstErrorCode, DriverParameter, DriverChannel, \
    ObservatoryState, DriverStatus, InstrumentCapability, DriverCapability, \
    MetadataParameter, AgentCommand, Datatype, TimeSource, ConnectionMethod, \
    AgentEvent, AgentStatus, ObservatoryCapability




#REMOVE WHEN NEW DM COMES ALONG
from ion.services.sa.instrument_management.hokey_dm import HokeyDM


class InstrumentStateChangeSubscriber(BusinessStateChangeSubscriber):
    """
    For subscribing to events
    """
    def __init__(self, *args, **kwargs):
        BusinessStateChangeSubscriber.__init__(self, *args, **kwargs)

        def ondata(self, data):
            log.debug("InstrumentStateChange event: \n%s", str(data))


class InstrumentManagementService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='instrument_mgmt',
                                             version='0.1.0',
                                             dependencies=[])

    DM_INSTRUMENT        = "instrument"
    DM_PLATFORM          = "platform"

    def __init__(self, *args, **kwargs):

        ServiceProcess.__init__(self, *args, **kwargs)

        log.debug('InstrumentManagementService.__init__()')

        #pre-load tables in my hokey DB
        self.db = HokeyDM([self.DM_INSTRUMENT,
                           self.DM_PLATFORM,
                           ])

        self.agents = {}
        self.subscribers = {}

    @defer.inlineCallbacks
    def op_define_instrument(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = yield self.define_instrument(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_get_instrument(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = yield self.get_instrument(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_update_instrument(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = yield self.update_instrument(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_find_instrument(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = yield self.find_instrument(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_retire_instrument(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = yield self.retire_instrument(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_activate_instrument(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = yield self.activate_instrument(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_define_platform(self, request, headers, msg):
        response = yield self.define_platform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_get_platform(self, request, headers, msg):
        response = yield self.get_platform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_update_platform(self, request, headers, msg):
        response = yield self.update_platform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_find_platform(self, request, headers, msg):
        response = yield self.find_platform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_retire_platform(self, request, headers, msg):
        response = yield self.retire_platform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_request_direct_access(self, request, headers, msg):
        response = yield self.request_direct_access(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    #######################################################


    # build an error message to return.  this will probably be standardized later.
    def make_error(self, message):
        return {"success"        : False,
                "error_message"  : message}

    @defer.inlineCallbacks
    def define_instrument(self, serialNumber='defaultSerial', make='defaultMake', model='defaultModel'):
        log.info("IMS define_instrument")

        # Validate the input filter and augment context as required
        def equals_new_item(_, v):
            return (v["serialNumber"] == serialNumber and
                    v["make"]         == make and
                    v["model"]        == model)

        log.debug("Looking for an existing instrument with the specified values")
        response = None
        if 0 < self.db.count(self.DM_INSTRUMENT, equals_new_item):
            response = self.make_error("An instrument already exists with values "
                                       + "serialNumber=" + str(serialNumber) + " "
                                       + "make=" + str(make) + " "
                                       + "model" + str(model) + " ")

        else:
            # Define YAML/params from
            # Instrument metadata draft: https://confluence.oceanobservatories.org/display/CIDev/Instrument+Metadata

            # Create instrument resource,
            new_id = self.db.insert(self.DM_INSTRUMENT, {"serialNumber" : serialNumber,
                                                         "make"         : make,
                                                         "model"        : model,
                                                         "agent"        : None
                                                         })
            
            # Create associations
            # IK: for ... ?

            # Return a resource ref
            response = {"success" : True, "resourceId" : new_id}

        yield defer.returnValue(response)


    @defer.inlineCallbacks
    def activate_instrument(self, resourceId='instrument', agentId='agent'):

        # Validate the input: make sure instrument exists
        inst = self.db.get_one(self.DM_INSTRUMENT, resourceId)
        response = None
        if not inst:
            response = self.make_error("No instrument found with resourceId "
                                       + str(resourceId))
        else:
            #MM: instantiate instrument agent
            # IK: don't think IMS is the right place to do this (yet)

            # IK: instead, do this:
            # Validate the input: make sure agent exists


            #record instrument agent params in store
            log.info("Storing instrument agent ID")
            self.db.update(self.DM_INSTRUMENT, resourceId, {"agent" : agentId})

            log.info("Creating subprocess for instrument")
            subproc = Process()
            yield subproc.spawn()

            log.info("creating instrument agent (client) instance")
            log.debug("target=%s", str(agentId))
            iac = instrument_agent.InstrumentAgentClient(proc=subproc, target=agentId)

            #FIXME : create state machine for instrument, 
            #        pass as argument to subscriber

            log.info("subscribing to instrument events in a subprocess")
            inst_state_change_handler = InstrumentStateChangeSubscriber(
                origin=("agent.%s" % str(agentId)),
                process=subproc)
            log.info("getting initial instrument state")
            log.debug("getting a transaction")
            #FIXME: what to do if transaction fails?
            transaction = yield iac.start_transaction(5)
            log.debug("got transaction: %s" + str(transaction))
            transaction_id = transaction["transaction_id"]

            queries = [AgentStatus.AGENT_STATE,
                       AgentStatus.CONNECTION_STATE]

            reply1 = yield iac.get_observatory_status(queries, transaction_id)
            if "OK" == reply1["success"][0]:
                for q in queries:
                    log.debug("%s: %s", q, reply1["result"][q])

            log.info("Agent connection state: %s", reply1["result"][AgentStatus.CONNECTION_STATE][1])
            log.info("Agent state: %s",            reply1["result"][AgentStatus.AGENT_STATE][1])
            
            log.info("closing transaction")
            yield iac.end_transaction(transaction_id)

            #FIXME: what does it mean to be in these states?
            
            
            log.info("Recording agent and listener process")
            self.agents[resourceId] = iac
            self.subscribers[resourceId] = inst_state_change_handler

            response = {"success" : True}

        yield defer.returnValue(response)



    @defer.inlineCallbacks
    def get_instrument(self, resourceId='instrument'):
        # Validate the input: make sure instrument exists
        inst = self.db.get_one(self.DM_INSTRUMENT, resourceId)
        if not inst:
            response = self.make_error("No instrument found with resourceId "
                                       + str(resourceId))
        else:
            response = {"success" : True, "instrument": inst}

        yield defer.returnValue(response)


    @defer.inlineCallbacks
    def update_instrument(self, resourceId='instrument', serialNumber=None, make=None, model=None):
        log.debug("Update instrument serialNumber=" + str(serialNumber) + " make=" + str(make) + " model=" + str(model))
        # Validate the input: make sure instrument exists
        if not self.db.key_exists(self.DM_INSTRUMENT, resourceId):
            response = self.make_error("No instrument found with resourceId "
                                       + str(resourceId))
        else:
            new_info = {}
            if serialNumber: new_info["serialNumber"] = serialNumber
            if make:         new_info["make"] = make
            if model:        new_info["model"] = model

            result = self.db.update(self.DM_INSTRUMENT, resourceId, new_info)

            #check the success of the update, just for fun
            if result:
                response = {"success" : True}
            if not result:
                response = self.make_error("Failed to update instrument with resourceId "
                                           + str(resourceId))

        yield defer.returnValue(response)


    @defer.inlineCallbacks
    def find_instrument(self, serialNumber=None, make=None, model=None):
        def selection_criteria(_, v):
            return ((not serialNumber) or v["serialNumber"] == serialNumber) \
                and ((not make)        or v["make"] == make) \
                and ((not model)       or v["model"] == model)

        results = self.db.select(self.DM_INSTRUMENT, selection_criteria)

        response = {"success" : True, "instruments" : results}

        yield defer.returnValue(response)


    @defer.inlineCallbacks
    def retire_instrument(self, resourceId='instrument'):
        # Validate the input: make sure instrument exists
        if not self.db.key_exists(self.DM_INSTRUMENT, resourceId):
            response = self.make_error("No instrument found with resourceId "
                                       + str(resourceId))
        else:
            new_info = {}
            new_info["retired"] = "FIXME today's date"

            result = self.db.update(self.DM_INSTRUMENT, resourceId, new_info)

            #check the success of the update, just for fun
            if result:
                response = {"success" : True}
            if not result:
                response = self.make_error("Failed to retire instrument with resourceId "
                                           + str(resourceId))

        yield defer.returnValue(response)



    @defer.inlineCallbacks
    def define_platform(self, serialNumber='defaultSerial', name='defaultName'):

        # Validate the input filter and augment context as required
        response = None
        def equals_new_item(_, v):
            return (v["serialNumber"] == serialNumber and
                    v["name"]         == name)

        if 0 < self.db.count(self.DM_PLATFORM, equals_new_item):
            response = (self.make_error("A platform already exists with values "
                                        + "serialNumber=" + str(serialNumber) + " "
                                        + "name=" + str(name)))
        else:


            # Define YAML/params from
            # Metadata from Steve Foley:
            # Anchor system, weight (water/air), serial, model, power characteristics (battery capacity), comms characteristics (satellite  systems, schedule, GPS), clock stuff), deploy length, data capacity, processing capacity


            #  Create platform and set initial state

            new_id = self.db.insert(self.DM_PLATFORM, {"serialNumber" : serialNumber,
                                                       "name"         : name,
                                                       })

            #set initial instrument state, persist
            # IK: where does initial instrument state come from?

            # Create associations
            # IK: for ... ?

            # Return a resource ref
            response = {"success" : True, "resourceId" : new_id}

        yield defer.returnValue(response)




    @defer.inlineCallbacks
    def get_platform(self, resourceId='platform'):
        # Validate the input: make sure platform exists
        inst = self.db.get_one(self.DM_PLATFORM, resourceId)
        if not inst:
            response = self.make_error("No platform found with resourceId "
                                       + str(resourceId))
        else:
            response = {"success" : True, "platform": inst}

        yield defer.returnValue(response)

    @defer.inlineCallbacks
    def update_platform(self, resourceId=None, serialNumber=None, name=None):
        # Validate the input: make sure platform exists
        if not self.db.key_exists(self.DM_PLATFORM, resourceId):
            response = self.make_error("No platform found with resourceId "
                                       + str(resourceId))
        else:
            new_info = {}
            if serialNumber: new_info["serialNumber"] = serialNumber
            if name:         new_info["name"] = name

            result = self.db.update(self.DM_PLATFORM, resourceId, new_info)

            #check the success of the update, just for fun
            if result:
                response = {"success" : True}
            if not result:
                response = self.make_error("Failed to update platform with resourceId "
                                           + str(resourceId))

        yield defer.returnValue(response)

    @defer.inlineCallbacks
    def find_platform(self, serialNumber=None, name=None):
        def selection_criteria(_, v):
            return ((not serialNumber) or v["serialNumber"] == serialNumber) \
                and ((not name)        or v["name"] == name)

        results = self.db.select(self.DM_PLATFORM, selection_criteria)

        response = {"success" : True, "platforms" : results}
        yield defer.returnValue(response)


    @defer.inlineCallbacks
    def retire_platform(self, resourceId='platform'):
        # Validate the input: make sure platform exists
        if not self.db.key_exists(self.DM_PLATFORM, resourceId):
            response = self.make_error("No platform found with resourceId "
                                       + str(resourceId))
        else:
            new_info = {}
            new_info["retired"] = "FIXME today's date"

            result = self.db.update(self.DM_PLATFORM, resourceId, new_info)

            #check the success of the update, just for fun
            if result:
                response = {"success" : True}
            if not result:
                response = self.make_error("Failed to retire platform with resourceId "
                                           + str(resourceId))

        yield defer.returnValue(response)




    @defer.inlineCallbacks
    def request_direct_access(self, identity='user', resourceId='instrument'):

        # Validate request; current instrument state, policy, and other

        # Validate the input: make sure instrument exists
        inst = self.db.get_one(self.DM_INSTRUMENT, resourceId)
        response = None
        if not inst:
            response = self.make_error("No instrument found with resourceId "
                                       + str(resourceId))
        elif not (resourceId in self.agents):
            response = self.make_error("This instrument has not been activated -- no agent!  "
                                       "resourceId was " + str(resourceId))

        else:
            log.debug("getting agent")
            iac = self.agents[resourceId]

            #check state
            transaction = yield iac.start_transaction(5)
            log.debug("got transaction: %s" + str(transaction))
            transaction_id = transaction["transaction_id"]

            reply1 = yield iac.get_observatory_status([AgentStatus.AGENT_STATE], transaction_id)

            if not "OK" == reply1["success"][0]:
                response = self.make_error("Instrument request failed! FIXME make more descriptive")
            elif not AgentState.OBSERVATORY_MODE == reply1["result"][AgentStatus.AGENT_STATE][1]:
                response = self.make_error("Instrument not in OBSERVATORY_MODE")
            else:

                #FIXME: what does it mean to be in these states?
            
                
                response = {"success" : True}

            log.info("closing transaction")
            yield iac.end_transaction(transaction_id)

        yield defer.returnValue(response)











        #IK: for this release, check that it's in observatory mode

        # IK: this is done by direct access: Retrieve and save current instrument settings

        # Request DA channel, save reference

        # Return direct access channel

        return



    @defer.inlineCallbacks
    def request_command(self, identity='user', resourceId='instrument', commands=[]):
        # validate request

        # check whether user has access to specified commands
        
        # send commands to instrument

        # FIXME: do we re-invent the transaction_id system used by instrument agent
        #        or just use it (with potentially disastrous results, as we don't have 
        #        access to the timeout callback)?  
        #
        #        leaning toward the following:
        #         - re-invention of transaction code
        #         - failing a transaction will tell you what user is blocking
        #         - transaction timeouts of instrument agents are hidden
        #         - failures to re-establish transactions are raised
        #         - successes in command list are recorded as they happen
        #
        #         - async implementation!
        #           - get results from subscription to IA event stream, not immediately
        #           - make a "check" function to check on progress 
        #           - make a get_results function to grab & clear async
        #           - get_results returns {} if still running but results not available
        #           - get_results returns errors if not running and results not available
        #           - get_results returns results otherwise: table of [command_num] = result



        return

        


class InstrumentManagementServiceClient(ServiceClient):

    """
    This is a service client for InstrumentManagementServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_mgmt"
        ServiceClient.__init__(self, proc, **kwargs)


    #methods for creating, retrieving, updating, finding, and retiring INSTRUMENTS
    @defer.inlineCallbacks
    def define_instrument(self, serialNumber='defaultSerial', make='defaultMake', model='defaultModel'):
        (content, headers, msg) = yield self.rpc_send('define_instrument', {'serialNumber':serialNumber, 'make':make, 'model':model})
        log.debug("got response from define_instrument service")

        yield defer.returnValue(content)

    #IK: adding an instrument agent registration call so that the physical platform can
    #    tell us what is running.  this allows defining and activating an instrument separately
    @defer.inlineCallbacks
    def activate_instrument(self, resourceId='instrument', agentId='agent'):
        (content, headers, msg) = yield self.rpc_send('activate_instrument', {'resourceId':resourceId, 'agentId':agentId})
        yield defer.returnValue(content)


    @defer.inlineCallbacks
    def get_instrument(self, resourceId='instrument'):
        (content, headers, msg) = yield self.rpc_send('get_instrument', {'resourceId':resourceId})
        yield defer.returnValue(content)

    @defer.inlineCallbacks
    def update_instrument(self, resourceId='instrument', serialNumber=None, make=None, model=None):
        (content, headers, msg) = yield self.rpc_send('update_instrument', {'resourceId':resourceId, 'serialNumber':serialNumber, 'make':make, 'model':model})
        yield defer.returnValue(content)

    @defer.inlineCallbacks
    def find_instrument(self, serialNumber=None, make=None, model=None):
        (content, headers, msg) = yield self.rpc_send('find_instrument', {'serialNumber':serialNumber, 'make':make, 'model':model})
        yield defer.returnValue(content)

    @defer.inlineCallbacks
    def retire_instrument(self, resourceId='instrument'):
        (content, headers, msg) = yield self.rpc_send('retire_instrument', {'resourceId':resourceId})
        yield defer.returnValue(content)



    #methods for creating, retrieving, updating, finding, and retiring PLATFORMS
    @defer.inlineCallbacks
    def define_platform(self, serialNumber='defaultSerial', name='defaultName'):
        (content, headers, msg) = yield self.rpc_send('define_platform', {'serialNumber':serialNumber, 'name':name})
        yield defer.returnValue(content)

    @defer.inlineCallbacks
    def get_platform(self, resourceId='platform'):
        (content, headers, msg) = yield self.rpc_send('get_platform', {'resourceId':resourceId})
        yield defer.returnValue(content)

    @defer.inlineCallbacks
    def update_platform(self, resourceId='platform', serialNumber=None, name=None):
        (content, headers, msg) = yield self.rpc_send('update_platform', {'resourceId':resourceId, 'serialNumber':serialNumber,'name':name})
        yield defer.returnValue(content)

    @defer.inlineCallbacks
    def find_platform(self, serialNumber=None, name=None):
        (content, headers, msg) = yield self.rpc_send('find_platform', {'serialNumber':serialNumber, 'name':name})
        yield defer.returnValue(content)

    @defer.inlineCallbacks
    def retire_platform(self, resourceId='platform'):
        (content, headers, msg) = yield self.rpc_send('retire_platform', {'resourceId':resourceId})
        yield defer.returnValue(content)




    @defer.inlineCallbacks
    def request_direct_access(self, identity='user', resourceId='instrument'):
        (content, headers, msg) = yield self.rpc_send('request_direct_access', {'identity':identity, 'resourceId':resourceId})
        yield defer.returnValue(content)




# Spawn of the process using the module name
factory = ProcessFactory(InstrumentManagementService)

