#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/instrument_direct_access.py
@author
@brief Service related to the management of a direct access session
"""

import ion.util.ionlog


log = ion.util.ionlog.getLogger (__name__)
from twisted.internet import defer
from twisted.python import reflect
import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.core.process.process import Process
from ion.agents.instrumentagents.instrument_constants import InstErrorCode, AgentCommand, AgentState, AgentEvent, AgentStatus, DriverChannel, DriverParameter
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


"""TODO: Change ServiceProcess to Process (use Instrument Agent as reference"""
class InstrumentDirectAccessService (ServiceProcess):
    """ """
    # Declaration of service
    declare = ServiceProcess.service_declare (name='instrument_direct_access',
                                              version='0.1.0',
                                              dependencies=[])

    def __init__(self, *args, **kwargs):
        """ """
        log.debug ("__init__(): Instrument Direct Access Service")
        ServiceProcess.__init__ (self, *args, **kwargs)
        self.instrumentName = self.spawn_args.get ("instrumentAgent")
        self.IA_Client = instrument_agent.InstrumentAgentClient (proc=self,
                                                                 targetname=self.instrumentName)
        log.debug (
            "----- DA now has InstrumentAgentClient with target= " + str (self.IA_Client.target))

    @defer.inlineCallbacks
    def op_start_session(self, request, headers, msg):
        """ """
        #assert(isinstance (request, dict)) # No params needed because IAclient is now defined in init
        log.debug ("op_start_session()")
        ia_name = request['instrumentAgent']
        success = yield self.start_session ()
        result = {'success': success}
        yield self.reply_ok (msg, result)

    @defer.inlineCallbacks
    def op_stop_session(self, request, headers, msg):
        """ """
        result = self.stop_session (**request)  # Unpack dict to kwargs
        yield self.reply_ok (msg, result)

    @defer.inlineCallbacks
    def op_relay_command(self, request, headers, msg):
        """ """
        result = self.relay_command (**request)  # Unpack dict to kwargs
        yield self.reply_ok (msg, result)

    @defer.inlineCallbacks
    def start_session(self):
        """ """
        log.debug ("start_session()")

        # Begin an explicit transaction.
        log.debug ('----- Begin an explicit transaction...')
        reply = yield self.IA_Client.start_transaction ()
        success = reply['success']
        self.tid = reply['transaction_id']
        goodTid = InstErrorCode.is_error (success)
        if InstErrorCode.is_error (success):
            log.debug ("      ...failed: " + str (success))
        else:
            assert (type (self.tid) == str), 'Transaction ID is an unexpected type.'
            assert (len (self.tid) == 36), 'Transaction ID is malformed.'
            log.debug ("      ...succeeded: " + str (success) + "with transaction ID: " + self.tid)

            # Snapshot and persist instrument state
            log.debug ('----- Request snapshot of current driver parameters...')
            params = [(DriverChannel.ALL, DriverParameter.ALL)]
            reply = yield self.IA_Client.get_device (params, self.tid)
            success = reply['success']
            result = reply['result']
            if InstErrorCode.is_error (success):
                log.debug ("      ...failed: " + str (success))
            else:
                log.debug ("      ...succeeded: " + str (success))

                # Strip off individual success vals to create a set params to restore original config later.
                self.orig_config = dict (map (lambda x: (x[0], x[1][1]), result.items ()))
                log.debug ('      ...params retrieved:\n' + str (self.orig_config))
                log.debug ('----- ' + str (len (self.orig_config)) + ' params cached.')

                # Get IA into direct access mode
                log.debug ('----- Get IA into direct access mode...')
                cmd = [AgentCommand.TRANSITION, AgentEvent.GO_DIRECT_ACCESS_MODE]
                reply = yield self.IA_Client.execute_observatory (cmd, self.tid)
                success = reply['success']
                if InstErrorCode.is_error (success):
                    log.debug ("      ...failed: " + str (success))
                else:
                    log.debug ("      ...succeeded: " + str (success))

                    # Verify that the agent is in observatory mode.
                    log.debug ('----- Verify that the agent is in direct access mode...')
                    params = [AgentStatus.AGENT_STATE]
                    reply = yield self.IA_Client.get_observatory_status (params, self.tid)
                    success = reply['success']
                    result = reply['result']
                    agent_state = result[AgentStatus.AGENT_STATE][1]
                    if InstErrorCode.is_error (success):
                        log.debug ("      ...verification request failed: " + str (success))
                    elif agent_state != AgentState.DIRECT_ACCESS_MODE:
                        log.debug ("      ...wrong state, now in: " + str (agent_state))
                        success = InstErrorCode.INCORRECT_STATE
                    else:
                        log.debug ('           ...correct state, now in: ' + str (agent_state))

        # Return a channel ref

        defer.returnValue (success)

    def stop_session(self, instrumentAgent='resourceRef', restoreState='false'):
        """ """
        # Retrieve state if required

        # Call instrument agent to close channel

        # Return

        return

    def relay_command(self, commandString='command'):
        """ """
        # Validate request; current instrument state

        # Persist command sequence

        # Relay command to channel

        # Persist result

        # Return ack

        return

    def start_direct_connection(self):
        """ """
        # Get the driver to launch socat
        AgentCommand.TRANSMIT_DATA

        # Pass ip:port info to user and explain how to launch socat


class InstrumentDirectAccessServiceClient (ServiceClient):
    """
    This is a service client for InstrumentDirectAccessServices.
    """

    def __init__(self, proc=None, **kwargs):
        """ """
        log.debug ("__init__(): Instrument Direct Access Service Client.")
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_direct_access"
        ServiceClient.__init__ (self, proc, **kwargs)

    @defer.inlineCallbacks
    def start_session(self, instrumentAgent='resourceRef'):
        """ """
        log.debug ("START: Direct Access session")
        (content, headers, msg) = yield self.rpc_send ('start_session',
                {'instrumentAgent': instrumentAgent})
        #log.debug ("rpc_send content:\n" + str (content))
        #log.debug ("rpc_send headers:\n" + str (headers))
        #log.debug ("rpc_send msg:\n" + str (msg))
        defer.returnValue (content)

    @defer.inlineCallbacks
    def stop_session(self, instrumentAgent='resourceRef', restoreState='false'):
        """ """
        log.debug ("FINISH: Direct Access session")
        (content, headers, msg) = yield self.rpc_send ('stop_session',
                {'instrumentAgent': instrumentAgent,
                 'restoreState': restoreState})
        defer.returnValue (content)

    @defer.inlineCallbacks
    def relay_command(self, commandString='command'):
        """ """
        (content, headers, msg) = yield self.rpc_send ('relay_command',
                {'commandString': commandString})
        defer.returnValue (content)


# Spawn of the process using the module name
factory = ProcessFactory (InstrumentDirectAccessService)
  
