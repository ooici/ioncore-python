#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/instrument_direct_access.py
@author
@brief Service related to the management of a direct access session
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect
import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.core.process.process import Process
from ion.agents.instrumentagents.instrument_constants import InstErrorCode


from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


class InstrumentDirectAccessService(ServiceProcess):
    """ """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='instrument_direct_access',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        """ """
        log.debug ("__init__(): Instrument Direct Access Service")
        ServiceProcess.__init__(self, *args, **kwargs)
        self._proc = None
        self.ia_client = None

    @defer.inlineCallbacks
    def op_start_session(self, request, headers, msg):
        """ """

        response = None
        assert(isinstance(request, dict))
        log.debug ("start_session()")
        ia_name = request['instrumentAgent']
        if not self._proc:
            self._proc = Process()
            self._proc.spawn()
        self.ia_client = instrument_agent.InstrumentAgentClient (proc=self._proc, targetname=ia_name)
        log.debug ("self.ia_client= " + str (self.ia_client.proc.id))
        log.debug ("self.ia_client.target= " + str (self.ia_client.target))


        #response = self.start_session(**request)  # Unpack dict to kwargs

        # Begin an explicit transaction.
        log.debug ('----- Begin an explicit transaction.')
        reply = yield self.ia_client.start_transaction()
        success = reply['success']
        tid = reply['transaction_id']
        if InstErrorCode.is_error (success):
            response['success'] = success
            yield self.reply_ok(msg, response)
            return
        #todo: self.assertEqual(type (tid), str)
        #todo: self.assertEqual(len (tid), 36)
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_stop_session(self, request, headers, msg):
        """ """
        response = self.stop_session(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_relay_command(self, request, headers, msg):
        """ """
        response = self.relay_command(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def start_session(self, instrumentAgent='resourceRef'):
        """ """
        # Validate the input filter and augment context as required
        log.debug ("start_session()")
        self.ia_client = instrument_agent.InstrumentAgentClient (target = instrumentAgent)
        log.debug ("Before Process()")
        self._proc = Process()
        log.debug("ia_client: " + str (ia_client))

        
        # Snapshot and persist instrument state

        # Request DA channel from instrument


        # Return a channel ref

        yield self.reply_ok (msg, response)

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

        # Persist response

        # Return ack

        return



class InstrumentDirectAccessServiceClient(ServiceClient):
    """
    This is a service client for InstrumentDirectAccessServices.
    """
    def __init__(self, proc=None, **kwargs):
        """ """
        log.debug ("__init__(): Instrument Direct Access Service Client.")
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_direct_access"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def start_session(self, instrumentAgent='resourceRef'):
        """ """
        log.debug ("START: Direct Access session")
        (content, headers, msg) = yield self.rpc_send('start_session', {'instrumentAgent':instrumentAgent})
        log.debug ("rpc_send content:\n" + str (content))
        log.debug ("rpc_send headers:\n" + str (headers))
        log.debug ("rpc_send msg:\n" + str (msg))
        defer.returnValue (content)

    @defer.inlineCallbacks
    def stop_session(self, instrumentAgent='resourceRef', restoreState='false'):
        """ """
        log.debug ("FINISH: Direct Access session")
        (content, headers, msg) = yield self.rpc_send('stop_session', {'instrumentAgent':instrumentAgent, 'restoreState':restoreState})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def relay_command(self, commandString='command'):
        """ """
        (content, headers, msg) = yield self.rpc_send('relay_command', {'commandString':commandString})
        defer.returnValue(content)



# Spawn of the process using the module name
factory = ProcessFactory(InstrumentDirectAccessService)
  
  