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


from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


class InstrumentDirectAccessService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='instrument_direct_access',
                                             version='0.1.0',
                                             dependencies=[])


    def __init__(self, *args, **kwargs):

        ServiceProcess.__init__(self, *args, **kwargs)

        log.debug('InstrumentDirectAccessService.__init__()')


    @defer.inlineCallbacks
    def op_start_session(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = self.start_session(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_stop_session(self, request, headers, msg):
        response = self.stop_session(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_relay_command(self, request, headers, msg):
        response = self.relay_command(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)



    def start_session(self, instrumentAgent='resourceRef' ):

        # Validate the input filter and augment context as required

        # Snapshot and persist instrument state

        # Request DA channel from instrument


        # Return a channel ref

        return

    def stop_session(self, instrumentAgent='resourceRef', restoreState='false'):

        # Retrieve state if required

        # Call instrument agent to close channel

        # Return

        return

    def relay_command(self, commandString='command'):

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
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_direct_access"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def start_session(self, instrumentAgent='resourceRef'):
        (content, headers, msg) = yield self.rpc_send('start_session', {'instrumentAgent':instrumentAgent})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def stop_session(self, instrumentAgent='resourceRef', restoreState='false'):
        (content, headers, msg) = yield self.rpc_send('stop_session', {'instrumentAgent':instrumentAgent, 'restoreState':restoreState})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def relay_command(self, commandString='command'):
        (content, headers, msg) = yield self.rpc_send('relay_command', {'commandString':commandString})
        defer.returnValue(content)



# Spawn of the process using the module name
factory = ProcessFactory(InstrumentDirectAccessService)
  
  