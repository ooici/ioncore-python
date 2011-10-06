#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/instrument_management.py
@author
@brief Services related to the orchastration of instrument activities
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect


from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


class InstrumentManagementService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='instrument_mgmt',
                                             version='0.1.0',
                                             dependencies=[])


    def __init__(self, *args, **kwargs):

        ServiceProcess.__init__(self, *args, **kwargs)

        log.debug('InstrumentManagementService.__init__()')


    @defer.inlineCallbacks
    def op_define_instrument(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = self.define_instrument(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_define_platform(self, request, headers, msg):
        response = self.define_platform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_request_direct_access(self, request, headers, msg):
        response = self.request_direct_access(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)



    def define_instrument(self, serialNumber='defaultSerial', make='defaultMake', model='defaultModel'):

        # Validate the input filter and augment context as required

        # Define YAML/params from
        # Instrument metadata draft: https://confluence.oceanobservatories.org/display/CIDev/Instrument+Metadata

        # Create instrument resource, set initial state, persist

        # Create associations

        # Return a resource ref

        return

    def define_platform(self, serialNumber='defaultSerial'):

        # Validate the input filter and augment context as required

        # Define YAML/params from
        # Metadata from Steve Foley:
        # Anchor system, weight (water/air), serial, model, power characteristics (battery capacity), comms characteristics (satellite  systems, schedule, GPS), clock stuff), deploy length, data capacity, processing capacity

        #  Create instrument resource and set initial state

        # Create associations

        # Return a resource ref

        return

    def request_direct_access(self, identity='user', resourceId='instrument'):

        # Validate request; current instrument state, policy, and other

        # Retrieve and save current instrument settings

        # Request DA channel, save reference

        # Return direct access channel

        return



class InstrumentManagementServiceClient(ServiceClient):

    """
    This is a service client for InstrumentManagementServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_mgmt"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_instrument(self, serialNumber='defaultSerial', make='defaultMake', model='defaultModel'):
        (content, headers, msg) = yield self.rpc_send('define_instrument', {'serialNumber':serialNumber, 'make':make, 'model':model})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def define_platform(self, serialNumber='defaultSerial'):
        (content, headers, msg) = yield self.rpc_send('define_platform', {'serialNumber':serialNumber})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def request_direct_access(self, identity='user', resourceId='instrument'):
        (content, headers, msg) = yield self.rpc_send('request_direct_access', {'identity':identity, 'resourceId':resourceId})
        defer.returnValue(content)



# Spawn of the process using the module name
factory = ProcessFactory(InstrumentManagementService)
  