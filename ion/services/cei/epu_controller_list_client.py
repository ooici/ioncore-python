#!/usr/bin/env python

"""
@file ion/integration/eoi/epu_controller_list/controller_list_client.py
@brief Provides client interface to controller_list service that provides a
       list of active EPU Controller service names in the system
"""

import sys

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.core.process.service_process import ServiceClient


class EPUControllerListClient(ServiceClient):
    """Client for querying EPUControllerListService
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "epu_controller_list"
        ServiceClient.__init__(self, proc, **kwargs)
        self.ServiceName = kwargs['targetname']

    @defer.inlineCallbacks
    def list(self):
        """Query the EPUControllerListService
        """
        log.debug("ServiceName = " + self.ServiceName)
        yield self._check_init()
        ServiceExists = yield self.does_service_exist(self.ServiceName)
        if not ServiceExists:
            log.debug("controller_list_client.list: Returning static list for AIS unit testing")
            defer.returnValue(['dataservices_epu_controller',
                               'agentservices_epu_controller',
                               'associationservices_epu_controller'])
        log.debug("controller_list_client.list: Sending EPU controller list query")
        (content, headers, msg) = yield self.rpc_send('list', {})
        log.debug("controller_list_client.list: list returned "+str(content))
        defer.returnValue(content)

