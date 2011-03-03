#!/usr/bin/env python

"""
@file ion/integration/ais/findDataResources/findDataResources.py
@author David Everett
@brief Worker class to find resources for a given user id, bounded by
spacial and temporal parameters.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance

from ion.core.object import object_utils

class FindDataResources(object):
    
    def __init__(self, *args, **kwargs):
        self.rc = ResourceClient()
        log.info('FindDataResources.__init__()')
        
    #@defer.inlineCallbacks
    def findDataResources(self, msg):
        log.debug("findDataResources Worker Class!")        
        return 'something useful'        
