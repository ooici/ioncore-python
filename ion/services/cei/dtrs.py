#!/usr/bin/env python

"""
@file ion/services/cei/dtrs.py
@author Alex Clemesha
@author David LaBissoniere
@brief Deployable Type Registry Service. Used to look up Deployable type data/metadata.
"""

import os
import logging

from twisted.internet import defer

from magnet.spawnable import Receiver
from ion.services.base_service import BaseService, BaseServiceClient
from ion.util.config import Config
from ion.core import ioninit

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

CONF = ioninit.config(__name__)
REGISTRY = Config(CONF['deployable_types']).getObject()

class DeployableTypeRegistryService(BaseService):
    """Deployable Type Registry service interface
    """
    declare = BaseService.service_declare(name='dtrs', version='0.1.0', dependencies=[])

    def slc_init(self):
        pass

    def op_lookup(self, content, headers, msg):
        """Resolve a depoyable type
        """
        # just using a file for this right now, to keep it simple
        dtId = content['deployable_type']
        nodes = content.get('nodes')
        try:
            dt = REGISTRY[dtId]
        except KeyError:
            #TODO how to throw errors..?
            logging.error('Invalid deployable type specified: ' + dtId)
            defer.fail()
        
        result = {'document' : dt['document'], 'nodes' : nodes}
        sites = dt['sites']

        for node_name in nodes.iterkeys():
            node = nodes[node_name]
            # uhhh...
            node_site = node['site']
            image = sites[node_site][node_name]['image']
            node['image'] = image

        return self.reply(msg, 'result', result)

class DeployableTypeRegistryClient(BaseServiceClient):
    """Client for accessing DTRS
    """
    def __init__(self, proc=None, pid=None):
        BaseServiceClient.__init__(self, "dtrs", proc, pid)
        
    @defer.inlineCallbacks
    def lookup(self, dt, nodes=None):
        """Lookup a deployable type
        """
        self._check_init()
        (content, headers, msg) = yield self.proc.rpc_send(self.svc, 'lookup', {
            'deployable_type' : dt,
            'nodes' : nodes
        })
        defer.returnValue(content)

# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = DeployableTypeRegistryService(receiver)
