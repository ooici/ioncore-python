#!/usr/bin/env python

"""
@file ion/services/coi/service_registry.py
@author Michael Meisinger
@brief service for registering service (types and instances).
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver
from magnet.store import Store

from ion.core import base_process
import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class ServiceRegistryService(BaseService):
    """Service registry service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='service_registry', version='0.1.0', dependencies=[])

    datastore = Store()

    @defer.inlineCallbacks
    def op_register_service(self, content, headers, msg):
        svcdesc = content['svc_desc'].copy()
        logging.info('op_register_service: '+str(svcdesc))

        yield self.datastore.put(svcdesc['name'],svcdesc)

    def op_get_service_spec(self, content, headers, msg):
        pass

    def op_register_instance(self, content, headers, msg):
        svcinstdesc = content['svcinst_desc'].copy()
        logging.info('op_register_instance: '+str(svcinstdesc))

        yield self.datastore.put(svcdesc['name'],svcinstdesc)

    def op_get_instance(self, content, headers, msg):
        svcname = content['svc_name']
        logging.info('op_get_instance: '+str(svcname))

        svcid = yield self.datastore.get(svcname)
        yield self.reply_message(msg, 'result', {'svc_id':str(svcid)}, {})


class ServiceRegistryClient(BaseServiceClient):
    """Class for
    """

    @defer.inlineCallbacks
    def registerServiceInstance(self, svc_inst):
        self.rpc = RpcClient()
        yield self.rpc.attach()

        (content, headers, msg) = yield self.rpc.rpc_send('', 'register_resource',
                                                          {'res_desc':res_desc.__dict__}, {})
        logging.info('Service reply: '+str(content))

    @defer.inlineCallbacks
    def get_service_instance(self, service_name):
        """
        pfh
        @bug Does not work as I expected. Debug me!
        """
        self.rpc = RpcClient()
        yield self.rpc.attach()
        msg = {'svc_name': service_name,}
        (c, h, m) = yield self.rpc.rpc_send('', 'get_instance', msg, {})
        defer.returnValue(c)

# Spawn of the process using the module name
factory = ProtocolFactory(ServiceRegistryService)
