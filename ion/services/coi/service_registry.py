#!/usr/bin/env python

"""
@file ion/services/coi/service_registry.py
@author Michael Meisinger
@brief service for registering service (types and instances).
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver
from ion.data.store import Store

from ion.core import base_process
from ion.data.dataobject import DataObject
import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.coi.resource_registry import ResourceDesc, ResourceTypeDesc

class ServiceRegistryService(BaseService):
    """
    Service registry service interface
    @todo a service is a resource and should also be living in the resource registry
    """
    # Declaration of service
    declare = BaseService.service_declare(name='service_registry', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.datastore = Store()

    @defer.inlineCallbacks
    def op_register_service(self, content, headers, msg):
        """
        Service operation: register a service by name
        """
        svcdesc = content['svc_desc'].copy()
        logging.info('op_register_service: '+str(svcdesc))

        yield self.datastore.put(svcdesc['name'],svcdesc)
        yield self.reply_ok(msg)

    def op_get_service_desc(self, content, headers, msg):
        """
        Service operation: Get service description. May include a service
        specification.
        """

    @defer.inlineCallbacks
    def op_register_instance(self, content, headers, msg):
        """
        Service operation:
        """
        svcinstdesc = content['svcinst_desc'].copy()
        logging.info('op_register_instance: '+str(svcinstdesc))

        yield self.datastore.put(svcinstdesc['svc_name'], svcinstdesc)
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_get_instance(self, content, headers, msg):
        """
        Service operation: Returns the exchange name of the service
        """
        svcname = str(content['svc_name'])
        logging.info('op_get_instance: '+str(svcname))

        svcid = yield self.datastore.get(svcname)
        yield self.reply_ok(msg, {'svcinst_desc':svcid})

# Spawn of the process using the module name
factory = ProtocolFactory(ServiceRegistryService)


class ServiceRegistryClient(BaseServiceClient):
    """
    Client class for accessing the service registry. This is most important for
    finding and accessing any other services. This client knows how to find the
    service registry
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "service_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def register_service(self, svc):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('register_service',
                        {'svc_desc':svc.encode()})

    @defer.inlineCallbacks
    def register_service_instance(self, svc_inst):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('register_instance',
                        {'svcinst_desc':svc_inst.encode()})

    @defer.inlineCallbacks
    def get_service_instance(self, service_name):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_instance',
                        {'svc_name': service_name})
        defer.returnValue(content['svcinst_desc'])

    @defer.inlineCallbacks
    def get_service_instance_name(self, service_name):
        sidesc = yield self.get_service_instance(service_name)
        defer.returnValue(sidesc['xname'])

class ServiceInstanceDesc(ResourceDesc):
    """Structured object for a service instance.

    Attributes:
    .name   name of the service
    """
    def __init__(self, **kwargs):
        kw = kwargs.copy() if kwargs else {}
        kw['res_type'] = 'rt_serviceinst'
        ResourceDesc.__init__(self, **kw)
        if len(kwargs) != 0:
            self.setServiceInstanceDesc(**kwargs)

    def setServiceInstanceDesc(self, **kwargs):
        if 'xname' in kwargs:
            self.set_attr('xname',kwargs['xname'])
        else:
            raise RuntimeError("Service exchange name missing")

        if 'svc_name' in kwargs:
            self.set_attr('svc_name',kwargs['svc_name'])
        else:
            raise RuntimeError("Service name missing")

class ServiceDesc(ResourceTypeDesc):
    """Structured object for a service description.

    Attributes:
    .name   name of the service
    """
    def __init__(self, **kwargs):
        kw = kwargs.copy() if kwargs else {}
        kw['res_type'] = 'rt_service'
        ResourceTypeDesc.__init__(self, **kw)
        if len(kwargs) != 0:
            self.setServiceDesc(**kwargs)

    def setServiceDesc(self, **kwargs):
        if 'xname' in kwargs:
            self.set_attr('xname',kwargs['xname'])
        else:
            self.set_attr('xname',kwargs['name'])
