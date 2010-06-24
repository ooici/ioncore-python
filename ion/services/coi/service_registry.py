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

from ion.core import base_process
import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.coi import resource_registry


from ion.data.datastore import registry
from ion.data import dataobject
from ion.data import store

from ion.core import ioninit
CONF = ioninit.config(__name__)


class ServiceRegistryService(resource_registry.ResourceRegistryService):
    """
    Service registry service interface
    @todo a service is a resource and should also be living in the resource registry
    """
    # Declaration of service
    declare = BaseService.service_declare(name='service_registry', version='0.1.0', dependencies=[])


# Spawn of the process using the module name
factory = ProtocolFactory(ServiceRegistryService)


class ServiceRegistryClient():
    """
    Client class for accessing the service registry. This is most important for
    finding and accessing any other services. This client knows how to find the
    service registry
    """
    def __init__(self, proc=None, **kwargs):
        kwargs['targetname'] = "service_registry"
        # rrc = Resource Registry Client
        self.rrc = resource_registry.ResourceRegistryClient(proc, **kwargs)

    @defer.inlineCallbacks
    def register_service(self, svc):
        svc_name = yield self.rrc.register_resource(svc.svc_name,svc)
        defer.returnValue(svc_name)

    @defer.inlineCallbacks
    def get_service(self, service_name):
        svc_desc = yield self.rrc.get_resource(service_name)
        defer.returnValue(svc_desc)

    @defer.inlineCallbacks
    def register_service_instance(self, svc_inst):
        svcinst_name = yield self.rrc.register_resource(svc_inst.svc_name,svc_inst)
        defer.returnValue(svcinst_name)

    @defer.inlineCallbacks
    def get_service_instance(self, service_name):
        svcinst_desc = yield self.rrc.get_resource(service_name)
        defer.returnValue(svcinst_desc)

    @defer.inlineCallbacks
    def get_service_instance_name(self, service_name):
        sidesc = yield self.get_service_instance(service_name)
        defer.returnValue(sidesc.xname)


class ServiceDesc(registry.ResourceDescription):
    """Structured object for a service instance.

    Attributes:
    .name   name of the service
    """    
    svc_name = dataobject.TypedAttribute(str)
    res_type = dataobject.TypedAttribute(str,'rt_service')

        


class ServiceInstanceDesc(registry.ResourceDescription):
    """Structured object for a service instance.

    Attributes:
    .name   name of the service
    """    
    xname = dataobject.TypedAttribute(str)
    svc_name = dataobject.TypedAttribute(str)
    res_type = dataobject.TypedAttribute(str,'rt_serviceinst')



#class ServiceInstanceDesc(ResourceDesc):
#    """Structured object for a service instance.
#
#    Attributes:
#    .name   name of the service
#    """
#    def __init__(self, **kwargs):
#        kw = kwargs.copy() if kwargs else {}
#        kw['res_type'] = 'rt_serviceinst'
#        ResourceDesc.__init__(self, **kw)
#        if len(kwargs) != 0:
#            self.setServiceInstanceDesc(**kwargs)
#
#    def setServiceInstanceDesc(self, **kwargs):
#        if 'xname' in kwargs:
#            self.set_attr('xname',kwargs['xname'])
#        else:
#            raise RuntimeError("Service exchange name missing")
#
#        if 'svc_name' in kwargs:
#            self.set_attr('svc_name',kwargs['svc_name'])
#        else:
#            raise RuntimeError("Service name missing")

#class ServiceDesc(ResourceTypeDesc):
#    """Structured object for a service description.
#
#    Attributes:
#    .name   name of the service
#    """
#    def __init__(self, **kwargs):
#        kw = kwargs.copy() if kwargs else {}
#        kw['res_type'] = 'rt_service'
#        ResourceTypeDesc.__init__(self, **kw)
#        if len(kwargs) != 0:
#            self.setServiceDesc(**kwargs)
#
#    def setServiceDesc(self, **kwargs):
#        if 'xname' in kwargs:
#            self.set_attr('xname',kwargs['xname'])
#        else:
#            self.set_attr('xname',kwargs['name'])
