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


class ServiceRegistryService(resource_registry.BaseResourceRegistryService):
    """
    Service registry service interface
    @todo a service is a resource and should also be living in the resource registry
    """
    # Declaration of service
    declare = BaseService.service_declare(name='service_registry', version='0.1.0', dependencies=[])


    @defer.inlineCallbacks
    def op_register_service_description(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        """
        svc_name = str(content['svc_name'])
        svc_enc = content['svc_enc']
        svc = dataobject.ResourceDescription.decode(svc_enc)()
        logging.info('op_register_service: \n' + str(svc))
  
        yield self.reg.register(svc_name,svc)
        yield self.reply_ok(msg, {'svc_name':str(svc_name)},)


    @defer.inlineCallbacks
    def op_get_service_description(self, content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        svc_name = content['svc_name']
        logging.info('op_get_service: '+str(svc_name))

        svc = yield self.reg.get_description(svc_name)
        logging.info('Got Resource:\n'+str(resource))
        if svc:
            yield self.reply_ok(msg, {'svc_enc':svc.encode()})
        else:
            yield self.reply_err(msg, {'svc_enc':None})


    @defer.inlineCallbacks
    def op_register_service_instance(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        """
        svc_name = str(content['svc_name'])
        svc_enc = content['svc_enc']
        svc = dataobject.ResourceDescription.decode(svc_enc)()
        logging.info('op_register_service: \n' + str(svc))
  
        yield self.reg.register(svc_name,svc)
        yield self.reply_ok(msg, {'svc_name':str(svc_name)},)

    @defer.inlineCallbacks
    def op_get_service_instance(self, content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        svc_name = content['svc_name']
        logging.info('op_get_service: '+str(svc_name))

        svc = yield self.reg.get_description(svc_name)
        logging.info('Got Resource:\n'+str(resource))
        if svc:
            yield self.reply_ok(msg, {'svc_enc':svc.encode()})
        else:
            yield self.reply_err(msg, {'svc_enc':None})


# Spawn of the process using the module name
factory = ProtocolFactory(ServiceRegistryService)


class ServiceRegistryClient(resource_registry.BaseRegistryClient):
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
    def register_service(self, svc_desc):        
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('register_service',
                                            {'svc_name':svc_desc.svc_name,'svc_enc':svc_desc.encode()})
        logging.info('Register Service reply: '+str(headers))
        defer.returnValue(str(content['svc_name']))
        

    @defer.inlineCallbacks
    def get_service(self, service_name):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_service',
                                                      {'svc_name':service_name})
        logging.info('Service reply: '+str(content))
        svc_enc = content['svc_enc']
        if res_enc != None:
            svc = dataobject.ResourceDescription.decode(svc_enc)()
            defer.returnValue(svc)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def register_service_instance(self, svc_inst):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('register_service',
                                            {'svc_inst_name':svc_inst.svc_name,'svc_enc':svc_desc.encode()})
        logging.info('Register Service reply: '+str(headers))
        defer.returnValue(str(content['svc_name']))

    @defer.inlineCallbacks
    def get_service_instance(self, service_instance_name):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_service_instance',
                                                      {'svc_name':service_instance_name})
        logging.info('Service reply: '+str(content))
        svc_enc = content['svc_enc']
        if res_enc != None:
            svc = dataobject.ResourceDescription.decode(svc_enc)()
            defer.returnValue(svc)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def get_service_instance_name(self, service_name):
        sidesc = yield self.get_service_instance(service_name)
        defer.returnValue(sidesc.xname)


class ServiceDesc(dataobject.ResourceDescription):
    """Structured object for a service instance.

    Attributes:
    .name   name of the service
    """    
    svc_name = dataobject.TypedAttribute(str)
    svc_module = dataobject.TypedAttribute(str)
    svc_class = dataobject.TypedAttribute(str)
    svc_spawnargs = dataobject.TypedAttribute(dict,{})
    res_type = dataobject.TypedAttribute(str,'rt_service')
    
    
class ServiceInterfaceDesc(dataobject.ResourceDescription):
    """
    op_service_method = dataobject.TypedAttribute(dict)
    Where the dict is a content is a description for the message
    """
    
        


class ServiceInstanceDesc(dataobject.ResourceDescription):
    """Structured object for a service instance.

    Attributes:
    .name   name of the service
    """    
    xname = dataobject.TypedAttribute(str)
    inst_name = dataobject.TypedAttribute(str)
    inst_type = dataobject.TypedAttribute(str)
    res_type = dataobject.TypedAttribute(str,'rt_serviceinst')


# Service interface description for the resource registry service
class ServiceRegistryInterfaceDesc(ServiceInterfaceDesc):
    op_register_service_description = dataobject.TypedAttribute(dict,{'svc_name':'str','svc_enc':'ResourceDescription'})
    op_get_service_description = dataobject.TypedAttribute(dict)
    op_register_service_instance = dataobject.TypedAttribute(dict)
    op_get_service_instance = dataobject.TypedAttribute(dict)


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
