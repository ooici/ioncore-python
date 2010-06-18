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
import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient


from ion.data.datastore import registry
from ion.data import dataobject
from ion.data import store

from ion.core import ioninit
CONF = ioninit.config(__name__)


class ServiceRegistryService(BaseService):
    """
    Service registry service interface
    @todo a service is a resource and should also be living in the resource registry
    """
    # Declaration of service
    declare = BaseService.service_declare(name='service_registry', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        # use spawn args to determine backend class, second config file
        backendcls = self.spawn_args.get('backend_class', CONF.getValue('backend_class', None))
        backendargs = self.spawn_args.get('backend_args', CONF.getValue('backend_args', {}))
        if backendcls:
            self.backend = pu.get_class(backendcls)
        else:
            self.backend = store.Store
        assert issubclass(self.backend, store.IStore)

        # Provide rest of the spawnArgs to init the store
        s = yield self.backend.create_store(**backendargs)
        
        self.reg = registry.ResourceRegistry(s)
        
        name = self.__class__.__name__
        logging.info(name + " initialized")
        logging.info(name + " backend:"+str(backendcls))
        logging.info(name + " backend args:"+str(backendargs))

    @defer.inlineCallbacks
    def op_register_service(self, content, headers, msg):
        """
        Service operation: register a service by name
        """
        # Copy content?
        svc_enc = content['svc_desc']
        svc_desc = registry.ResourceDescription.decode(svc_enc)()
        logging.info('op_register_service: \n' + str(svc_desc))

        yield self.reg.register(svc_desc.svc_name,svc_desc)
        yield self.reply_ok(msg, {'svc_name':str(svc_desc.svc_name)},)

    def op_get_service_desc(self, content, headers, msg):
        """
        Service operation: Get service description. May include a service
        specification.
        """
        svc_name = content['svc_name']
        logging.info('op_get_service_desc: '+str(svc_name))

        svc_desc = yield self.reg.get_description(svc_name)
        logging.info('Got Service Description \n: '+str(svc_desc))

        yield self.reply_ok(msg, {'svc_desc':svc_desc.encode()})

    @defer.inlineCallbacks
    def op_register_instance(self, content, headers, msg):
        """
        Service operation:
        """
        #svcinstdesc = content['svcinst_desc'].copy()

        svcinst_enc = content['svcinst_desc']
        svcinst_desc = registry.ResourceDescription.decode(svcinst_enc)()
        logging.info('op_register_instance: \n' + str(svcinst_desc))

        yield self.reg.register(svcinst_desc.svc_name,svcinst_desc)
        yield self.reply_ok(msg, {'svcinst_name':str(svcinst_desc.svc_name)},)

    @defer.inlineCallbacks
    def op_get_instance(self, content, headers, msg):
        """
        Service operation: Returns the exchange name of the service
        """
        svcinst_name = str(content['svcinst_name'])
        logging.info('op_get_instance: '+str(svcinst_name))

        svcinst_desc= yield self.reg.get_description(svcinst_name)
        yield self.reply_ok(msg, {'svcinst_desc':svcinst_desc.encode()})
        
        

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
        logging.info('Service reply: '+str(headers))
        defer.returnValue(str(content['svc_name']))

    @defer.inlineCallbacks
    def get_service_instance(self, service_name):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_service_desc',
                        {'svc_name': service_name})
        
        svc_enc = content['svc_desc']
        if svc_enc != None:
            svc_desc = registry.ResourceDescription.decode(svc_enc)()
            defer.returnValue(svc_desc)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def register_service_instance(self, svc_inst):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('register_instance',
                        {'svcinst_desc':svc_inst.encode()})
        logging.info('Service reply: '+str(headers))
        defer.returnValue(str(content['svcinst_name']))

    @defer.inlineCallbacks
    def get_service_instance(self, service_name):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_instance',
                        {'svcinst_name': service_name})
        
        svcinst_enc = content['svcinst_desc']
        if svcinst_enc != None:
            svcinst_desc = registry.ResourceDescription.decode(svcinst_enc)()
            defer.returnValue(svcinst_desc)
        else:
            defer.returnValue(None)

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
    res_type = dataobject.TypedAttribute(str,'rt_serviceinst')

        


class ServiceInstanceDesc(registry.ResourceDescription):
    """Structured object for a service instance.

    Attributes:
    .name   name of the service
    """    
    xname = dataobject.TypedAttribute(str)
    svc_name = dataobject.TypedAttribute(str)
    res_type = dataobject.TypedAttribute(str,'rt_service')



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
