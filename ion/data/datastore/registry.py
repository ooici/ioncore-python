"""
@file ion/data/datastore/registry.py
"""

import logging
logging = logging.getLogger(__name__)

from zope import interface

from twisted.internet import defer

from ion.data import store
from ion.data import dataobject
from ion.data.datastore import objstore

from ion.core import ioninit
from ion.core import base_process
from ion.core.base_process import ProtocolFactory, BaseProcess
from ion.services.base_service import BaseService, BaseServiceClient
from ion.resources import coi_resource_descriptions
import ion.util.procutils as pu

CONF = ioninit.config(__name__)

class LCStateMixin(object):
    def set_resource_lcstate_new(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.new)

    def set_resource_lcstate_active(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.active)
        
    def set_resource_lcstate_inactive(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.inactive)

    def set_resource_lcstate_decomm(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.decomm)

    def set_resource_lcstate_retired(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.retired)

    def set_resource_lcstate_developed(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.developed)

    def set_resource_lcstate_commissioned(self, resource_reference):
        return self.set_resource_lcstate(resource_reference, dataobject.LCStates.commissioned)

class IRegistry(object):
    """
    @brief General API of any registry
    @TOD change to use zope interface!
    """
    def clear_registry(self):
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def register_resource(self,resource):
        """
        @brief Register resource description.
        @param uuid unique name of resource instance.
        @param resource instance of OOIResource.
        @note Does the resource instance define its own name/uuid?
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def get_resource(self,resource_reference):
        """
        @param uuid name of resource.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def set_resource_lcstate(self,resource_reference,lcstate):
        """
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def find_resource(self,description,regex=True,ignore_defaults=True,attnames=[]):
        """
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
class RegistryBackend(objstore.ObjectChassis):
    """
    """
    objectClass = dataobject.Resource

class Registry(objstore.ObjectStore, IRegistry, LCStateMixin):
    """
    """

    objectChassis = RegistryBackend

    def clear_registry(self):
        return self.backend.clear_store()



    @defer.inlineCallbacks
    def register_resource(self, resource):
        """
        @brief Add a new resource description to the registry. Implemented
        by creating a new (unique) resource object to the store.
        @note Is the way objectClass is referenced awkward?
        """
        #print 'Dataobject Register Start',dataobject.DataObject._types.has_key('__builtins__')
        #del dataobject.DataObject._types['__builtins__']
        #print 'Dataobject Register Removed',dataobject.DataObject._types.has_key('__builtins__')

        if isinstance(resource, self.objectChassis.objectClass):
        
            id = resource.RegistryIdentity
            if not id:
                raise RuntimeError('Can not register a resource which does not have an identity.')

            #print 'Dataobject Register Is Instance',dataobject.DataObject._types.has_key('__builtins__')
        
            
            try:
                res_client = yield self.create(id, self.objectChassis.objectClass)
            except objstore.ObjectStoreError:
                res_client = yield self.clone(id)
 
            #print 'Dataobject Chasis',dataobject.DataObject._types.has_key('__builtins__')
            
            yield res_client.checkout()
            
            #print 'Dataobject checkout',dataobject.DataObject._types.has_key('__builtins__')
            
            res_client.index = resource
            resource.RegistryCommit = yield res_client.commit()
        else:
            resource = None
        defer.returnValue(resource)

    @defer.inlineCallbacks
    def get_resource(self, resource_reference):
        """
        @brief Get resource description object
        """
        resource=None
        if isinstance(resource_reference, dataobject.ResourceReference):
        
            branch = resource_reference.RegistryBranch
            resource_client = yield self.clone(resource_reference.RegistryIdentity)
            if resource_client:
                if not resource_reference.RegistryCommit:
                    resource_reference.RegistryCommit = yield resource_client.get_head(branch)

                pc = resource_reference.RegistryCommit
                resource = yield resource_client.checkout(commit_id=pc)
                resource.RegistryBranch = branch
                resource.RegistryCommit = pc
            
        defer.returnValue(resource)

    @defer.inlineCallbacks
    def set_resource_lcstate(self, resource_reference, lcstate):
        """
        Service operation: set the life cycle state of resource
        """
        resource = yield self.get_resource(resource_reference)
        
        if resource:
            resource.set_lifecyclestate(lcstate)
            resource = yield self.register_resource(resource)
           
            defer.returnValue(resource.reference())
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def _list(self):
        """
        @brief list of resource references in the registry
        @note this is a temporary solution to implement search
        """
        idlist = yield self.refs.query('([-\w]*$)')
        defer.returnValue([dataobject.ResourceReference(RegistryIdentity=id) for id in idlist])


    @defer.inlineCallbacks
    def _list_descriptions(self):
        """
        @brief list of resource descriptions in the registry
        @note this is a temporary solution to implement search
        """
        refs = yield self._list()
        defer.returnValue([(yield self.get_resource(ref)) for ref in refs])
        
        
    @defer.inlineCallbacks
    def find_resource(self,description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @brief Find resource descriptions in the registry meeting the criteria
        in the FindResourceContainer 
        """
        
        # container for the return arguments
        results=[]
        if isinstance(description,dataobject.DataObject):
            refs = yield self._list()

            # Get the list of descriptions in this registry
            
            reslist = yield self._list_descriptions()

            for ref in refs:
                res = yield self.get_resource(ref)
                
                if description.compared_to(res,
                                        regex=regex,
                                        ignore_defaults=ignore_defaults,
                                        attnames=attnames):
                    results.append(res)
                    
        defer.returnValue(results)
            


@defer.inlineCallbacks
def test(ns):
    from ion.data import store
    s = yield store.Store.create_store()
    ns.update(locals())
    reg = yield ResourceRegistry.new(s, 'registry')
    res1 = dataobject.Resource.create_new_resource()
    ns.update(locals())
    res1.name = 'foo'
    commit_id = yield reg.register_resource(res1)
    res2 = dataobject.Resource.create_new_resource()
    res2.name = 'doo'
    commit_id = yield reg.register_resource(res2)
    ns.update(locals())



class BaseRegistryService(BaseService):
    """
    @Brief Base Registry Service Clase
    """

    
    # For now, keep registration in local memory store. override with spawn args to use cassandra
    @defer.inlineCallbacks
    def slc_init(self):
        # use spawn args to determine backend class, second config file
        backendcls = self.spawn_args.get('backend_class', CONF.getValue('backend_class', None))
        backendargs = self.spawn_args.get('backend_args', CONF.getValue('backend_args', {}))
        
        # self.backend holds the class which is instantiated to provide the Store for the registry
        if backendcls:
            self.backend = pu.get_class(backendcls)
        else:
            self.backend = store.Store
        assert issubclass(self.backend, store.IStore)

        # Provide rest of the spawnArgs to init the store
        s = yield self.backend.create_store(**backendargs)
        
        # Now pass the instance of store to create an instance of the registry
        self.reg = Registry(s)
        
        name = self.__class__.__name__
        logging.info(name + " initialized")
        logging.info(name + " backend:"+str(backendcls))
        logging.info(name + " backend args:"+str(backendargs))


    @defer.inlineCallbacks
    def base_clear_registry(self, content, headers, msg):
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'])
        yield self.reg.clear_registry()
        yield self.reply_ok(msg)


    @defer.inlineCallbacks
    def base_register_resource(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        """
        logging.info('msg headers:'+ str(headers))
        resource = dataobject.Resource.decode(content)
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', Resource: \n' + str(resource))
  
        resource = yield self.reg.register_resource(resource)
        if resource:
            yield self.reply_ok(msg, resource.encode())
        else:
            yield self.reply_err(msg, None)


    @defer.inlineCallbacks
    def base_get_resource(self, content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        resource_reference = dataobject.Resource.decode(content)
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', Reference: \n' + str(resource_reference))

        resource = yield self.reg.get_resource(resource_reference)
        logging.info('Got Resource:\n'+str(resource))
        if resource:
            yield self.reply_ok(msg, resource.encode())
        else:
            yield self.reply_err(msg, None)

        
    @defer.inlineCallbacks
    def base_set_resource_lcstate(self, content, headers, msg):
        """
        Service operation: set the life cycle state of resource
        """
        container = dataobject.Resource.decode(content)
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', container: \n' + str(container))

        if isinstance(container,  coi_resource_descriptions.SetResourceLCStateContainer):
            resource_reference = container.reference
            lcstate = container.lcstate

            resource = yield self.reg.set_resource_lcstate(resource_reference, lcstate)
        
            if resource:
                yield self.reply_ok(msg, resource.reference().encode())

        else:
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks
    def base_find_resource(self, content, headers, msg):
        """
        @brief Find resource descriptions in the registry meeting the criteria
        listed in the properties dictionary 
        """
        description = None
        regex = None
        ignore_defaults = None
        attnames=[]
                
        container = dataobject.Resource.decode(content)
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', container: \n' + str(container))

        result_list = []
        if isinstance(container,  coi_resource_descriptions.FindResourceContainer):
            description = container.description
            regex = container.regex
            ignore_defaults = container.ignore_defaults
            attnames = container.attnames
            
            result_list = yield self.reg.find_resource(description,regex,ignore_defaults, attnames)
        
        results=coi_resource_descriptions.ResourceListContainer()
        results.resources = result_list
        
        yield self.reply_ok(msg, results.encode())


class RegistryService(BaseRegistryService):

     # Declaration of service
    declare = BaseService.service_declare(name='registry_service', version='0.1.0', dependencies=[])

    op_clear_registry = BaseRegistryService.base_clear_registry
    op_register_resource = BaseRegistryService.base_register_resource
    op_get_resource = BaseRegistryService.base_get_resource
    op_set_resource_lcstate = BaseRegistryService.base_set_resource_lcstate
    op_find_resource = BaseRegistryService.base_find_resource


# Spawn of the process using the module name
factory = ProtocolFactory(RegistryService)


class BaseRegistryClient(BaseServiceClient):
    """
    Do not instantiate this class!
    """
            
    @defer.inlineCallbacks
    def base_clear_registry(self,op_name):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send(op_name,None)
        if content['status']=='OK':
            defer.returnValue(None)


    @defer.inlineCallbacks
    def base_register_resource(self,op_name ,resource):
        """
        @brief Store a resource in the registry by its ID. It can be new or
        modified.
        @param res_id is a resource identifier unique to this resource.
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send(op_name,
                                            resource.encode())
        logging.info('Service reply: '+str(headers))
        if content['status']=='OK':
            resource = dataobject.Resource.decode(content['value'])
            defer.returnValue(resource)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def base_get_resource(self,op_name ,resource_reference):
        """
        @brief Retrieve a resource from the registry by Reference
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send(op_name,
                                                      resource_reference.encode())
        logging.info('Service reply: '+str(headers))

        if content['status']=='OK':
            resource = dataobject.Resource.decode(content['value'])
            defer.returnValue(resource)
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def base_set_resource_lcstate(self, op_name, resource_reference, lcstate):
        """
        @brief Retrieve a resource from the registry by its ID
        """
        yield self._check_init()

        container = coi_resource_descriptions.SetResourceLCStateContainer()
        container.lcstate = lcstate
        container.reference = resource_reference

        (content, headers, msg) = yield self.rpc_send(op_name,
                                                      container.encode())
        logging.info('Service reply: '+str(headers))
        
        if content['status'] == 'OK':
            resource_reference = dataobject.ResourceReference.decode(content['value'])
            defer.returnValue(resource_reference)
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def base_find_resource(self, op_name, description, regex=True,ignore_defaults=True,attnames=[]):
        """
        @brief Retrieve all the resources in the registry
        @param attributes is a dictionary of attributes which will be used to select a resource
        """
        yield self._check_init()

        container = coi_resource_descriptions.FindResourceContainer()
        container.description = description
        container.ignore_defaults = ignore_defaults
        container.regex = regex
        container.attnames = attnames
        
        (content, headers, msg) = yield self.rpc_send(op_name,container.encode())
        logging.info('Service reply: '+str(headers))
        
        # Return a list of resources
        if content['status'] == 'OK':            
            results = dataobject.DataObject.decode(content['value'])
            defer.returnValue(results.resources)
        else:
            defer.returnValue([])


class RegistryClient(BaseRegistryClient,IRegistry,LCStateMixin):
    """
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "registry_service"
        BaseServiceClient.__init__(self, proc, **kwargs)


    def clear_registry(self):
        return self.base_clear_registry('clear_registry')

    def register_resource(self,resource):
        return self.base_register_resource('register_resource', resource)

    def get_resource(self,resource_reference):
        return self.base_get_resource('get_resource', resource_reference)
        
    def set_resource_lcstate(self, resource_reference, lcstate):
        return self.base_set_resource_lcstate('set_resource_lcstate',resource_reference, lcstate)

    def find_resource(self, description,regex=True,ignore_defaults=True, attnames=[]):
        return self.base_find_resource('find_resource',description,regex,ignore_defaults,attnames)


