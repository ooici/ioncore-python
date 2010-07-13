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


class IRegistry(object):
    """
    @brief General API of any registry
    @TOD change to use zope interface!
    """
    def clear_registry(self):
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def register_resource_description(self,resource_description):
        """
        @brief Register resource description.
        @param uuid unique name of resource instance.
        @param resource instance of OOIResource.
        @note Does the resource instance define its own name/uuid?
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def get_resource_description(self,resource_reference):
        """
        @param uuid name of resource.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def set_resource_lcstate(self,resource_reference,lcstate):
        """
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
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
    
    def find_resource_description(self,properties):
        """
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
class RegistryBackend(objstore.ObjectChassis):
    """
    """
    objectClass = dataobject.ResourceDescription

class Registry(objstore.ObjectStore, IRegistry):
    """
    """

    objectChassis = RegistryBackend

    def clear_registry(self):
        return self.backend.clear_store()



    @defer.inlineCallbacks
    def register_resource_description(self, resource_description):
        """
        @brief Add a new resource description to the registry. Implemented
        by creating a new (unique) resource object to the store.
        @note Is the way objectClass is referenced awkward?
        """
        if isinstance(resource_description, self.objectChassis.objectClass):
        
            id = resource_description._identity
            if not id:
                raise RuntimeError('Can not register a resource which does not have an identity.')
        
            try:
                res_client = yield self.create(id, self.objectChassis.objectClass)
            except objstore.ObjectStoreError:
                res_client = yield self.clone(id)
            
            yield res_client.checkout()
            res_client.index = resource_description
            resource_description._parent_commit = yield res_client.commit()
            #print 'pcommit',resource_description._parent_commit
        else:
            resource_description = None
        defer.returnValue(resource_description)

    @defer.inlineCallbacks
    def get_resource_description(self, resource_reference):
        """
        @brief Get resource description object
        """
        resource_description=None
        if isinstance(resource_reference, dataobject.ResourceReference):
        
            branch = resource_reference._branch
            resource_client = yield self.clone(resource_reference._identity)
            if resource_client:
                if not resource_reference._parent_commit:
                    resource_reference._parent_commit = yield resource_client.get_head(branch)

                pc = resource_reference._parent_commit
                resource_description = yield resource_client.checkout(commit_id=pc)
                resource_description._branch = branch
                resource_description._parent_commit = pc
            
        defer.returnValue(resource_description)

    @defer.inlineCallbacks
    def set_resource_lcstate(self, resource_reference, lcstate):
        """
        Service operation: set the life cycle state of resource
        """

        resource_description = yield self.get_resource_description(resource_reference)
        
        if resource_description:
            resource_description.set_lifecyclestate(lcstate)
            resource_description = yield self.register_resource_description(resource_description)
           
            defer.returnValue(resource_description.reference())
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def _list(self):
        """
        @brief list of resource references in the registry
        @note this is a temporary solution to implement search
        """
        idlist = yield self.refs.query('([-\w]*$)')
        defer.returnValue([dataobject.ResourceReference(id=id) for id in idlist])


    @defer.inlineCallbacks
    def _list_descriptions(self):
        """
        @brief list of resource descriptions in the registry
        @note this is a temporary solution to implement search
        """
        refs = yield self._list()
        defer.returnValue([(yield self.get_resource_description(ref)) for ref in refs])
        
        
    @defer.inlineCallbacks
    def find_resource_description(self,description,regex=True,ignore_defaults=True):
        """
        @brief Find resource descriptions in the registry meeting the criteria
        in the FindResourceDescriptionContainer 
        """
        # container for the return arguments
        results=[]
        if isinstance(description,dataobject.ResourceDescription):
            # Get the list of descriptions in this registry
            reslist = yield self._list_descriptions()

            for res in reslist:                        
                # Test for failure and break
                test = False
                if regex:
                    if ignore_defaults:
                        test = compare_regex_no_defaults(description,res)
                    else:
                        test = compare_regex_defaults(description,res)
                else:
                    if ignore_defaults:
                        test = compare_ignore_defaults(description,res)
                    else:
                        test = compare_defaults(description,res)
                if test:
                    results.append(res)

        defer.returnValue(results)

def compare_regex_defaults(r1,r2):
    """
    test=True
    for k,v in properties.items():                
        # if this resource does not contain this attribute move on
        if not k in res.attributes:
            test = False
            break
            
        att = getattr(res, k, None)
            
        # Bogus - can't send lcstate objects in a dict must convert to sting to test
        if isinstance(att, dataobject.LCState):
            att = str(att)
            
        if isinstance(v, (str, unicode) ):
            # Use regex
            if not re.search(v, att):
                test=False
                break
        else:
            # test equality
            #@TODO add tests for range and in list...
            
            
            if att != v and v != None:
                test=False
                break                    
    """
        
def compare_regex_no_defaults(r1,r2):
    """
    """
        
def compare_defaults(r1,r2):
    try:
        m = [getattr(r1, a) == getattr(r2, a) for a in r1.attributes]
        return reduce(lambda a, b: a and b, m)
    except:
        return False
        
def compare_ignore_defaults(r1,r2):
    """
    """
    try:
        default = r1.__class__()
        m = [getattr(r1, a) == getattr(r2, a) or getattr(r1, a) ==  getattr(default,a) for a in r1.attributes]
        return reduce(lambda a, b: a and b, m)
    except:
        return False
            


@defer.inlineCallbacks
def test(ns):
    from ion.data import store
    s = yield store.Store.create_store()
    ns.update(locals())
    reg = yield ResourceRegistry.new(s, 'registry')
    res1 = dataobject.ResourceDescription.create_new_resource()
    ns.update(locals())
    res1.name = 'foo'
    commit_id = yield reg.register_resource_description(res1)
    res2 = dataobject.ResourceDescription.create_new_resource()
    res2.name = 'doo'
    commit_id = yield reg.register_resource_description(res2)
    ns.update(locals())



class BaseRegistryService(BaseService):
    """
    @Brief Base Registry Service Clase
    """
    
    # Declaration of service
    declare = BaseService.service_declare(name='registry_service', version='0.1.0', dependencies=[])

    
    # For now, keep registration in local memory store. override with spawn args to use cassandra
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
        
        self.reg = Registry(s)
        
        name = self.__class__.__name__
        logging.info(name + " initialized")
        logging.info(name + " backend:"+str(backendcls))
        logging.info(name + " backend args:"+str(backendargs))


    @defer.inlineCallbacks
    def op_clear_registry(self, content, headers, msg):
        logging.info('op_clear_registry!')
        yield self.reg.clear_registry()
        yield self.reply_ok(msg)


    @defer.inlineCallbacks
    def op_register_resource_description(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        """
        resource = dataobject.ResourceDescription.decode(content)()
        logging.info('op_register_resource_description: \n' + str(resource))
  
        resource = yield self.reg.register_resource_description(resource)
        if resource:
            yield self.reply_ok(msg, resource.encode())
        else:
            yield self.reply_err(msg, None)


    @defer.inlineCallbacks
    def op_get_resource_description(self, content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        resource_reference = dataobject.ResourceDescription.decode(content)()
        logging.info('op_get_resource_description: '+str(resource_reference))

        resource = yield self.reg.get_resource_description(resource_reference)
        logging.info('Got Resource:\n'+str(resource))
        if resource:
            yield self.reply_ok(msg, resource.encode())
        else:
            yield self.reply_err(msg, None)

        
    @defer.inlineCallbacks
    def op_set_resource_lcstate(self, content, headers, msg):
        """
        Service operation: set the life cycle state of resource
        """
        container = dataobject.ResourceDescription.decode(content)()

        if isinstance(reference_lcstate,  coi_resource_descriptions.SetResourceLCStateContainer):
            logging.info('op_set_resource_lcstate: '+str(container))
            resource_reference = container.reference
            lcstate = container.lcstate

            resource = yield self.reg.set_resource_lcstate(resource_reference, lcstate)
        
            if resource:
                yield self.reply_ok(msg, resource.reference().encode())

        else:
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks
    def op_find_resource_description(self, content, headers, msg):
        """
        @brief Find resource descriptions in the registry meeting the criteria
        listed in the properties dictionary 
        """
        description = None
        regex = None
        ignore_defaults = None
        
        container = dataobject.ResourceDescription.decode(content)()
        
        result_list = []
        if isinstance(container,  coi_resource_descriptions.FindResourceDescriptionContainer):
            description = container.description
            regex = contianer.regex
            ignore_defaults = container.ignore_defaults
        
            result_list = yield self.reg.find_resource_description(description,regex,ignore_results)
        
        
        results=coi_resource_descriptions.ResourceDescriptionListContainer()
        results.resources = result_list
        
        yield self.reply_ok(msg, results.encode())

# Spawn of the process using the module name
factory = ProtocolFactory(BaseRegistryService)


class BaseRegistryClient(BaseServiceClient,IRegistry):
    """
    Do not instantiate this class!
    """
            
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "registry_service"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def clear_registry(self):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('clear_registry',None)
        if content['status']=='OK':
            defer.returnValue(None)


    @defer.inlineCallbacks
    def register_resource_description(self,resource_description):
        """
        @brief Store a resource in the registry by its ID. It can be new or
        modified.
        @param res_id is a resource identifier unique to this resource.
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('register_resource_description',
                                            resource_description.encode())
        logging.info('Service reply: '+str(headers))
        if content['status']=='OK':
            resource_description = dataobject.ResourceDescription.decode(content['value'])()
            defer.returnValue(resource_description)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def get_resource_description(self,resource_reference):
        """
        @brief Retrieve a resource from the registry by Reference
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_resource_description',
                                                      resource_reference.encode())
        logging.info('Service reply: '+str(headers))

        if content['status']=='OK':
            resource_description = dataobject.ResourceDescription.decode(content['value'])()
            defer.returnValue(resource_description)
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def set_resource_lcstate(self, resource_reference, lcstate):
        """
        @brief Retrieve a resource from the registry by its ID
        """
        yield self._check_init()

        container = coi_resource_descriptions.SetResourceLCStateContainer()
        container.lcstate = lcstate
        container.reference = resource_reference

        (content, headers, msg) = yield self.rpc_send('set_resource_lcstate',
                                                      container.encode())
        logging.info('Service reply: '+str(headers))
        
        if content['status'] == 'OK':
            resource_reference = dataobject.ResourceReference.decode(content['value'])()
            defer.returnValue(resource_reference)
        else:
            defer.returnValue(None)


    @defer.inlineCallbacks
    def find_resource_description(self,description,regex=True,ignore_defaults=True):
        """
        @brief Retrieve all the resources in the registry
        @param attributes is a dictionary of attributes which will be used to select a resource
        """
        yield self._check_init()

        container = coi_resource_descriptions.FindResourceDescriptionContainer()
        container.description = description
        container.ignore_defaults = ignore_defaults
        container.regex = regex
        
        (content, headers, msg) = yield self.rpc_send('find_resource_description',
                                                      container.encode())
        logging.info('Service reply: '+str(headers))
        
        # Return a list of resources
        if content['status'] == 'OK':
            results = dataobject.DataObject.decode(content['value'])()
            defer.returnValue(results.resources)
        else:
            defer.returnValue([])






