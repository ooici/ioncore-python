#!/usr/bin/env python


"""
@file ion/data/datastore/registry.py
@author David Stuebe
@author Matt Rodriguez
@brief base service for registering ooi resources
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.services.dm.preservation import store
from ion.data import dataobject
from ion.data.datastore import objstore

from ion.core import ioninit
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.resources import coi_resource_descriptions
import ion.util.procutils as pu

CONF = ioninit.config(__name__)

class LCStateMixin(object):
    """
    @brief This mixin class is used to add life cycle state convenience methods
    """
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
    @todo change to use zope interface!
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
    @brief Registry is the backend implementation used by all registry services
    """

    objectChassis = RegistryBackend

    def clear_registry(self):
        log.info(self.__class__.__name__ + '################################################################# clear_registry called ')
        return self.backend.clear_store()



    @defer.inlineCallbacks
    def register_resource(self, resource):
        """
        @brief Add a new resource description to the registry. Implemented
        by creating a new (unique) resource object to the store.
        @note Is the way objectClass is referenced awkward?
        """
        log.info("Calling Registry.register_resource")
        if isinstance(resource, self.objectChassis.objectClass):

            id = resource.RegistryIdentity
            if not id:
                raise RuntimeError('Can not register a resource which does not have an identity.')

            try:
                log.info("creating res_client")
                res_client = yield self.create(id, self.objectChassis.objectClass)
                log.info("Created client")
            except objstore.ObjectStoreError:
                log.info("ObjectStoreError")
                res_client = yield self.clone(id)
            
            log.info("Check out resource")    
            yield res_client.checkout()
            log.info("Checked out resource")
            res_client.index = resource
            log.info("Committing resource")
            resource.RegistryCommit = yield res_client.commit()
            log.info("Committed resource")
        else:
            resource = None

        defer.returnValue(resource)

    @defer.inlineCallbacks
    def get_resource(self, resource_reference):
        """
        @brief Get resource description object
        """
        log.info("get_resource called")
        log.info("resource_reference %s" % resource_reference)
        resource=None
        if isinstance(resource_reference, dataobject.ResourceReference):
            log.info("resource_reference is correct type")
            branch = resource_reference.RegistryBranch
            resource_client = yield self.clone(resource_reference.RegistryIdentity)
            log.info("resource_client %s" % resource_client)
            if resource_client:
                log.info("Retrieved resource_client")
                if not resource_reference.RegistryCommit:
                    resource_reference.RegistryCommit = yield resource_client.get_head(branch)

                pc = resource_reference.RegistryCommit
                resource = yield resource_client.checkout(commit_id=pc)
                resource.RegistryBranch = branch
                resource.RegistryCommit = pc

        defer.returnValue(resource)

    @defer.inlineCallbacks
    def get_resource_by_id(self, id):
        resource_client = yield self.clone(id)
        resource = yield resource_client.checkout()
        resource.RegistryCommit = resource_client.cur_commit
        resource.RegistryBranch = 'master'
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
        @param description DataObject that 
        @param regex Whether a regex is used or not
        @param ignore_defaults ignore registry defaults
        @param attnames attribute names associated with the resource
        """

        # container for the return arguments
        log.info("called find_resource")
        log.info("description class %s" % description.__class__)
        results=[]
        if isinstance(description,dataobject.DataObject):
            refs = yield self._list()
            log.debug(description)

            # Get the list of descriptions in this registry

            reslist = yield self._list_descriptions()
            log.info(self.__class__.__name__ + ': find_resource found ' + str(len(reslist)) + ' items in registry')
            num_match = 1
            for ref in refs:
                log.info("ref: %s" % ref)
                res = yield self.get_resource(ref)
                log.debug("Found #"+str(num_match)+":"+str(res))
                num_match += 1
                matches_desc = description.compared_to(res,
                                        regex=regex,
                                        ignore_defaults=ignore_defaults,
                                        attnames=attnames)
                if matches_desc:
                    results.append(res)

        defer.returnValue(results)





class BaseRegistryService(ServiceProcess):
    """
    @brief Base Registry Service Clase
    To create a Registry Service inherit this class and over ride the method
    names as in the RegistryService class example bellow.
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
        log.info(name + " initialized; backend:%s; backend args:%s" % (backendcls, backendargs))

    @defer.inlineCallbacks
    def base_clear_registry(self, content, headers, msg):
        log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'])
        yield self.reg.clear_registry()
        yield self.reply_ok(msg)


    @defer.inlineCallbacks
    def base_register_resource(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        """
        log.debug('Registry Service MSG:'+ str(headers))
        #resource = dataobject.Resource.decode(content)
        accept_encoding = headers.get('accept-encoding', '')
        resource = dataobject.serializer.decode(content, headers['encoding'])
        log.info(self.__class__.__name__ + ' received: op_'+ headers['op'] )
        log.debug('Resource: \n' + str(resource))

        resource = yield self.reg.register_resource(resource)
        log.debug('%%%%%%%%%%%%')
        log.debug(resource)
        if resource:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            #yield self.reply_ok(msg, resource.encode())
            encoding, _, data = dataobject.serializer.encode(resource, accept_encoding)
            headers = dict(encoding=encoding)
            yield self.reply_ok(msg, data, headers)
        else:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
            yield self.reply_err(msg, None)


    @defer.inlineCallbacks
    def base_get_resource(self, content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        log.debug('Registry Service MSG:'+ str(headers))
        #resource_reference = dataobject.Resource.decode(content)
        accept_encoding = headers.get('accept-encoding', '')
        resource_reference = dataobject.serializer.decode(content, headers['encoding'])
        log.info(self.__class__.__name__ + ' received: op_'+ headers['op'] +', Reference: \n' + str(resource_reference))

        resource = yield self.reg.get_resource(resource_reference)
        #log.info('Got Resource:\n'+str(resource))
        if resource:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            #yield self.reply_ok(msg, resource.encode())
            encoding, _, data = dataobject.serializer.encode(resource, accept_encoding)
            headers = dict(encoding=encoding)
            yield self.reply_ok(msg, data, headers)
        else:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks
    def base_get_resource_by_id(self, content, headers, msg):
        accept_encoding = headers.get('accept-encoding', '')
        resource = yield self.reg.get_resource_by_id(content)
        if resource:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            #yield self.reply_ok(msg, resource.encode())
            encoding, _, data = dataobject.serializer.encode(resource, accept_encoding)
            headers = dict(encoding=encoding)
            yield self.reply_ok(msg, data, headers)
        else:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks
    def base_set_resource_lcstate(self, content, headers, msg):
        """
        Service operation: set the life cycle state of resource
        """
        log.debug('Registry Service MSG:'+ str(headers))
        #container = dataobject.Resource.decode(content)
        accept_encoding = headers.get('accept-encoding', '')
        container = dataobject.serializer.decode(content, headers['encoding'])
        log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', container: \n' + str(container))

        #This makes things difficult; shouldn't use python class resolution
        #to determine DataObject stuff
        if isinstance(container,  coi_resource_descriptions.SetResourceLCStateContainer):
            resource_reference = container.reference
            lcstate = container.lcstate

            resource = yield self.reg.set_resource_lcstate(resource_reference, lcstate)

            if resource:
                log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
                encoding, _, data = dataobject.serializer.encode(resource.reference(), accept_encoding)
                headers = dict(encoding=encoding)
                #yield self.reply_ok(msg, resource.reference().encode())
                yield self.reply_ok(msg, data, headers)

        else:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
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
        attnames = []

        log.debug('Registry Service MSG:'+ str(headers))
        #container = dataobject.Resource.decode(content)
        #This container object is expected to have certain functionality
        accept_encoding = headers.get('accept-encoding', '')
        container = dataobject.serializer.decode(content, headers['encoding'])
        log.debug(self.__class__.__name__ + ' received: op_'+ headers['op'] +', container: \n' + str(container))

        result_list = []
        #This makes things difficult; shouldn't use python class resolution
        #to determine DataObject stuff
        #if isinstance(container,  coi_resource_descriptions.FindResourceContainer):
        if type(container).__name__ == coi_resource_descriptions.FindResourceContainer.__name__:
            description = container.description
            regex = container.regex
            ignore_defaults = container.ignore_defaults
            attnames = container.attnames

            result_list = yield self.reg.find_resource(description,regex,ignore_defaults, attnames)

        results = coi_resource_descriptions.ResourceListContainer()
        results.resources = result_list

        log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success! ' + str(len(result_list)) + ' Matches Found')
        encoding, _, data = dataobject.serializer.encode(results, accept_encoding)
        headers = dict(encoding=encoding)
        yield self.reply_ok(msg, data, headers)


class RegistryService(BaseRegistryService):
    """
    @brief Example Registry Service implementation using the base class
    """
     # Declaration of service
    declare = ServiceProcess.service_declare(name='registry_service', version='0.1.0', dependencies=[])

    op_clear_registry = BaseRegistryService.base_clear_registry
    op_register_resource = BaseRegistryService.base_register_resource
    op_get_resource = BaseRegistryService.base_get_resource
    op_get_resource_by_id = BaseRegistryService.base_get_resource_by_id
    op_set_resource_lcstate = BaseRegistryService.base_set_resource_lcstate
    op_find_resource = BaseRegistryService.base_find_resource


# Spawn of the process using the module name
factory = ProcessFactory(RegistryService)


class BaseRegistryClient(ServiceClient):
    """
    @brief BaseRegistryClient is the base class used to simplify implementation
    of Registry Service Clients. The client for a particular registry should
    inherit from this base class and use the base methods to communicate with
    the service. The client can do what ever business logic is neccissary before
    calling the base client methods.
    """

    @defer.inlineCallbacks
    def base_clear_registry(self,op_name):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send(op_name,None)
        defer.returnValue(None)


    @defer.inlineCallbacks
    def base_register_resource(self,op_name ,resource):
        """
        @brief Store a resource in the registry by its ID. It can be new or
        modified.
        @param op_name The operation name to call in the service
        @param resource is an instance of a Resource object which has been
        created using the create_new_resource method. Specific registries may
        override this behavior to create the resource inside the register method
        """
        log.info("called BaseRegistryClient.base_register_resource")
        yield self._check_init()
        log.info(self.__class__.__name__ + '; Calling: '+ op_name)

        assert isinstance(resource, dataobject.Resource), 'Invalid argument to base_register_resource'
        assert isinstance(op_name, str), 'Invalid argument to base_register_resource'

        encoding, _, data = dataobject.serializer.encode(resource)
        headers = {'encoding':encoding, 'accept-encoding':encoding}
        (content, headers, msg) = yield self.rpc_send(op_name, data, headers)

        log.debug(self.__class__.__name__ + ': '+ op_name + '; Result:' + str(headers))

        #resource = dataobject.Resource.decode(content['value'])
        #resource = dataobject.serializer.decode(content['value'], headers['encoding'])
        resource = dataobject.serializer.decode(content, headers['encoding'])
        log.info(self.__class__.__name__ + ': '+ op_name + ' Success!')
        defer.returnValue(resource)

    @defer.inlineCallbacks
    def base_get_resource(self,op_name ,resource_reference):
        """
        @brief Retrieve a resource from the registry by Reference
        @param op_name the operation name to call in the service
        @param resource_reference is the registry identifier for a particular
        version of an object in the registry.
        """
        yield self._check_init()
        log.info(self.__class__.__name__ + '; Calling:'+ op_name)

        assert isinstance(resource_reference, dataobject.ResourceReference), 'Invalid argument to base_register_resource'
        assert isinstance(op_name, str), 'Invalid argument to base_register_resource'

        encoding, _, data = dataobject.serializer.encode(resource_reference)
        headers = {'encoding':encoding, 'accept-encoding':encoding}
        (content, headers, msg) = yield self.rpc_send(op_name, data, headers)

        log.debug(self.__class__.__name__ + ': '+ op_name + '; Result:' + str(headers))

        #resource = dataobject.Resource.decode(content['value'])
        #resource = dataobject.serializer.decode(content['value'], headers['encoding'])
        resource = dataobject.serializer.decode(content, headers['encoding'])
        log.info(self.__class__.__name__ + ': '+ op_name + ' Success!')
        defer.returnValue(resource)

    @defer.inlineCallbacks
    def base_get_resource_by_id(self, op_name, id):
        yield self._check_init()
        log.info(self.__class__.__name__ + '; Calling:'+ op_name)
        encoding = dataobject.serializer._default_content_type
        headers = {'encoding':encoding, 'accept-encoding':encoding}
        (content, headers, msg) = yield self.rpc_send(op_name, id, headers)
        log.debug(self.__class__.__name__ + ': '+ op_name + '; Result:' + str(headers))

        #resource = dataobject.Resource.decode(content['value'])
        #resource = dataobject.serializer.decode(content['value'], headers['encoding'])
        resource = dataobject.serializer.decode(content, headers['encoding'])
        log.info(self.__class__.__name__ + ': '+ op_name + ' Success!')
        defer.returnValue(resource)


    @defer.inlineCallbacks
    def base_set_resource_lcstate(self, op_name, resource_reference, lcstate):
        """
        @brief Retrieve a resource from the registry by its ID
        @param op_name The operation name to call in the service
        @param resource_reference is the registry identifier for a particular
        version of an object in the registry.
        @parm lcstate is a life cycle state object which provides an enum like
        behavior.
        """
        yield self._check_init()
        log.info(self.__class__.__name__ + '; Calling:'+ op_name)

        assert isinstance(resource_reference, dataobject.ResourceReference), 'Invalid argument to base_register_resource'
        assert isinstance(op_name, str), 'Invalid argument to base_register_resource'
        assert isinstance(lcstate, dataobject.LCState), 'Invalid argument to base_register_resource'


        container = coi_resource_descriptions.SetResourceLCStateContainer()
        container.lcstate = lcstate
        container.reference = resource_reference

        encoding, _, data = dataobject.serializer.encode(container)
        headers = {'encoding':encoding, 'accept-encoding':encoding}
        (content, headers, msg) = yield self.rpc_send(op_name, data, headers)

        log.debug(self.__class__.__name__ + ': '+ op_name + '; Result:' + str(headers))

        #resource_reference = dataobject.ResourceReference.decode(content['value'])
        #resource_reference = dataobject.serializer.decode(content['value'], headers['encoding'])
        resource_reference = dataobject.serializer.decode(content, headers['encoding'])
        log.info(self.__class__.__name__ + ': '+ op_name + ' Success!')
        defer.returnValue(resource_reference)


    @defer.inlineCallbacks
    def base_find_resource(self, op_name, description, regex=True,ignore_defaults=True,attnames=[]):
        """
        @brief Retrieve all the resources in the registry
        @param description is an instance of a Resource which is compared with
        those in the registry. The user can select which typed attributes of the
        description to compare.
        @param regex flag to use regex when comparing string
        @param ignore_defaults flag to ignore typed attributes in the description
        which are still set to their default value.
        @param attnames is a list of the tyeped attribute names which should
        match to select a resource
        """
        yield self._check_init()
        log.info(self.__class__.__name__ + '; Calling:'+ op_name)

        assert isinstance(description, dataobject.DataObject), 'Invalid argument to base_register_resource'
        assert isinstance(op_name, str), 'Invalid argument to base_register_resource'
        assert isinstance(regex, bool), 'Invalid argument to base_register_resource'
        assert isinstance(ignore_defaults, bool), 'Invalid argument to base_register_resource'
        assert isinstance(attnames, list), 'Invalid argument to base_register_resource'


        container = coi_resource_descriptions.FindResourceContainer()
        container.description = description
        container.ignore_defaults = ignore_defaults
        container.regex = regex
        container.attnames = attnames

        encoding, _, data = dataobject.serializer.encode(container)
        headers = {'encoding':encoding, 'accept-encoding':encoding}
        content, headers, msg = yield self.rpc_send(op_name, data, headers)

        log.debug(self.__class__.__name__ + ': '+ op_name + '; Result:' + str(headers))

        # Return a list of resources
        #results = dataobject.DataObject.decode(content['value'])
        #results = dataobject.serializer.decode(content['value'], headers['encoding'])
        results = dataobject.serializer.decode(content, headers['encoding'])
        log.info(self.__class__.__name__ + ': '+ op_name + ' Success!')
        defer.returnValue(results.resources)


class RegistryClient(BaseRegistryClient,IRegistry,LCStateMixin):
    """
    #@todo How can we make it so that the client infact uses a local registry
    for testing rather than using the registry service? Can we switch it in init?
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "registry_service"
        ServiceClient.__init__(self, proc, **kwargs)


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

    def get_resource_by_id(self, id):
        return self.base_get_resource_by_id('get_resource_by_id', id)
