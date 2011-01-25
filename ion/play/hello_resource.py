#!/usr/bin/env python

"""
@file ion/play/hello_resource.py
@author David Stuebe
@brief An example service definition that can be used as template for resource management.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.services.coi.resource_registry_beta.resource_client import ResourceClientError, ResourceInstanceError

from ion.core.object import object_utils

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)
update_resource_type = object_utils.create_type_identifier(object_id=10, version=1)

class HelloResourceError(Exception):
    """
    An exception class for the Hello Object example
    """


class HelloResource(ServiceProcess):
    """
    Example service which manages a set of resources.
    
    It is possible to make fewer "op_'s" and send more complex arguments.
    Here I have choosen to break out the various operations to demonstrate each
    function more clearly. It is up to the developer and the architecture team
    whether there one 'update' operation and the mode (clobber, merge, append) is
    sent as part of the message object, or like in this example, there are
    multiple operations one for each mode.
    
    Similarly this example shows an service which 'loosly' controls its resources.
    The caller is has access to the resources by reference. With a resource client
    the caller could just as easily make changes directly.
    
    The developer may wish to abstract the calls to the service and never expose
    the actual resource references to the caller.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_resource',
                                             version='0.1.0',
                                             dependencies=[])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        
        # Can be called in __init__ or in slc_init... no yield required
        self.rc = ResourceClient(proc=self)
        
        self.instance_counter = 1
        
        log.info('SLC_INIT HelloResource')

    @defer.inlineCallbacks
    def op_create_addressbook_resource(self, addresslink, headers, msg):
        """
        This method assumes that the caller provides an addresslink object which
        should be made into a resource. A higher level service might provide
        a more abstract interface rather than actually sending the object which
        will become the resource...
        """
        log.info('op_create_addressbook_resource: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if addresslink.GPBType != addresslink_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected object class AddressLink, received %s; object class: %s'
                                     % (str(addressbook), str(addressbook.ObjectClass)))
            
        # Attributes of a resource like name and description should be controlled
        # by the service that manages them
        name = 'Addressbook #%d' % self.instance_counter
        self.instance_counter+=1
        
        resource = yield self.rc.create_instance(addresslink_type, name=name, description='Preposterous names!')
        
        # the ResourceObject property of the ResourceInstance provides a setter
        # for the value of the entire resource object - it is usefull for creation
        # and clobber updates...
        resource.ResourceObject = addresslink
        
        yield self.rc.put_instance(resource)
        
        log.info(str(resource))
        
        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller
        ref = self.rc.reference_instance(resource)
        
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, ref)

    @defer.inlineCallbacks
    def op_clobber_addressbook_resource(self, update_request, headers, msg):
        """
        This method is an example using a standard resource update request.
        It contains two parts, the reference to the resource to update and the
        state of the resource which should be set.
        """
        
        log.info('op_clobber_addressbook_resource: ')
            
        if update_request.GPBType != update_resource_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected object class UpdateResource, received %s; object class: %s'
                                     % (str(update_request), str(update_request.ObjectClass)))
            
        # Get the current state of the resource
        resource = yield self.rc.get_instance(update_request.resource_reference)
        
        # Merge the requested state into the resource history
        resource.MergeResourceUpdate(resource.CLOBBER, update_request.configuration)
            
        # Clobber the current state with the update
        resource.ResourceObject = resource.CompareToUpdates[0]
        # resource.ResourceObject is a property setter/getter for the resource object
        # resource.CompareToUpdates is a getter for the list of updated states that are being merged
           
        if resource.title == resource.CompareToUpdates[0].title:
            
        yield self.rc.put_instance(resource)
            
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_merge_addressbook_resource(self, update_request, headers, msg):
        """
        This method is an example using a standard resource update request.
        It contains two parts, the reference to the resource to update and the
        state of the resource which should be set.
        """
        
        log.info('op_clobber_addressbook_resource: ')
            
        if update_request.GPBType != update_resource_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected object class UpdateResource, received %s; object class: %s'
                                     % (str(update_request), str(update_request.ObjectClass)))
            
        # Get the current state of the resource
        resource = yield self.rc.get_instance(update_request.resource_reference)
        
        # Merge the requested state into the resource history
        resource.MergeResourceUpdate(resource.MERGE, update_request.configuration)
            
        # Merge the current state with the update
        
        # Compare and resolve differences...
        if resource.title != resource.CompareToUpdates[0].title:
            # resolve a difference in a string field...
            resource.title = resource.CompareToUpdates[0].title
            
        if resource.owner != resource.CompareToUpdates[0].owner:
            # resolve a difference in a object field
            resource.owner = resource.CompareToUpdates[0].owner

        # merge something more complex...
        for updated_person in resource.CompareToUpdates[0].person:
            # Resolve a difference between two lists of objects based on their ID #
            for idx, person in zip(range(len(resource.person)), resource.person):
                
                if updated_person.id == person.id:
                    resource.person[idx] = updated_person
                    break
            
            else:
                new_person = resource.person.add()
                new_person.SetLink(updated_person)
                    
            
            
        yield self.rc.put_instance(resource)
            
        yield self.reply_ok(msg)


class HelloResourceClient(ServiceClient):
    """
    This is an exemplar service client that calls the hello object service. It
    makes service calls RPC style using GPB object. There is no special handling
    here, just call send. The clients should become exteremly thin wrappers with
    no business logic.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_resource"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def create_addressbook_resource(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('create_addressbook_resource', msg)
        
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def clobber_addressbook_resource(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('clobber_addressbook_resource', msg)
        
        defer.returnValue(content)

    @defer.inlineCallbacks
    def merge_addressbook_resource(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('clobber_addressbook_resource', msg)
        
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(HelloResource)



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
