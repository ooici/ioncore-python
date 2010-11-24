#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry_beta/resource_client.py
@author David Stuebe
@brief base classes for resrouce client
"""

from twisted.internet import defer, reactor
from twisted.python import failure
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ReceivedError

import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject
from ion.core.messaging.ion_reply_codes import ResponseCodes
from ino.core.process import process

from ion.services.coi.resource_registry_beta.resource_registry import ResourceRegistryClient


CONF = ioninit.config(__name__)


class ResourceClient(object):
    """
    This is the base class for a resource client. It is a factory for resource
    instances. The api for working with a resource is in the instance. The client
    helps create and manage resources.
    """
    def __init__(self, proc=None, datastore='datastore', registry='resource_registry_2'):
        """
        Initializes a process client
        @param proc a IProcess instance as originator of messages
        @param datastore the name of the datastore service with which you wish to
        interact
        @param registy the name of the registry services with which you wish to
        interact
        """
        if not proc:
            proc = process.Process()
        self.proc = proc
        
        self.datastore = self.proc.get_scoped_name('system', datastore)
        
        self.registry = self.proc.get_scoped_name('system', registry)
        
        self.workbench = self.proc.workbench        
        
        assert hasattr(self.proc, 'op_fetch_linked_objects'), \
            'This process can not support a resource client. It must expose the fetch_linked_objects operation '
        
        self.registry_client = ResourceRegistryClient(proc=self.proc)
        

    @defer.inlineCallbacks
    def _check_init(self):
        """
        Called in client methods to ensure that there exists a spawned process
        to send messages from
        """
        if not self.proc.is_spawned():
            yield self.proc.spawn()


    @defer.inlineCallbacks
    def create_type_identifier(self, package='', protofile='', cls=''):
        """
        Currently not a deferred method but it will be!
        """
        yield self._check_init()
        
        type_id = None
        defer.returnValue(type_id)

    
    @defer.inlineCallbacks
    def create_resource_instance(self, type_id, name, description=''):
        """
        A Factory Method for Resrouce Instance Objects
        Ask the resource registry to create the instance!
        """
        
        request = None
        resource = yield self.registry_client.register_resource_instance(request)
        defer.returnValue(resource)
        
    @defer.inlineCallbacks
    def read_resource_instance(self, resource_id):
        """
        A factory method for resource instances which are already in the data store
        """
        
    @defer.inlineCallbacks
    def get_resource_instance(self, resource_id):
        """
        A factory method for resource instances
        """
        

    
    
    
class ResourceInstance(object):
    """
    The instance is the vehicle through which the developer interacts with a
    particular resource. It hides the git semantics of the data store and deals
    with resource specific properties.
    """
    
    
    @defer.inlineCallbacks
    def read_resource(self, version=None):
        """
        Read the current state of the resource - returns the head object for a structure
        """
        
    @defer.inlineCallbacks
    def write_resource(self, comment=None):
        """
        Write the current state of the resource to the data store- returns OK
        """
        
    def save_resource(self, comment=None):
        """
        Locally commit the current state of this resource - potentially to save process state
        """
        
    def version_resource(self, description=None):
        """
        Create a new version of this resource
        """
        
    @defer.inlineCallbacks
    def load_resource(self, version, commit_id):
        """
        Load a particular (previous) version of this resource
        """
        
    @property
    def resource_identity(self):
        """
        Return the resource identity
        """
    
    def _set_life_cycle_state(self, state):
        """
        Set the Life Cycel State of the resource
        """
        
    def _get_life_cycle_state(self):
        """
        Get the life cycle state of the resource
        """
    resource_life_cycle_state = property(_get_life_cycle_state, _set_life_cycle_state)
    
    def _set_resource_name(self, name):
        """
        """
        
    def _get_resource_name(self):
        """
        """
    
    resource_name = property(_get_resource_name, _set_resource_name)
    
    def _set_resource_description(self, description):
        """
        """
        
    def _get_resource_description(self):
        """
        """
    
    resource_description = property(_get_resource_description, _set_resource_description)
    
    
    

