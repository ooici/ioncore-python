#!/usr/bin/env python

"""
@file ion/core/messaging/message_client.py
@author David Stuebe
@brief Message Client and and Message Instance classes are used to manage
message objects in services and processes. They provide a simple interface to
create and manage messages.

@ TODO
Add methods to access the state of updates which are merging...
"""

from twisted.internet import defer, reactor
from twisted.python import failure
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ReceivedError
import ion.util.procutils as pu
from ion.core.process import process
from ion.core.object.repository import RepositoryError

from ion.core.object import workbench

from ion.core.object import object_utils

ion_message_type = object_utils.create_type_identifier(object_id=11, version=1)
idref_Type = object_utils.create_type_identifier(object_id=4, version=1)

CONF = ioninit.config(__name__)


class MessageClientError(Exception):
    """
    A class for message client exceptions
    """

class MessageClient(object):
    """
    @brief This is the base class for a message client. It is a factory for message
    instances. The message instance provides the interface for working with messages.
    The client helps create and manage message instances.
    """
    
    def __init__(self, proc=None):
        """
        Initializes a process client
        @param proc a IProcess instance as originator of messages
        @param datastore the name of the datastore service with which you wish to
        interact with the OOICI.
        """
        if not proc:
            proc = process.Process()
        
        self.proc = proc
        
        # The message client is backed by a process workbench.
        self.workbench = self.proc.workbench
        

    @defer.inlineCallbacks
    def _check_init(self):
        """
        Called in client methods to ensure that there exists a spawned process
        to send and receive messages
        """
        if not self.proc.is_spawned():
            yield self.proc.spawn()
        
        assert isinstance(self.workbench, workbench.WorkBench), \
        'Process workbench is not initialized'

    
    @defer.inlineCallbacks
    def create_instance(self, msg_type_id, name=''):
        """
        @brief Create an instance of the message type!
        @param msg_type_id is a type identifier object
        @retval message is a MInstance object
        """
        yield self._check_init()
        
        # Create a sendable message object
        msg_repo, msg_object = self.workbench.init_repository(ion_message_type)
        
        # Set the type and name
        msg_object.type.GPBMessage.CopyFrom(msg_type_id)
        msg_object.name = name
        
        # For now let the message ID be set by the process that created it?
        msg_object.identity = msg_repo.repository_key
        
        # Add an empty message object of the requested type
        msg_object.message_object = msg_repo.create_object(msg_type_id)
        
        # make a local commit 
        msg_repo.commit('Message object instantiated')
        
        # Create a message instance
        msg_instance = MessageInstance(msg_repo)
        
        defer.returnValue(msg_instance)
        
        
        
    def reference_instance(self, instance, current_state=False):
        """
        @brief Reference message creates a data object which can be used as a
        message or part of a message or added to another data object or message.
        @param instance is a messageInstance object
        @param current_state is a boolen argument which determines whether you
        intend to reference exactly the current state of the message.
        @retval an Identity Reference object to the message
        """
        
        return self.workbench.reference_repository(instance.MessageIdentity, current_state)
        

    
class MessageInstanceError(Exception):
    """
    Exceptoin class for Message Instance Object
    """
    
class MessageInstance(object):
    """
    @brief The resoure instance is the vehicle through which a process
    interacts with a message instance. It hides the git semantics of the data
    store and deals with message specific properties.
    """
        
    def __init__(self, message_repository):
        """
        message Instance objects are created by the message client
        """
        object.__setattr__(self,'_repository',None)
        
        self._repository = message_repository
            
    @property
    def Repository(self):
        return object.__getattribute__(self, '_repository')
        
    @property
    def Message(self):
        repo = object.__getattribute__(self, '_repository')
        return repo._workspace_root
        
    
    def _get_message_object(self):
        repo = object.__getattribute__(self, '_repository')
        return repo._workspace_root.message_object
        
    def _set_message_object(self, value):
        repo = object.__getattribute__(self, '_repository')
        if value.ObjectType != self.MessageType:
            raise MessageInstanceError('Can not change the type of a message object!')
        repo._workspace_root.message_object = value
        
    MessageObject = property(_get_message_object, _set_message_object)
        
        
    def __str__(self):
        output  = '============== Message ==============\n'
        output += str(self.Message) + '\n'
        output += '============== Object ==============\n'
        output += str(self.MessageObject) + '\n'
        output += '============ End Message ============\n'
        return output
        
        
    def CreateObject(self, type_id):
        """
        @brief CreateObject is used to make new locally created objects which can
        be added to the message's data structure.
        @param type_id is the type_id of the object to be created
        @retval the new object which can now be attached to the message
        """
        return self.Repository.create_object(type_id)
        
        
    def __getattribute__(self, key):
        """
        @brief We want to expose the message and its object through a uniform
        interface. To do so we override getattr to expose the data fields of the
        message object
        """
        # Because we have over-riden the default getattribute we must be extremely
        # careful about how we use it!
        repo = object.__getattribute__(self, '_repository')
        
        message = getattr(repo, '_workspace_root', None)

        message_object = getattr(message, 'message_object', None)

        gpbfields = getattr(message_object, '_gpbFields', [])
        
        if key in gpbfields:
            # If it is a Field defined by the gpb...
            #value = getattr(res_obj, key)
            value = message_object.__getattribute__(key)
                
        else:
            # If it is a attribute of this class, use the base class's getattr
            value = object.__getattribute__(self, key)
        return value
        
        
    def __setattr__(self,key,value):
        """
        @brief We want to expose the message and its object through a uniform
        interface. To do so we override getattr to expose the data fields of the
        message object
        """
        repo = object.__getattribute__(self, '_repository')
        
        message = getattr(repo, '_workspace_root', None)
        
        message_object = getattr(message, 'message_object', None)

        gpbfields = getattr(message_object, '_gpbFields', [])
        
        if key in gpbfields:
            # If it is a Field defined by the gpb...
            #setattr(res_obj, key, value)
            message_object.__setattr__(key,value)
                
        else:
            v = object.__setattr__(self, key, value)
        
    def ListSetFields(self):
        """
        Return a list of the names of the fields which have been set.
        """
        return self.ResourceObject.ListSetFields()
        
    def IsFieldSet(self, field):
        return self.MessageObject.IsFieldSet(field)
        
    def HasField(self, field):
        log.warn('HasField is depricated because the name is confusing. Use IsFieldSet')
        return self.IsFieldSet(field)
        
    def ClearField(self, field):
        return self.MessageObject.ClearField(field)
        
    @property
    def MessageIdentity(self):
        """
        @brief Return the message identity as a string
        """
        return str(self.Message.identity)
    
    @property
    def MessageType(self):
        """
        @brief Returns the message type - A type identifier object - not the wrapped object.
        """
        return self.Message.type.GPBMessage
    
    def _set_message_name(self, name):
        """
        Set the name of the message object
        """
        self.Message.name = name
        
    def _get_message_name(self):
        """
        """
        return str(self.Message.name)
    
    MessageName = property(_get_message_name, _set_message_name)
    """
    @var MessageName is a getter setter property for the name of the message
    """
    
    