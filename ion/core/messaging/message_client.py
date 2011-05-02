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
#from ion.core.process import process
from ion.core.object.repository import RepositoryError

from ion.core.object import workbench
from ion.core.object import repository
from ion.core.object import gpb_wrapper

from ion.core.object import object_utils

ION_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=11, version=1)
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
            #proc = process.Process()
            raise MessageClientError('Message Client can not be used without a process')
        self.proc = proc
        
        # The message client is backed by a process workbench.
        self.workbench = self.proc.workbench
        

    @defer.inlineCallbacks
    def _check_init(self):
        """
        Called in client methods to ensure that there exists a spawned process
        to send and receive messages
        """
        
        ### Let someone else worry about whether the process is spawnded...
        if not self.proc.is_spawned():
            yield self.proc.spawn()
        
        #Must use a yield to keep the defered interface
        #yield None
        assert isinstance(self.workbench, workbench.WorkBench), \
        'Process workbench is not initialized'

    
    @defer.inlineCallbacks
    def create_instance(self, MessageContentTypeID, MessageName=''):
        """
        @brief Create an instance of the message type!
        @param MessageContentTypeID is a type identifier object
        @param MessageName is a depricated architectural concept. Please do not use it!
        @retval message is a MInstance object
        """
        yield self._check_init()

        # Create a sendable message object
        msg_repo= self.workbench.create_repository(ION_MESSAGE_TYPE)
        
        msg_object = msg_repo.root_object
        
        if MessageName:
            log.info('MessageName is a depricated architectural concept. Please do not use it!')
            msg_object.name = MessageName
        
        # For now let the message ID be set by the process that created it?
        msg_object.identity = msg_repo.repository_key
        
        # Add an empty message object of the requested type
        if MessageContentTypeID:
            msg_object.message_object = msg_repo.create_object(MessageContentTypeID)
        
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
    Exception class for Message Instance Object
    """
    
class MessageFieldProperty(object):
    
    def __init__(self, name, msg_prop, doc=None):
        self.name = name
        if doc: self.__doc__ = doc
        self.field_type = msg_prop.field_type
        self.field_enum = msg_prop.field_enum
        
    def __get__(self, message_instance, objtype=None):
        return getattr(message_instance._repository.root_object.message_object, self.name)
        
    def __set__(self, message_instance, value):
        return setattr(message_instance._repository.root_object.message_object, self.name, value)
        
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Message Instance property')
        
        
class MessageEnumProperty(object):
    
    def __init__(self, name, doc=None):
        self.name = name
        if doc: self.__doc__ = doc
        
    def __get__(self, message_instance, objtype=None):
        return getattr(message_instance._repository.root_object.message_object, self.name)
        
    def __set__(self, wrapper, value):
        raise AttributeError('Can not set a Message Instance enum object')
        
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Message Instance property')
    
class MessageInstanceType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.
    
    This approach is generally applicable to wrap data structures. It is extremely powerful!
    """

    _type_cache = {}

    def __call__(cls, message_repository, *args, **kwargs):
        # Cache the custom-built classes
        
        # Check that the object we are wrapping is a Google Message object
        if not isinstance(message_repository, repository.Repository):
            raise MessageInstanceError('MessageInstance init argument must be an instance of a Repository')
        
        if message_repository.status == repository.Repository.NOTINITIALIZED:
            raise MessageInstanceError('MessageInstance init Repository argument is in an invalid state - checkout first!')
        
        if message_repository.root_object.ObjectType != ION_MESSAGE_TYPE:
            raise MessageInstanceError('MessageInstance init Repository is not a message object!')
        
        message_obj = message_repository.root_object.message_object
        
        msgType, clsType = type(message_obj), None

        if msgType in MessageInstanceType._type_cache:
            clsType = MessageInstanceType._type_cache[msgType]
        else:
            
            
            # Get the class name
            clsName = '%s_%s' % (cls.__name__, msgType.__name__)
            clsDict = {}

            if msgType is not type(None):
                # Now setup the properties to map through to the GPB object
                for propName, msgProp in msgType._Properties.items():
                    #print 'Key: %s; Type: %s' % (fieldName, type(message_field))
                    prop = MessageFieldProperty(propName, msgProp)
                    clsDict[propName] = prop

                for enumName, enumProp in msgType._Enums.items():
                    enum = MessageEnumProperty(enumName)
                    clsDict[enumName] = enum

                clsDict['_Properties'] = msgType._Properties
                clsDict['_Enums'] = msgType._Enums

            # Try rewriting using slots - would be more efficient...
            def obj_setter(self, k, v):
                if self._init and not hasattr(self, k):
                    raise AttributeError(\
                        '''Cant add properties to the ION Message Instance.\n'''
                        '''Unknown property name - "%s"; value - "%s"''' % (k, v))
                super(MessageInstance, self).__setattr__(k, v)
                
            clsDict['_init'] = False
            clsDict['__setattr__'] = obj_setter
            
            clsType = MessageInstanceType.__new__(MessageInstanceType, clsName, (cls,), clsDict)
                
            MessageInstanceType._type_cache[msgType] = clsType

        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(MessageInstanceType, clsType).__call__(message_repository, *args, **kwargs)
        return obj
    
    
    
class MessageInstance(object):
    """
    @brief The resoure instance is the vehicle through which a process
    interacts with a message instance. It hides the git semantics of the data
    store and deals with message specific properties.
    """
        
    __metaclass__ = MessageInstanceType

        
    def __init__(self, message_repository):
        """
        message Instance objects are created by the message client
        """

        self._repository = message_repository
            
        self._init = True
            
    @property
    def Repository(self):
        return self._repository
        
    @property
    def Message(self):
        return self._repository._workspace_root
        
    
    def _get_message_object(self):
        repo = self._repository
        return repo._workspace_root.message_object
        
    def _set_message_object(self, value):
        repo = self._repository
        if value.ObjectType != self.MessageType:
            raise MessageInstanceError('Can not change the type of a message object!')
        repo._workspace_root.message_object = value
        
    MessageObject = property(_get_message_object, _set_message_object)
        
        
    def __str__(self):
        output  = '============== Message ==============\n'
        try:
            output += str(self.Message) + '\n'
        except gpb_wrapper.OOIObjectError, oe:
            log.error(oe)
            output += 'Message envelope object in an invalid state!'
        output += '============== Object ==============\n'
        try:
            output += str(self.MessageObject) + '\n'
        except gpb_wrapper.OOIObjectError, oe:
            log.error(oe)
            output += 'Message content object in an invalid state!'
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
        
        
        
    def ListSetFields(self):
        """
        Return a list of the names of the fields which have been set.
        """
        return self.MessageObject.ListSetFields()
        
    def IsFieldSet(self, field):
        return self.MessageObject.IsFieldSet(field)
        
    def HasField(self, field):
        log.warn('HasField is depricated because the name is confusing. Use IsFieldSet')
        return self.IsFieldSet(field)
        
    def ClearField(self, field):
        return self.MessageObject.ClearField(field)
      
    #@property
    #def IonResponse(self):
    #    return self.Message.IonResponse
    #  
    #def _set_message_ion_response(self, value):
    #    """
    #    Set the name of the message object
    #    """
    #    self.Message.ion_response = value
    #    
    #def _get_message_ion_response(self):
    #    """
    #    """
    #    return self.Message.ion_response
    #
    #MessageIonResponse = property(_get_message_ion_response, _set_message_ion_response)
      
    #@property
    #def ApplicationResponse(self):
    #    return self.Repository._workspace_root.ApplicationResponse
    #    
    #def _set_message_application_response(self, value):
    #    """
    #    Set the name of the message object
    #    """
    #    self.Message.application_response = value
    #    
    #def _get_message_application_response(self):
    #    """
    #    """
    #    return self.Message.application_response
    #
    #MessageApplicationResponse = property(_get_message_application_response, _set_message_application_response)
    
    @property
    def ResponseCodes(self):
        return self.Repository._workspace_root.ResponseCodes
        
    def _set_message_response_code(self, value):
        """
        Set the name of the message object
        """
        self.Message.response_code = value
        
    def _get_message_response_code(self):
        """
        """
        return self.Message.response_code
    
    MessageResponseCode = property(_get_message_response_code, _set_message_response_code)
        
        
    def _set_message_response_body(self, value):
        """
        Set the name of the message object
        """
        self.Message.response_body = value
        
    def _get_message_response_body(self):
        """
        """
        return self.Message.response_body
    
    MessageResponseBody = property(_get_message_response_body, _set_message_response_body)
        
        
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
        if self._repository._workspace_root.IsFieldSet('message_object'):
            return self._repository._workspace_root.GetLink('message_object').GPBMessage.type
        else:
            return None
    
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
    
    
