#!/usr/bin/env python

"""
@file ion/play/hello_object.py
@author David Stuebe
@brief An example process definition that can be used as template for object communication.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.exception import ApplicationError
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient



from ion.core.object import object_utils

# from net.ooici.play addressbook.proto
PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)
"""
message Person {
  enum _MessageTypeIdentifier {
    _ID = 20001;
    _VERSION = 1;
  }
  optional string name = 1;
  optional int32 id = 2;        // Unique ID number for this person.
  optional string email = 3;

  enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
  }

  message PhoneNumber {
    optional string number = 1;
    optional PhoneType type = 2 [default = HOME];
  }

  repeated PhoneNumber phone = 4;
}

"""

# from net.ooici.play addressbook.proto
ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
"""
message AddressLink {
  enum _MessageTypeIdentifier {
    _ID = 20003;
    _VERSION = 1;
  }
  repeated net.ooici.core.link.CASRef person = 1;
  optional net.ooici.core.link.CASRef owner = 2;
  optional string title = 3;
}
"""
class HelloObjectError(ApplicationError):
    """
    An exception class for the Hello Object example
    """


class HelloObject(ServiceProcess):
    """
    Example process sends objects
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_object',
                                             version='0.1.0',
                                             dependencies=[])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        log.info('SLC_INIT HelloProcess')

    @defer.inlineCallbacks
    def op_hello_person(self, person, headers, msg):
        """
        @brief Respond to a simple message
        @param params person GPB, 20001/1, a person object from net.ooici.play.
        @retval response, GPB 20001/1, a person message if successful.
        """
        log.info('op_hello_person: %s' % person)

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if person.ObjectType != PERSON_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloObjectError('Unexpected type received %s; type: %s' % (str(person), str(person.ObjectType)), 400)
            
                
        # Creepy hello person object log statements...
        log.info( 'Hello ' + person.name +'...')
        log.info('I know your phone number ' + person.name + '... it is: '+ person.phone[0].number)
        
        # Change something...
        
        person.name = person.name + ' stuebe'
        
        # Print the name of the repository which holds this object
        log.info('The name of the person object in the service: ' +person.Repository.repository_key)
        
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, person)

    @defer.inlineCallbacks
    def op_hello_everyone(self, addresslink, headers, msg):
        """
        @brief Respond to a simple message
        @param params addresslink GPB, 20003/1, a addresslink object from net.ooici.play.
        @retval ack - a message envelope with ResponseCode OK and no content.
        """
        log.info('op_hello_everyone: ')

        if addresslink.ObjectType != ADDRESSLINK_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloObjectError('Unexpected type received %s; type: %s' % (str(addresslink), str(addresslink.ObjectType)),400)
            
        log.info('Received addresslink; Title: ' + addresslink.title)
        for person in addresslink.person:
            log.info('Logging Person: \n' +str(person))
            
        yield self.reply_ok(msg)


class HelloObjectClient(ServiceClient):
    """
    This is an exemplar service client that calls the hello object service. It
    makes service calls RPC style using GPB object. There is no special handling
    here, just call send. The clients should become exteremly thin wrappers with
    no business logic.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_object"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def hello_person(self, msg):
        """
        @brief Respond to a simple message
        @param params person GPB, 20001/1, a person object from net.ooici.play.
        @retval response, GPB 20001/1, a person message if successful.
        """
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('hello_person', msg)
        
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def hello_everyone(self, msg):
        """
        @brief Respond to a simple message
        @param params addresslink GPB, 20003/1, a addresslink object from net.ooici.play.
        @retval ack - a message envelope with ResponseCode OK and no content.
        """
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('hello_everyone', msg)
        
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(HelloObject)

