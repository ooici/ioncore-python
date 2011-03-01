#!/usr/bin/env python

"""
@file ion/play/rot13_service.py
@author Paul Hubbard
@date 3/1/11
@brief Example code for the Trial tutorial - a trivial Ion service
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.core.exception import ApplicationError
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient

import codecs

# Globals
log = ion.util.ionlog.getLogger(__name__)

# Request and response message types
REQUEST_TYPE = object_utils.create_type_identifier(object_id=20032, version=1)
RESPONSE_TYPE = object_utils.create_type_identifier(object_id=20033, version=1)

class R13Exception(ApplicationError):
    """
    Rot13 exception class
    """

class Rot13Service(ServiceProcess):
    """
    This service is an example Ion service that replies to a message with the content
    processed by the rot13 algorithm.
    """

    declare = ServiceProcess.service_declare(name='rot13',
                                          version='1.2.7',
                                          dependencies=[])

    def __init__(self, *args, **kwargs):
        if kwargs:
            # Service class initializer. Basic config, but no yields allowed.
            ServiceProcess.__init__(self, *args, **kwargs)

    def slc_init(self):
        self.mc = MessageClient(proc=self)

    @defer.inlineCallbacks
    def op_rot13(self, content, headers, msg):
        # Check for correct protocol buffer type
        if content.MessageType != REQUEST_TYPE:
            raise R13Exception('Bad message type receieved, ignoring',
                               content.ResponseCodes.BAD_REQUEST)

        # Check for required field in message
        if not hasattr(content, 'input_string'):
            raise R13Exception('Required field not found in message',
                               content.ResponseCodes.BAD_REQUEST)

        # Call actual service to do the work
        log.debug('Starting R13 decode....')
        rval = self.rot13(content.input_string)

        log.debug('Encode complete, composing reply...')
        # Compose reply
        reply = yield self.mc.create_instance(RESPONSE_TYPE)
        reply.output_string = rval

        # Off it goes
        yield self.reply_ok(msg, reply)

        log.debug('R13 completed')

    def rot13(self, in_str):
        """
        Service implementation. Strings in and out, no transport/protocol here.
        """
        encoder = codecs.lookup('rot13')
        return encoder.encode(in_str)[0]

class Rot13Client(ServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'rot13'
        ServiceClient.__init__(self, proc, **kwargs)


    @defer.inlineCallbacks
    def rot13(self, gpb_msg):
        """
        @brief Message client for the Amazing Rot13 Service.
        @param gpb_msg GPB 20032/1, input_string field populated
        @retval GPB 20033/1, output_string, empty if error
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('rot13', gpb_msg)
        defer.returnValue(content)

# Ion magic
factory = ProcessFactory(Rot13Service)



