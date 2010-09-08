#!/usr/bin/env python

"""
@file ion/services/coi/conversation_repository.py
@author Michael Meisinger
@brief Service that receives all sent messages in the system
"""

import ion.util.ionlog
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

logserv = logging.getLogger('convRepos')
log = ion.util.ionlog.getLogger(__name__)

class ConversationRepositoryService(BaseService):
    """Conversation repository service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='conversation_repository', version='0.1.0', dependencies=[])

    def slc_init(self):
        pass

    def op_define_conv_type(self, content, headers, msg):
        """Service operation: Define a new conversation type (aka protocol,
        interaction pattern)
        """

    def op_get_conv_type(self, content, headers, msg):
        """Service operation: Returns the description of the conversation type
        including the specification
        """

    def op_define_conversation(self, content, headers, msg):
        """Service operation: Create a new conversation (instance) definition
        """

    def op_bind_conversation(self, content, headers, msg):
        """Service operation: Add oneself to the conversation role binding
        """

    def op_log_message(self, content, headers, msg):
        """Service operation: Log an occurred message with the repository
        """
        logmsg = content['msg']
        logserv.info("-----------------------------------------------\n"+
                     str(logmsg))


# Spawn of the process using the module name
factory = ProtocolFactory(ConversationRepositoryService)
