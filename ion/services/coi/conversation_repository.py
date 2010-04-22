#!/usr/bin/env python

"""
@file ion/services/coi/conversation_repository.py
@author Michael Meisinger
@brief Service that receives all sent messages in the system
"""

import logging
import logging.config
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

logserv = logging.getLogger('convRepos')

class ConversationRepositoryService(BaseService):
    """Conversation repository service interface
    """
    
    def slc_init(self):
        pass

    def op_register_conv_type(self, content, headers, msg):
        pass
    
    def op_get_conv_spec(self, content, headers, msg):
        pass
    
    def op_newconv(self, content, headers, msg):
        pass

    def op_logmsg(self, content, headers, msg):
        logmsg = content['msg']
        logserv.info("-----------------------------------------------\n"+
                     str(logmsg))

    
# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = ConversationRepositoryService(receiver)
