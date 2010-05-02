#!/usr/bin/env python

"""
@file ion/core/cc/interceptor.py
@author Michael Meisinger
@brief capability container interceptor system (security, governance)
"""

import logging
import hashlib
import json

from magnet.container import InterceptorSystem, Interceptor
from carrot.backends.base import BaseMessage

class IdmInterceptor(Interceptor):
    @classmethod
    def transform(cls, msg):
        """Identity management transform
        """
        if isinstance(msg, BaseMessage):
            msg = cls.message_validator(msg)
        else:
            msg = cls.message_signer(msg)
        return msg

    @classmethod
    def message_validator(cls, msg):
        #logging.info('IdM interceptor IN')
        cont = msg.payload.copy()
        hashrec = cont.pop('signature')
        blob = json.dumps(cont, sort_keys=True)
        hash = hashlib.sha1(blob).hexdigest()
        if hash != hashrec:
            logging.info("*********message signature wrong***********")
        return msg

    @classmethod
    def message_signer(cls, msg):
        #logging.info('IdM interceptor OUT')
        blob = json.dumps(msg, sort_keys=True)
        hash = hashlib.sha1(blob).hexdigest()
        msg['signature'] = hash
        return msg
                
class PolicyInterceptor(Interceptor):
    @classmethod
    def transform(cls, msg):
        """Policy transform -- pass all
        """
        if isinstance(msg, BaseMessage):
            ""
            #logging.info('Policy interceptor IN')
        else:
            ""
            #logging.info('Policy interceptor OUT')
        return msg
    
class GovernanceInterceptor(Interceptor):
    @classmethod
    def transform(cls, msg):
        """Governance transform -- pass all
        """
        if isinstance(msg, BaseMessage):
            ""
            #logging.info('Governance interceptor IN')
        else:
            ""
            #logging.info('Governance interceptor OUT')
        return msg

class BaseInterceptorSystem(InterceptorSystem):
    """Custom capability container interceptor system for secutiry and
    governance purposes.
    """
    def __init__(self):
        InterceptorSystem.__init__(self)
        self.registerIdmInterceptor(IdmInterceptor.transform)
        self.registerPolicyInterceptor(PolicyInterceptor.transform)
        self.registerGovernanceInterceptor(GovernanceInterceptor.transform)
        logging.info("Initialized ION BaseInterceptorSystem")
