#!/usr/bin/env python

"""
@file ion/core/cc/interceptor.py
@author Michael Meisinger
@brief capability container interceptor system (security, governance)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import hashlib
try:
    import json
except:
    import simplejson as json
    
from magnet.container import InterceptorSystem, Interceptor
from carrot.backends.base import BaseMessage
from ion.core import ioninit
import ion.util.procutils as pu

# Configuration
CONF = ioninit.config(__name__)
master_off = CONF.getValue('master_off', False)
msg_sign = CONF.getValue('msg_sign', True)
encrypt = CONF.getValue('encrypt', False)
encrypt_mod = CONF.getValue('encrypt_mod', None)
encrypt_key = CONF.getValue('encrypt_key', None)
encrypt_pad = CONF.getValue('encrypt_pad', 16)

if encrypt:
    try:
        mod = pu.get_module(encrypt_mod)
        encrypter = mod.new(encrypt_key)
    except ImportError:
        encrypter = None
else:
    encrypter = None

class IdmInterceptor(Interceptor):
    """Message interceptor for identity management and security purposes.
    Called last before message hits the wire, and first after message received.
    """
    @classmethod
    def transform(cls, msg):
        """Identity management transform
        """
        if master_off: return msg
        try:
            if isinstance(msg, BaseMessage):
                # @todo: don't use flag to decide whether to validate or decrypt
                # but the presence of the resp. message attributes
                if msg_sign:
                    msg = cls.message_validator(msg)
                if encrypt:
                    msg = MessageEncrypter.decrypt_message(msg)
            else:
                if encrypt:
                    msg = MessageEncrypter.encrypt_message(msg)
                if msg_sign:
                    msg = cls.message_signer(msg)
        except Exception, e:
            pu.log_exception("IdM Interceptor failed",e)
        return msg

    @classmethod
    def message_validator(cls, msg):
        #log.info('IdM interceptor IN')
        cont = msg.payload.copy()
        hashrec = cont.pop('signature')
        blob = json.dumps(cont, sort_keys=True)
        hash = hashlib.sha1(blob).hexdigest()
        if hash != hashrec:
            log.info("*********message signature wrong***********")
        return msg

    @classmethod
    def message_signer(cls, msg):
        #log.info('IdM interceptor OUT')
        blob = json.dumps(msg, sort_keys=True)
        hash = hashlib.sha1(blob).hexdigest()
        msg['signature'] = hash
        return msg

class MessageEncrypter(object):
    @classmethod
    def encrypt_message(cls, msg):
        #log.info("Encrypting message: "+str(msg))
        blob = json.dumps(msg, sort_keys=True)
        padding = int(((len(blob) + encrypt_pad) // encrypt_pad) * encrypt_pad)
        padmsg = blob.ljust(padding)
        #log.info("Padded message json: '"+str(padmsg)+"'")
        encmsg = encrypter.encrypt(padmsg)
        log.info("Encrypted message len="+str(len(encmsg)))
        # HACK1: Returning the encrypted message in a mutable dict so that
        # we can replace dict content when decoding
        # HACK2: Need to repr the binary encmsg because otherwise failure        
        return {'msg':repr(encmsg)}

    @classmethod
    def decrypt_message(cls, msg):
        # Note: modifying the dict in the msg.payload does not work
        encmsc = msg.payload.pop('msg')
        msgblob = encrypter.decrypt(eval(encmsc))
        #log.info("Message decrypted: "+str(msgblob))
        msgobj = json.loads(msgblob)
        #log.info("Message recreated: "+str(msgobj))
        msg.payload.update(msgobj)
        msg._decoded_cache = msgobj
        assert msgobj is msg.payload
        #log.info("Message payload recreated: "+str(msg.payload))
        return msg
         
class PolicyInterceptor(Interceptor):
    @classmethod
    def transform(cls, msg):
        """Policy transform -- pass all
        """
        if master_off: return msg
        if isinstance(msg, BaseMessage):
            ""
            #log.info('Policy interceptor IN')
        else:
            ""
            #log.info('Policy interceptor OUT')
        return msg
    
class GovernanceInterceptor(Interceptor):
    @classmethod
    def transform(cls, msg):
        """Governance transform -- pass all
        """
        if master_off: return msg
        if isinstance(msg, BaseMessage):
            ""
            #log.info('Governance interceptor IN')
        else:
            ""
            #log.info('Governance interceptor OUT')
        return msg


class MessageEncoder(object):
    pass

class BaseInterceptorSystem(InterceptorSystem):
    """Custom capability container interceptor system for secutiry and
    governance purposes.
    """
    def __init__(self):
        InterceptorSystem.__init__(self)
        self.registerIdmInterceptor(IdmInterceptor.transform)
        self.registerPolicyInterceptor(PolicyInterceptor.transform)
        self.registerGovernanceInterceptor(GovernanceInterceptor.transform)
        log.info("Initialized ION BaseInterceptorSystem")
