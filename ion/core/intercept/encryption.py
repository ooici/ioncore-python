#!/usr/bin/env python

"""
@file ion/core/intercept/encryption.py
@author Michael Meisinger
@brief Encryption and decryption interceptor
"""

import hashlib
try:
    import json
except:
    import simplejson as json

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.id import Id
from ion.core.intercept.interceptor import EnvelopeInterceptor, PassThroughInterceptor
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject


# Configuration
CONF = ioninit.config(__name__)
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


class EncryptionInterceptor(EnvelopeInterceptor):
    def before(self, invocation):
        msg = invocation.message

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

        return invocation

    def after(self, invocation):
        msg = invocation.message

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
        invocation.message = {'msg':repr(encmsg)}

        return invocation

if not encrypt:
    del EncryptionInterceptor
    EncryptionInterceptor = PassThroughInterceptor
