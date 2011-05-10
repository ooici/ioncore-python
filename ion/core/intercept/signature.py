#!/usr/bin/env python

"""
@file ion/core/intercept/signature.py
@author Michael Meisinger
@brief Digitally sign and validate message interceptor
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
from ion.core.intercept import interceptor
from ion.core.security import authentication
from ion.util import procutils as pu
from ion.util.path import adjust_dir


# Configuration
CONF = ioninit.config(__name__)
msg_sign = CONF.getValue('msg_sign', True)
#XXX HACKS
_priv_key_path = adjust_dir(CONF.getValue('priv_key_path'))
_cert_path = adjust_dir(CONF.getValue('cert_path'))


class DigitalSignatureInterceptor(interceptor.EnvelopeInterceptor):
    def before(self, invocation):
        msg = invocation.message

        #log.info('IdM interceptor IN')
        cont = msg.payload.copy()
        hashrec = cont.pop('signature')
        blob = json.dumps(cont, sort_keys=True)
        hash = hashlib.sha1(blob).hexdigest()
        if hash != hashrec:
            log.info("*********message signature wrong***********")

        return invocation

    def after(self, invocation):
        msg = invocation.message

        #log.info('IdM interceptor OUT')
        blob = json.dumps(msg, sort_keys=True)
        hash = hashlib.sha1(blob).hexdigest()
        msg['signature'] = hash

        invocation.message = msg
        return invocation

class SystemSecurityPlugin(interceptor.EnvelopeInterceptor):
    """Decorate outgoing messages with security attributes and read
    security attributes of incoming messages.

    @todo Decide on what the necessary security headers are.

    What/who is the user?
    The send command of Process sets the user header to be whatever its
    proc_name is. So, messages are sent based on process/container
    identity. That means, once the process is started, the identity does
    not change, so signing does not need to asynchronously look up
    credentials.

    What is the user of an incoming message? The interceptor will have to
    do a look up based on the sender-name header in order to verify a
        signature.

    System signature. Use container certificate to sign/validate message.
    
    Need to research more on what other user/security attributes should be
    included in the message headers.
    """

    def __init__(self, name, system_priv_key_path=None, allowed_certs=None):
        if allowed_certs is None: allowed_certs = {}
        
        interceptor.EnvelopeInterceptor.__init__(self, name)
        #XXX @todo need to be able to properly configure this interceptor
        #during container startup

        if system_priv_key_path is None:
            system_priv_key_path = _priv_key_path #From hard coded CONF
        self._priv_key_path = system_priv_key_path

        if not allowed_certs:
            allowed_certs['ooi-ion'] = _cert_path #Use cert from CONF
        self.allowed_certs = allowed_certs
        self.auth = authentication.Authentication()

    def _read_key(self, path):
        f = open(path)
        key = f.read()
        f.close()
        return key

    def certs(self, id):
        """
        Get cert path by given id.
        Read cert file and return cert.
        """
        path = self.allowed_certs[id] #XXX Need an error condition for a
                                      #bad id
        cert = self._read_key(path)
        return cert

    @property
    def priv_key(self):
        key = self._read_key(self._priv_key_path)
        return key

    def after(self, invocation):
        """
        Use the system private key to sign the message content.
        Decorate an outgoing message with a digital signature of the
        encoded content. Add signature to the message headers.
        """
        content = invocation.message['content'] #Hope this is a string!
        try:
            hash = hashlib.sha1(content).hexdigest()
        except TypeError:
            # Not sure what to do, being hashable is not really a policy,
            # so dropping might not be appropriate. Need to raise some kind
            # of error.
            invocation.error(note='Error taking hash of content!')
            return invocation
        priv_key = self.priv_key
        signature = self.auth.sign_message(hash, priv_key)
        invocation.message['signer'] = 'ooi-ion' #XXX What should this header be?
        invocation.message['signature'] = signature
        # Do we call invocation.proceed ???
        return invocation

    def before(self, invocation):
        """
        If the signature and signer headers are missing, then drop the
        message. Otherwise, verify the message.
        """
        message = invocation.message
        #hack check of message spec!
        if message.has_key('signature') and message.has_key('signer'):
            content = invocation.message['content'] #this better be there
            hash = hashlib.sha1(content).hexdigest()
            signature = invocation.message['signature']
            signer = invocation.message['signer']
            cert = self.certs(signer)
            verifiedQ = self.auth.verify_message(hash, cert, signature)
            if verifiedQ:
                # Do we call invocation.proceed ???
                return invocation
            else:
                invocation.drop('Unverified Signature')
                return invocation
        else:
            invocation.drop('Invalid Message Format')
            return invocation


if not msg_sign:
    del DigitalSignatureInterceptor
    DigitalSignatureInterceptor = interceptor.PassThroughInterceptor
