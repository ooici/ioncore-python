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
from ion.core.intercept.interceptor import EnvelopeInterceptor, PassThroughInterceptor
import ion.util.procutils as pu

# Configuration
CONF = ioninit.config(__name__)
msg_sign = CONF.getValue('msg_sign', True)


class DigitalSignatureInterceptor(EnvelopeInterceptor):
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


if not msg_sign:
    del DigitalSignatureInterceptor
    DigitalSignatureInterceptor = PassThroughInterceptor
