#!/usr/bin/env python

"""
@file ion/core/intercept/interceptor.py
@author Michael Meisinger
@brief capability container interceptor system (security, governance)
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.process.cprocess import ContainerProcess, Invocation
from ion.core.exception import ConfigurationError
import ion.util.procutils as pu

# Configuration
CONF = ioninit.config(__name__)
master_off = CONF.getValue('master_off', False)
msg_sign = CONF.getValue('msg_sign', True)


class Interceptor(ContainerProcess):
    """
    Interceptor that processes messages as the come along and passes them on.
    """

class EnvelopeInterceptor(Interceptor):
    """
    Interceptor that can process messages in the in-path and out-path. Just a
    wrapper for code to keep both complementing actions together.
    Note: There is NO guanantee that for one incoming message there is one
    outgoing message
    """
    def process(self, invocation):
        """
        @param invocation container object for parameters
        @retval invocation instance, may be modified
        """
        if invocation.path == Invocation.PATH_IN:
            return defer.maybeDeferred(self.before, invocation)
        elif invocation.path == Invocation.PATH_OUT:
            return defer.maybeDeferred(self.after, invocation)
        else:
            raise ConfigurationError("Illegal EnvelopeInterceptor path: %s" % invocation.path)

    def before(self, invocation):
        return invocation
    def after(self, invocation):
        return invocation

class PassThroughInterceptor(EnvelopeInterceptor):
    """
    Interceptor that drops messages.
    """
    def before(self, invocation):
        invocation.proceed()
        return invocation
    def after(self, invocation):
        invocation.proceed()
        return invocation

class DropInterceptor(EnvelopeInterceptor):
    """
    Interceptor that drops messages.
    """
    def before(self, invocation):
        invocation.drop()
        return invocation
    def after(self, invocation):
        invocation.drop()
        return invocation


class IdmInterceptor(object):
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
            else:
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


class PolicyInterceptor(object):
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

class GovernanceInterceptor(object):
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
