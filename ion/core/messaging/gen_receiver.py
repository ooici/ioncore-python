#!/usr/bin/env python

"""
@file ion/core/messaging/gen_receiver.py
@author Michael Meisinger
@brief Receiver compliant class to do arbitrary messaging attachments
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.intercept.interceptor import Invocation
from ion.core.messaging import messaging
from ion.core.messaging.receiver import Receiver
import ion.util.procutils as pu

class GenericReceiver(Receiver):
    """
    Generic extension to receiver.
    A generic receiver represents one consumer from one queue with
    arbitrary bindings (including pattern bindings) in an arbitrary exchange.
    """

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"

        name_config = messaging.process(self.xname)
        name_config.update({'name_type':'process'})

        yield self._init_receiver(name_config, store_config=True)
