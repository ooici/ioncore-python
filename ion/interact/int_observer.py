#!/usr/bin/env python

"""
@file ion/interact/int_observer.py
@author Michael Meisinger
@brief A process that observes interactions in the Exchange
"""
from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.process.process import Process, ProcessClient, ProcessFactory
import ion.util.procutils as pu


class InteractionObserver(Process):
    """
    @brief Process that observes ongoing interactions in the Exchange. Logs
        them to disk and makes them available in the local container (for
        testing) and on request.
    """

    def __init__(self, *args, **kwargs):
        """
        """
        Process.__init__(self, *args, **kwargs)

        # Determine public service messaging name either from spawn args or
        # use default name from service declaration
        #default_svcname = self.declare['name'] + '_' + self.declare['version']
        default_svcname = self.declare['name']
        self.svc_name = self.spawn_args.get('servicename', default_svcname)
        assert self.svc_name, "Service must have a declare with a valid name"

        # Create a receiver (inbound queue consumer) for service name
        self.svc_receiver = ServiceWorkerReceiver(
                label=self.svc_name+'.'+self.receiver.label,
                name=self.svc_name,
                scope='system',
                group=self.receiver.group,
                process=self, # David added this - is it a good idea?
                handler=self.receive)
        self.add_receiver(self.svc_receiver)

    def plc_init(self):
        pass


class InteractionMonitor(InteractionObserver):
    """
    @brief Extension of the InteractionObserver that observes interactions of
        a specific process and monitors it for correctness.
    @note The tricky thing is to relate incoming and outgoing messages of a
        process.
    """


class ConversationMonitor(InteractionObserver):
    """
    @brief Extension of the InteractionMonitor that distinguishes and monitors
        conversations within the interactions of a specific for correctness.
        Such conversations need to comply to a conversation type, which must
        be specified in an electronic format (such as Scribble, FSM) that can
        be operationally enacted (i.e. followed message by message)
    """
