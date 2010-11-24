#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry_beta/resource_client.py
@author David Stuebe
@brief base classes for resrouce client
"""

from twisted.internet import defer, reactor
from twisted.python import failure
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ReceivedError

import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject
from ion.core.messaging.ion_reply_codes import ResponseCodes
from ino.core.process import process
from ion.core.object import workbench

CONF = ioninit.config(__name__)


class ResourceClient(object,ResponseCodes):
    """
    This is the base class for a resource client. It handels communication with
    the resource registry and the data store to work with and update resources.
    """
    def __init__(self, proc=None, datastore='datastore', registry='resource_registry_2'):
        """
        Initializes a process client
        @param proc a IProcess instance as originator of messages
        @param target  global scoped (process id or name) to send to
        @param targetname  system scoped exchange name to send messages to
        """
        if not proc:
            proc = process.Process()
        self.proc = proc
        
        self.datastore = self.proc.get_scoped_name('system', datastore)
        
        self.registry = self.proc.get_scoped_name('system', registry)
        
        self.workbench = self.proc.workbench        
        

    @defer.inlineCallbacks
    def _check_init(self):
        """
        Called in client methods to ensure that there exists a spawned process
        to send messages from
        """
        if not self.proc.is_spawned():
            yield self.proc.spawn()

    #@defer.inlineCallbacks
    #def attach(self):
    #    yield self._check_init()

    




    def rpc_send(self, *args):
        """
        Sends an RPC message to the specified target via originator process
        """
        return self.proc.rpc_send(self.target, *args)

    def send(self, *args):
        """
        Sends a message to the specified target via originator process
        """
        return self.proc.send(self.target, *args)

    def reply(self, *args):
        """
        Replies to a message via the originator process
        """
        return self.proc.reply(*args)