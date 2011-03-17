#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/test/receiver.py
@date 10/6/10
@author Paul Hubbard
@brief Simple listener to receive scheduler messages and report count
"""

from twisted.internet import defer
from ion.core.process.process import ProcessFactory
from ion.core.process.process import Process, ProcessClient
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class ScheduledTask(Process):
    """
    Test listener class to receive scheduler messages.
    """
    def plc_init(self):
        log.info('ScheduledTask starting up')
        self.msg_count = 0

    def op_scheduler(self, content, headers, msg):
        self.msg_count = self.msg_count + 1
        log.debug('Got a scheduler message! %d so far' % self.msg_count)

    def op_get_count(self, content, headers, msg):
        log.debug('Got a request for message count (%d)' % self.msg_count)
        self.reply_ok(msg, {'value': self.msg_count})

class STClient(ProcessClient):
    @defer.inlineCallbacks
    def get_count(self):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_count', {})
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(ScheduledTask)
