#!/usr/bin/env python

"""
@file ion/services/cei/test/test_queuestat.py
@author David LaBissoniere
@brief Test queuestat behavior
"""


import os
import uuid

from twisted.internet import defer
from twisted.trial import unittest

from ion.core.process.process import Process
import ion.util.procutils as pu
import ion.test.iontest
from ion.test.iontest import IonTestCase
from ion.services.cei.queuestat import QueueStatClient

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class TestQueueStatService(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):

        if not os.path.exists(os.path.expanduser('~/.erlang.cookie')):
            raise unittest.SkipTest('Needs a RabbitMQ server on localhost')

        log.debug('Temporarily changing broker_host to 127.0.0.1')
        self.other_broker_host = ion.test.iontest.CONF.obj['broker_host']
        ion.test.iontest.CONF.obj['broker_host'] = '127.0.0.1'

        yield self._start_container()
        procs = [
            {'name':'queuestat','module':'ion.services.cei.queuestat', 
                'class':'QueueStatService', 
                'spawnargs' : {'interval_seconds' : 0.1}},
                ]
        self.sup = yield self._spawn_processes(procs)
        
        id = str(uuid.uuid4())
        id = id[id.rfind('-')+1:] # shorter id
        self.queuename = '_'.join((__name__, id))
        messaging = {self.queuename: 
                {'name_type':'worker', 'args':{'scope':'global'}}}
        yield self._declare_messaging(messaging)
        #TODO is there some need/way to clean up this queue?

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

        log.debug('Resetting broker_host')
        ion.test.iontest.CONF.obj['broker_host'] = self.other_broker_host

    @defer.inlineCallbacks
    def test_queue_stat(self):
        subscriber = TestSubscriber()
        subId = yield self._spawn_process(subscriber)
        queuestat_client = QueueStatClient(subscriber)

        yield queuestat_client.watch_queue(self.queuename, str(subId), 'stat')
        yield pu.asleep(0.3)

        assert subscriber.queue_length[self.queuename] == 0
        
        yield self._add_messages(5)
        yield pu.asleep(0.3)
        assert subscriber.queue_length[self.queuename] == 5
        
        yield self._add_messages(3)
        yield pu.asleep(0.3)
        assert subscriber.queue_length[self.queuename] == 8

        yield pu.asleep(0.3)
        yield queuestat_client.unwatch_queue(self.queuename, str(subId), 'stat')
        
        yield self._add_messages(3)
        yield pu.asleep(0.3)
        assert subscriber.queue_length[self.queuename] == 8


    @defer.inlineCallbacks
    def _add_messages(self, count):
        for i in range(count):
            yield self.sup.send(self.queuename, 'work', 
                {'deal' : 'this is a fake message'})


class TestSubscriber(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)

        self.queue_length = {}
        self.recv_count = {}
    
    def op_stat(self, content, headers, msg):
        log.info('Got queuestat: %s', content)

        q = content['queue_name']
        self.queue_length[q] = content['queue_length']

        count = self.recv_count.get(q, None)
        self.recv_count[q] = count + 1 if count else 1

