#!/usr/bin/env python

"""
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.test.iontest import IonTestCase

from ion.core.process.process import Process, ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core import bootstrap
from twisted.trial import unittest

from ion.core.object import object_utils
from ion.core.object import workbench
from ion.core.messaging import message_client

from ion.util import procutils as pu

# Static entry point for "thread local" context storage during request
# processing, eg. to retaining user-id from request message
from ion.core.ioninit import request

ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)

class ReceiverService(ServiceProcess):

    declare = ServiceProcess.service_declare(name='receiver_service',
                                          version='0.1.1',
                                          dependencies=[])


    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.

        ServiceProcess.__init__(self, *args, **kwargs)

        self._a_time = self.spawn_args.get('a_time',0)
        self._b_time = self.spawn_args.get('b_time',0)

        self.action = defer.Deferred()
        #self.action_b = defer.Deferred()


    @defer.inlineCallbacks
    def op_a(self, content, headers, msg):
        """
        Dummy operation that takes 'a_time' seconds to complete
        """
        log.info('Starting Op A: time=%d' % self._a_time)

        context = request.get('workbench_context', 'None Set!')
        log.info('Got Context: "%s"' % context)
        self.action.callback(context)

        log.info('Made callback, going to sleep')
        yield pu.asleep(self._a_time)

        log.info('Replying OK')
        yield self.reply_ok(msg, content)

        log.info('Op A Complete!')


    @defer.inlineCallbacks
    def op_b(self, content, headers, msg):
        """
        Dummy operation that takes 'b_time' seconds to complete
        """
        log.info('Starting Op B: time=%d' % self._b_time)

        context = request.get('workbench_context', [])
        log.info('Got Context: "%s"' % context)
        self.action.callback(context)

        log.info('Made callback, going to sleep')
        yield pu.asleep(self._b_time)

        log.info('Replying OK')
        yield self.reply_ok(msg, content)

        log.info('Op B Complete!')



factory = ProcessFactory(ReceiverService)


class ReceiverServiceClient(ServiceClient):

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'receiver_service'
        ServiceClient.__init__(self, proc, **kwargs)


    @defer.inlineCallbacks
    def a(self, msg):
        """
        @brief Call op_a
        @retval ok
        """
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('a', msg)
        #defer.returnValue(ret)

        defer.returnValue((ret, heads, message))

    @defer.inlineCallbacks
    def b(self, msg):
        """
        @brief Call op_a
        @retval ok
        """
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('b', msg)
        defer.returnValue((ret, heads, message))

