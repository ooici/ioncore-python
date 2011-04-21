#!/usr/bin/env python

"""
@file ion/core/process/test/test_service.py
@author David Stuebe
@brief test case for process base class
"""

import ion.core.ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer

from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.exception import ReceivedContainerError, ReceivedApplicationError

from ion.core.process.test import test_process
from ion.core.process.process import ProcessFactory

from ion.test.iontest import IonTestCase


class EchoService(ServiceProcess, test_process.EchoProcess):

    declare = ServiceProcess.service_declare(name='echo_service',
                                          version='0.1.1',
                                          dependencies=[])

factory = ProcessFactory(EchoService)


class EchoServiceClient(ServiceClient):

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'echo_service'
        ServiceClient.__init__(self, proc, **kwargs)


    @defer.inlineCallbacks
    def echo(self, msg):
        """
        @brief Add a recurring task to the scheduler
        @param msg is any protocol buffer
        @retval
        """
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('echo', msg)
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def echo_fail(self, msg):
        """
        @brief Add a recurring task to the scheduler
        @param msg is any protocol buffer
        @retval
        """
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('echo_fail', msg)
        defer.returnValue(ret)


    @defer.inlineCallbacks
    def echo_exception(self, msg):
        """
        @brief Add a recurring task to the scheduler
        @param msg is any protocol buffer
        @retval
        """
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('echo_exception', msg)
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def echo_apperror(self, msg):
        """
        @brief Add a recurring task to the scheduler
        @param msg is any protocol buffer
        @retval
        """
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('echo_apperror', msg)
        defer.returnValue(ret)


class EchoServiceTest(IonTestCase):

    services = [
            {'name':'echo_service','module':'ion.core.process.test.test_service','class':'EchoService'},
            ]

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        yield self._spawn_processes(self.services)

        # Don't create send_content and echo_client here - this method is over-ridden in ion-integration
        # Want to keep the same tests - but change setUp and tearDown!

    @defer.inlineCallbacks
    def tearDown(self):

        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_echo(self):

        self.send_content = 'content123'

        self.echo_client = EchoServiceClient()

        result_content = yield self.echo_client.echo(self.send_content)
        self.assertEqual(result_content, self.send_content)

    @defer.inlineCallbacks
    def test_echo_fail(self):

        self.send_content = 'content123'

        self.echo_client = EchoServiceClient()
        yield self.failUnlessFailure(self.echo_client.echo_fail(self.send_content), ReceivedApplicationError)

    @defer.inlineCallbacks
    def test_echo_exception(self):
        self.send_content = 'content123'

        self.echo_client = EchoServiceClient()
        yield self.failUnlessFailure(self.echo_client.echo_exception(self.send_content), ReceivedContainerError)

    @defer.inlineCallbacks
    def test_echo_apperror(self):
        self.send_content = 'content123'

        self.echo_client = EchoServiceClient()
        yield self.failUnlessFailure(self.echo_client.echo_apperror(self.send_content), ReceivedApplicationError)

    