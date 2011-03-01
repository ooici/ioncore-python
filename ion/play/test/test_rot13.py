#!/usr/bin/env python

"""
@file ion/play/test/test_rot13.py
@author Paul Hubbard
@date 3/1/11
@brief Unit and integration tests of the example rot13 service
"""

from twisted.internet import defer
from twisted.trial import unittest

from ion.play.rot13_service import Rot13Service, Rot13Client, REQUEST_TYPE, RESPONSE_TYPE

from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils
from ion.core.exception import ReceivedApplicationError
from ion.test.iontest import IonTestCase
import ion.util.ionlog
from ion.core import ioninit

from ion.util.itv_decorator import itv

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)
BAD_MSG = object_utils.create_type_identifier(object_id=10, version=1)

class RUnit(IonTestCase):
    def setUp(self):
        self.timeout = 5

    def notest_algorithm(self):
        """
        Unit test of rot13 algorithm plg GPB mechanics.
        """
        dut = Rot13Service()
        rc = dut.rot13('abcsdsd')
        self.failUnlessEqual('nopfqfq', rc)

class R13Test(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 5
        # idea - typo the service name
        services = [{'name':'rot13',
                     'module': 'ion.play.rot13_service',
                     'class' : 'Rot13Service'}]

        self.in_str = 'abcsdsd'
        self.out_str = 'nopfqfq'

        yield self._start_container()
        sup = yield self._spawn_processes(services)

        self.mc = MessageClient(proc=sup)
        self.rclient = Rot13Client(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    #######################################################
    # Integration tests, declared as such via itv decorator
    @itv(CONF)
    def test_start_stop(self):
        """
        Start and stop the container and service
        """
        pass

    @itv(CONF)
    @defer.inlineCallbacks
    def test_full_stack(self):
        """
        Run the same string through, using messaging and the container
        """
        msg = yield self.mc.create_instance(REQUEST_TYPE)
        msg.input_string = self.in_str

        try:
            rc = yield self.rclient.rot13(msg)
        except ReceivedApplicationError:
            log.exception('Error invoking rot13 service')
            self.fail('Unexpected error')

        self.failUnless(rc.MessageType == RESPONSE_TYPE)
        self.failUnlessEqual(self.out_str, rc.output_string)

        # Verify that a wrong GPB throws an exception
        msg = yield self.mc.create_instance(BAD_MSG)
        try:
            yield self.rclient.rot13(msg)
        except ReceivedApplicationError:
            pass
        else:
            self.fail('Should have an error from a bad message!')




