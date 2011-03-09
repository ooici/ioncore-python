#!/usr/bin/env python

"""
@file ion/play/test/test_hello_policy.py
@test ion.play.hello_policy Example unit tests for sample code.
@author Thomas Lennan
"""

from twisted.internet import defer

from ion.play.hello_policy import HelloPolicyClient
from ion.test.iontest import IonTestCase
import ion.util.ionlog
from ion.core import ioninit

import time

from ion.util.itv_decorator import itv

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

class HelloPolicyTest(IonTestCase):
    """
    Testing example hello policy.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'hello_policy','module':'ion.play.hello_policy','class':'HelloPolicy'}
        ]

        sup = yield self._spawn_processes(services)
        self.hc = HelloPolicyClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello_anonymous_request_anonymous(self):
        # Default send behavior today.
        #
        # Call service via service client.  Service client will use standard
        # RPC send. This will be an ANONYMOUS request. Service operation
        # in res/config/ionpolicydb.cfg has been marked as allowing
        # anonymous access.
        #
        # HelloPolicyClient ---> rpc_send() ---> HelloPolicy 
        result = yield self.hc.hello_request('hello_anonymous_request')
        self.assertEqual(result, "{'value': 'ANONYMOUS'}")

    @defer.inlineCallbacks
    def test_hello_anonymous_request_userid(self):
        # New behavior illustrating rpc_send_protected method and policy enforcement.
        # Service operation in res/config/ionpolicydb.cfg has been marked as
        # allowing anonymous access.
        #
        # Call service via service client.  Service client will use protected
        # RPC send, passing user id 'MYUSER'.
        # HelloPolicyClient ---> rpc_send_protected('MYUSER') ---> HelloPolicy
        current_time = int(time.time())
        expiry = str(current_time + 30)

        result = yield self.hc.hello_request('hello_anonymous_request', 'MYUSER', expiry)
        self.assertEqual(result, "{'value': 'MYUSER'}")

    @defer.inlineCallbacks
    def test_hello_authenticated_request_fail_not_auth(self):
        # Default send behavior today.
        #
        # Call service via service client.  Service client will use standard
        # RPC send. This will be an ANONYMOUS request. However, this test
        # will fail because Service operation in res/config/ionpolicydb.cfg
        # has been marked as requiring authenticated access.
        #
        # HelloPolicyClient ---> rpc_send() ---> HelloPolicy 
        sent = False
        try:
            result = yield self.hc.hello_request('hello_authenticated_request')
            sent = True
        except:
            pass
        # for now, expect timeout
        self.assertFalse(sent)

    @defer.inlineCallbacks
    def test_hello_authenticated_request(self):
        # New behavior illustrating rpc_send_protected method and policy enforcement.
        # Service operation in res/config/ionpolicydb.cfg has been marked as
        # requiring authenticated access.
        #
        # Call service via service client.  Service client will use protected
        # RPC send, passing user id 'MYUSER'.
        # HelloPolicyClient ---> rpc_send_protected('MYUSER') ---> HelloPolicy
        current_time = int(time.time())
        expiry = str(current_time + 30)
        
        result = yield self.hc.hello_request('hello_authenticated_request', 'MYUSER', expiry)
        self.assertEqual(result, "{'value': 'MYUSER'}")

    @defer.inlineCallbacks
    def test_hello_authenticated_request_fail_expiry(self):
        # New behavior illustrating rpc_send_protected method and policy enforcement.
        # Service operation in res/config/ionpolicydb.cfg has been marked as
        # requiring authenticated access.  However, expiry will be exceeded.
        #
        # Call service via service client.  Service client will use protected
        # RPC send, passing user id 'MYUSER'.
        # HelloPolicyClient ---> rpc_send_protected('MYUSER') ---> HelloPolicy
        expiry = '12345'
        
        sent = False
        try:
            result = yield self.hc.hello_request('hello_authenticated_request', 'MYUSER', expiry)
            sent = True
        except:
            pass
        # for now, expect timeout
        self.assertFalse(sent)

