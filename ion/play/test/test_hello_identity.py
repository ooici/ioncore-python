#!/usr/bin/env python

"""
@file ion/play/test/test_hello_identity.py
@test ion.play.hello_identity Example unit tests for sample code.
@author Thomas Lennan
"""

from twisted.internet import defer

from ion.play.hello_identity import HelloIdentityClient
from ion.play.hello_identity_delegate import HelloIdentityDelegateClient
from ion.test.iontest import IonTestCase
import ion.util.ionlog
from ion.core import ioninit

import time

from ion.util.itv_decorator import itv

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

class HelloIdentityTest(IonTestCase):
    """
    Testing example hello identity.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'hello_identity','module':'ion.play.hello_identity','class':'HelloIdentity'},
            {'name':'hello_identity_delegate','module':'ion.play.hello_identity_delegate','class':'HelloIdentityDelegate'}
        ]

        sup = yield self._spawn_processes(services)
        self.hc = HelloIdentityClient(proc=sup)
        self.hcd = HelloIdentityDelegateClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello_anonymous_request(self):
        # Default behavior today.
        #
        # Call service via service client.  Service client will use standard
        # RPC send. This will be an ANONYMOUS request.
        # HelloIdentityClient ---> rpc_send() ---> HelloIdentity 
        result = yield self.hc.hello_request('hello_anonymous_request')
        self.assertEqual(result, "{'value': 'Hello there ANONYMOUS'}")

    @defer.inlineCallbacks
    def test_hello_user_request(self):
        # New behavior illustrating rpc_send_protected method to allow passing
        # of user id and expiry in message header.
        #
        # Call service via service client.  Service client will use protected
        # RPC send, passing user id 'MYUSER'.
        # HelloIdentityClient ---> rpc_send_protected('MYUSER') ---> HelloIdentity 
        current_time = int(time.time())
        expiry = str(current_time + 30)

        result = yield self.hc.hello_request('hello_user_request', 'MYUSER', expiry)
        self.assertEqual(result, "{'value': 'Hello there MYUSER'}")

    @defer.inlineCallbacks
    def test_hello_anonymous_delegate_request(self):
        # Default behavior today.
        #
        # Call delegating service via service client using standard RPC send.
        # Service will then use service client of "backend" service to send
        # standard RPC to complete request.
        # HelloIdentityDelegateClient ---> rpc_send() ---> HelloIdentityDelegate  ---> HelloIdentityClient ---> rpc_send() ---> HelloIdentity
        result = yield self.hcd.hello_request('hello_anonymous_request_delegate')
        self.assertEqual(result, "{'value': 'Hello there ANONYMOUS'}")

    @defer.inlineCallbacks
    def test_hello_user_delegate_request(self):
        # New behavior illustrating first level RPC send protected passing user id
        # and expiry on to subordinate service calls.
        #
        # Call delegating service via service client using protected RPC send.
        # Service will then use service client of "backend" service to send
        # standard RPC to complete request.
        # HelloIdentityDelegateClient ---> rpc_send_protected('MYUSER') ---> HelloIdentityDelegate  ---> HelloIdentityClient ---> rpc_send() ---> HelloIdentity
        current_time = int(time.time())
        expiry = str(current_time + 30)

        result = yield self.hcd.hello_request('hello_user_request_delegate', 'MYUSER', expiry)
        self.assertEqual(result, "{'value': 'Hello there MYUSER'}")

    @defer.inlineCallbacks
    def test_hello_user_delegate_request_switch_users(self):
        # Oddball situation, but illustrates how an 'Application' layer service
        # could call RPC send protected to utilize a different user id and expiry
        # on subordinate service calls.
        #
        # Call delegating service via service client using protected RPC send.
        # Service will then use service client of "backend" service to send
        # protected RPC to complete request.
        # HelloIdentityDelegateClient ---> rpc_send_protected('MYUSER') ---> HelloIdentityDelegate  ---> HelloIdentityClient ---> rpc_send_protected('NEWUSER') ---> HelloIdentity
        current_time = int(time.time())
        expiry = str(current_time + 30)

        result = yield self.hcd.hello_request('hello_user_request_delegate_switch_user', 'MYUSER', expiry)
        self.assertEqual(result, "{'value': 'Hello there NEWUSER'}")

                

