#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_pubsub.py
@author Paul Hubbard
@test ion.services.dm.distribution.pubsub_service Test suite for revised pubsub code
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.services.dm.distribution.pubsub_service import PubSubClient, \
    REQUEST_TYPE, REGEX_TYPE, XP_TYPE, XS_TYPE, PUBLISHER_TYPE, SUBSCRIBER_TYPE

from ion.test.iontest import IonTestCase
from twisted.trial import unittest
from ion.util.procutils import asleep
from ion.core import ioninit

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient

from ion.core.exception import ReceivedApplicationError

from ion.util.itv_decorator import itv

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)


class PST(IonTestCase):
    """
    New tests to match the updated code for R1C3
    """
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 5
        services = [
            {
                'name':'pubsub_service',
                'module':'ion.services.dm.distribution.pubsub_service',
                'class':'PubSubService'
            },
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                    'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry_beta.resource_registry',
                'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}},
            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },

            ]
        yield self._start_container()
        self.sup = yield self._spawn_processes(services)
        self.psc = PubSubClient(self.sup)

        # Fixed parameters for these tests
        self.xs_name = 'swapmeet'
        self.xp_name = 'science_data'
        self.topic_name = 'http://ooici.net:8001/coads.nc'
        self.publisher_name = 'Otto Niemand' # Hey, it's thematically correct.
        self.credentials = 'Little to none'

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    def test_start_stop(self):
        pass

    @defer.inlineCallbacks
    def _create_xs(self):
        # Try and create the 'swapmeet' exchange space

        msg = yield self.create_message(XS_TYPE)
        msg.exchange_space_name = self.xs_name

        xs_id = yield self.psc.declare_exchange_space(msg)
        defer.returnValue(xs_id)


    @defer.inlineCallbacks
    def test_xs_creation(self):
        # Try and create the 'swapmeet' exchange space
        xs_id = yield self._create_xs()

        self.failIf(len(xs_id.id_list) == 0)
        self.failIf(xs_id.id_list[0] == '')

    @defer.inlineCallbacks
    def test_xs_exceptions(self):
        """
        Test new exception raising
        """
        wrong_type = object_utils.create_type_identifier(object_id=10, version=1)
        bad_msg = yield self.create_message(wrong_type)

        try:
            yield self.psc.declare_exchange_space(bad_msg)
        except ReceivedApplicationError:
            pass
        else:
            self.fail('Did not get the expected exception from a bad request to PSC!')

    @defer.inlineCallbacks
    def test_undeclare_xs(self):

        xs_id = yield self._create_xs()

        msg = yield self.create_message(REQUEST_TYPE)
        msg.resource_reference = xs_id.id_list[0]

        yield self.psc.undeclare_exchange_space(msg)

    @defer.inlineCallbacks
    def test_bad_xs_creation(self):
        raise unittest.SkipTest('EMS doesnt do paramater validation yet')
        # Make sure it fails if you skip the argument

        msg = yield self.create_message(XS_TYPE)

        xs_id = yield self.psc.declare_exchange_space(msg)

        self.failIf(len(xs_id.id_list) > 0)

    @defer.inlineCallbacks
    def test_xs_query(self):
        xs_id = yield self._create_xs()

        self.failUnless(len(xs_id.id_list) > 0)
        log.debug('exchange declared')
        msg = yield self.create_message(REGEX_TYPE)
        msg.regex = self.xs_name

        log.debug('querying now')
        idlist = yield self.psc.query_exchange_spaces(msg)
        self.failUnless(len(idlist.id_list) > 0)

    @defer.inlineCallbacks
    def _create_xp(self, xs_id):
        msg = yield self.create_message(XP_TYPE)
        msg.exchange_point_name = self.xp_name
        msg.exchange_space_id = xs_id.id_list[0]

        xp_id = yield self.psc.declare_exchange_point(msg)
        defer.returnValue(xp_id)

    @defer.inlineCallbacks
    def test_xp_creation(self):
        xs_id = yield self._create_xs()
        xp_id = yield self._create_xp(xs_id)

        self.failUnless(len(xp_id.id_list) > 0)

    @defer.inlineCallbacks
    def test_undeclare_xp(self):
        xs_id = yield self._create_xs()
        xp_id = yield self._create_xp(xs_id)

        msg = self.create_message(REQUEST_TYPE)
        msg.resource_reference = xp_id

        # Should throw an error if problem, trial will catch same as failure
        yield self.psc.undeclare_exchange_point(msg)

    @defer.inlineCallbacks
    def _declare_topic(self, xs=None, xp=None):
        if not xs:
            xs = yield self._create_xs()
        if not xp:
            xp = yield self._create_xp(xs_id)

        msg = yield self.create_message(TOPIC_TYPE)
        msg.exchange_space_id = xs
        msg.exchange_point_id = xp
        msg.topic_name = self.topic_name

        topic_id = yield self.psc.declare_topic(msg)
        defer.returnValue(topic_id)

    @defer.inlineCallbacks
    def test_declare_topic(self):
        topic_id = yield self._declare_topic()
        self.failUnless(len(topic_id.id_list) > 0)

    @defer.inlineCallbacks
    def test_undeclare_topic(self):
        topic_id = yield self._declare_topic()
        self.failUnless(len(topic_id.id_list) > 0)
        msg = self.create_message(REQUEST_TYPE)
        msg.resource_reference = topic_id

        yield self.psc.undeclare_topic(msg)

        # @todo do a query and verify it's gone...

    @defer.inlineCallbacks
    def test_query_topics(self):
        yield self._declare_topic()

        msg = self.create_message(REGEX_TYPE)
        msg.regex = '.+'

        topic_list = self.psc.query_topics(msg)

        self.failUnless(len(topic_list.id_list) >= 1)

    @defer.inlineCallbacks
    def _declare_publisher(self):
        xs_id = yield self._create_xs()
        xp_id = yield self._create_xp(xs_id)
        topic_id = yield self._declare_topic(xs=xsid, xp=xp_id)

        msg = self.create_message(PUBLISHER_TYPE)
        msg.exchange_space_id = xs_id
        msg.exchange_point_id = xp_id
        msg.topic_id = topic_id
        msg.publisher_name = self.publisher_name
        msg.credentials = self.credentials

        pid = yield self.psc.declare_publisher(msg)
        defer.returnValue(pid)

    @defer.inlineCallbacks
    def test_declare_publisher(self):
        pid = yield self._declare_publisher()
        self.failUnless(len(pid.id_list) > 0)

    @defer.inlineCallbacks
    def test_subscribe(self):
        xs_id = yield self._create_xs()
        xp_id = yield self._create_xp(xs_id)
        topic_id = yield self._declare_topic(xs=xsid, xp=xp_id)

        msg = self.create_message(SUBSCRIBER_TYPE)

        msg.exchange_space_id = xs_id
        msg.exchange_point_id = xp_id
        msg.topic_id = topic_id

        rc = yield self.psc.subscribe(msg)
        self.failUnless(len(rc.id_list) > 0)

    @defer.inlineCallbacks
    def test_declare_queue(self):
        pass

    @defer.inlineCallbacks
    def test_add_binding(self):
        pass
    


        


    

        
