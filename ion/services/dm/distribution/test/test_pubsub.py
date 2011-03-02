#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_pubsub.py
@author Paul Hubbard
@test ion.services.dm.distribution.pubsub_service Test suite for revised pubsub code
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.services.dm.distribution.pubsub_service import PubSubClient, REQUEST_TYPE, REGEX_TYPE, XP_TYPE, XS_TYPE

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
        self.mc = MessageClient(proc=self.sup)

        self.xs_name = 'swapmeet'
        self.xp_name = 'science_data'
        self.topic_name = 'http://ooici.net:8001/coads.nc'

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    def test_start_stop(self):
        pass

    @defer.inlineCallbacks
    def test_xs_creation(self):
        # Try and create the 'swapmeet' exchange space

        msg = yield self.mc.create_instance(XS_TYPE)
        msg.exchange_space_name = self.xs_name

        xs_id = yield self.psc.declare_exchange_space(msg)

        self.failIf(len(xs_id.id_list) == 0)
        self.failIf(xs_id.id_list[0] == '')


    @defer.inlineCallbacks
    def test_xs_exceptions(self):
        """
        Test new exception raising
        """
        wrong_type = object_utils.create_type_identifier(object_id=10, version=1)
        bad_msg = yield self.mc.create_instance(wrong_type)

        try:
            yield self.psc.declare_exchange_space(bad_msg)
        except ReceivedApplicationError:
            pass
        else:
            self.fail('Did not get the expected exception from a bad request to PSC!')

    @defer.inlineCallbacks
    def test_undeclare_xs(self):
        msg = yield self.mc.create_instance(XS_TYPE)
        msg.exchange_space_name = self.xs_name

        xs_id = yield self.psc.declare_exchange_space(msg)

        msg = yield self.mc.create_instance(REQUEST_TYPE)
        msg.resource_reference = xs_id.id_list[0]

        yield self.psc.undeclare_exchange_space(msg)

    @defer.inlineCallbacks
    def test_bad_xs_creation(self):
        raise unittest.SkipTest('EMS doesnt do paramater validation yet')
        # Make sure it fails if you skip the argument

        msg = yield self.mc.create_instance(XS_TYPE)

        xs_id = yield self.psc.declare_exchange_space(msg)

        self.failIf(len(xs_id.id_list) > 0)

    @defer.inlineCallbacks
    def test_xs_query(self):

        msg = yield self.mc.create_instance(XS_TYPE)
        msg.exchange_space_name = self.xs_name

        xs_id = yield self.psc.declare_exchange_space(msg)

        self.failUnless(len(xs_id.id_list) > 0)
        log.debug('exchange declared')
        msg = yield self.mc.create_instance(REGEX_TYPE)
        msg.regex = self.xs_name

        log.debug('querying now')
        idlist = yield self.psc.query_exchange_spaces(msg)
        log.debug(idlist)

    @defer.inlineCallbacks
    def test_xp_creation(self):
        msg = yield self.mc.create_instance(XS_TYPE)
        msg.exchange_space_name = self.xs_name

        xs_id = yield self.psc.declare_exchange_space(msg)

        msg = yield self.mc.create_instance(XP_TYPE)
        msg.exchange_point_name = self.xp_name
        msg.exchange_space_id = xs_id.id_list[0]

        xp_id = yield self.psc.declare_exchange_point(msg)

        self.failUnless(len(xp_id.id_list) > 0)