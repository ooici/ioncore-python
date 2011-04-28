#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@author David Stuebe
@brief test service for registering resources and client classes
"""
'''
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.sa.instrument_registry import InstrumentRegistryClient
from ion.test.iontest import IonTestCase
from ion.resources import sa_resource_descriptions


class InstrumentRegistryTest(IonTestCase):
    """
    Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'inst_registry','module':'ion.services.sa.instrument_registry','class':'InstrumentRegistryService'}]
        sup = yield self._spawn_processes(services)

        self.irc = InstrumentRegistryClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.irc.clear_registry()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_resource_reg(self):
        # put in a bogus resource for now...
        res = sa_resource_descriptions.InstrumentResource.create_new_resource()
        res = yield self.irc.register_instrument_type(res)

        #res = yield self.irc.set_resource_lcstate_commissioned(res)

        ref = res.reference(head=True)

        res2 = yield self.irc.get_instrument_type(ref)

        res2.manufacturer = "SeaBird Electronics"

        res2 = yield self.irc.register_instrument_type(res2)

        res3 = yield self.irc.get_instrument_type(ref)

        self.assertEqual(res3.manufacturer, "SeaBird Electronics")
'''
