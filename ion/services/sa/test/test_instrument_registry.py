#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@author David Stuebe
@brief test service for registering resources and client classes
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.data.datastore import registry

from ion.services.sa.instrument_registry import InstrumentRegistryClient
from ion.test.iontest import IonTestCase
from ion.data import dataobject
from ion.resources import sa_resource_descriptions


class InstrumentRegistryTest(IonTestCase):
    """
    Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._start_core_services()
        services = [
            {'name':'resourceregistry1','module':'ion.services.sa.instrument_registry','class':'InstrumentRegistryService'}]
        sup = yield self._spawn_processes(services)

        self.irc = InstrumentRegistryClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.irc.clear_registry
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_resource_reg(self):
        # put in a bogus resource for now...
        res = sa_resource_descriptions.InstrumentResource.create_new_resource()
        res = yield self.irc.register_instrument_type(res)
        
        #res = yield self.irc.set_resource_lcstate_commissioned(res)
        
        ref = res.reference(head=True)
        
        res2 = yield self.irc.get_instrument_type(ref)
        
        res2.tau = -909.0
        
        res2 = yield self.irc.register_instrument_type(res2)
        
        res3 = yield self.irc.get_instrument_type(ref)
        
        self.assertEqual(res3.tau,-909.0)
        #print res3
        


class ResourceRegistryCoreServiceTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.sup = yield self._start_core_services()
        #logging.info('self.sup.proc_state'+str(self.sup.proc_state))
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


