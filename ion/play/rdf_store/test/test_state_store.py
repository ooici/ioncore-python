#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging

from twisted.internet import defer

from ion.play.rdf_store.state_service import StateServiceClient
from ion.test.iontest import IonTestCase

from ion.data.dataobject import DataObject
from ion.data.objstore import ValueObject

class StateTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [{'name':'StateStore1','module':'ion.play.rdf_store.state_service','class':'StateStoreService'},]

        sup = yield self._spawn_processes(services)

        self.ssc = StateServiceClient(proc=sup)
        
        self.state1=set([5,'t',39.0])
        self.state2=set([6,'d',39.0])
        self.key = 'key1'
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_put_get_update(self):

        cref1 = yield self.ssc.put(self.key,self.state1)
        print 'Put Result', cref1
        
        res = yield self.ssc.get(self.key)
        print 'Get key (1)', res
        self.assertEqual(res,self.state1)
        
        cref2 = yield self.ssc.put(self.key,self.state2,parents=cref1)
        
        res = yield self.ssc.get(self.key)
        print 'Get key (2)', res
        self.assertEqual(res,self.state2)
        
        # Can't get previous yet
        res = yield self.ssc.get(self.key,commit=cref1)
        print 'Get Cref1', res
        self.assertEqual(res,self.state1)
        
        
        