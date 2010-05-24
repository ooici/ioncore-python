#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging

from twisted.internet import defer

from ion.play.rdf_store.reference_service import ReferenceServiceClient
from ion.test.iontest import IonTestCase

from ion.data.dataobject import DataObject
from ion.data.objstore import ValueObject

class ReferenceTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [{'name':'reference1','module':'ion.play.rdf_store.reference_service','class':'ReferenceService'},]

        sup = yield self._spawn_processes(services)

        self.bsc = ReferenceServiceClient(proc=sup)
        
        self.key='key1'
        self.ref1='ref1'
        self.ref2='ref2'
        self.ref3='ref3'
        self.ref4='ref4'
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_put_get_delete(self):

        res = yield self.bsc.add_reference(self.key,self.ref1)
        self.assertEqual(res,self.key)
        

        res = yield self.bsc.add_reference(self.key,self.ref2)
        res = yield self.bsc.add_reference(self.key,self.ref4)
        res = yield self.bsc.add_reference(self.key,self.ref3)


        res = yield self.bsc.get_references(self.key)
        self.assertEqual(set(res),set([self.ref1,self.ref2,self.ref3,self.ref4]))
        
        res = yield self.bsc.del_reference(self.key,self.ref1)
        self.assertEqual(res,'success')
        
        
        res = yield self.bsc.get_references(self.key)
        self.assertEqual(set(res),set([self.ref2,self.ref3,self.ref4]))
        
        