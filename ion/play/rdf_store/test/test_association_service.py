#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging

from twisted.internet import defer

from ion.play.rdf_store.association_service import AssociationServiceClient
from ion.test.iontest import IonTestCase

from ion.data.dataobject import DataObject
from ion.data.objstore import ValueObject

class AssociationTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [{'name':'association1','module':'ion.play.rdf_store.association_service','class':'AssociationService'},]

        sup = yield self._spawn_processes(services)

        self.bsc = AssociationServiceClient(proc=sup)
        
        d = dict()
        d['S']='key1'
        d['O']='key2'
        d['P']='key3'
        
        self.dobj=DataObject.from_encoding(d)
        assoc = ValueObject(self.dobj.encode())
        self.dobj_key=assoc.identity
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_put_get_delete(self):

        res = yield self.bsc.put_association(self.dobj)
        self.assertEqual(res,self.dobj_key)
        

        res = yield self.bsc.get_association(self.dobj_key)
        self.assertEqual(res,self.dobj)
        
        res = yield self.bsc.del_association(self.dobj_key)
        self.assertEqual(res,'success')
        
        
        