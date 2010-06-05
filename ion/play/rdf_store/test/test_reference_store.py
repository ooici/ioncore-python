#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.play.rdf_store.reference_store import ReferenceStore


class ReferenceTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.rs=ReferenceStore()
        yield self.rs.init()
        
        self.key='key1'
        self.ref1='ref1'
        self.ref2='ref2'
        self.ref3='ref3'
        self.ref4='ref4'
        

    @defer.inlineCallbacks
    def test_put_get_delete(self):

        res = yield self.rs.add_references(self.key,self.ref1)
        self.assertEqual(res,None)
        

        res = yield self.rs.add_references(self.key,(self.ref2,self.ref3,self.ref4))


        res = yield self.rs.get_references(self.key)
        self.assertEqual(set(res),set([self.ref1,self.ref2,self.ref3,self.ref4]))
        
        res = yield self.rs.remove_references(self.key,self.ref1)
        self.assertEqual(res,None)
        
        
        res = yield self.rs.get_references(self.key)
        self.assertEqual(set(res),set([self.ref2,self.ref3,self.ref4]))
        
        