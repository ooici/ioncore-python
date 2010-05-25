#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging

from twisted.internet import defer

from ion.play.rdf_store.blob_service import BlobServiceClient
from ion.test.iontest import IonTestCase

from ion.data.dataobject import DataObject
from ion.data.objstore import ValueObject

class BlobTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [{'name':'blob1','module':'ion.play.rdf_store.blob_service','class':'BlobService'},]

        sup = yield self._spawn_processes(services)

        self.bsc = BlobServiceClient(proc=sup)
        
        d = dict()
        d['a']=1
        d['b']='b'
        d['c']=3.14159
        
        self.dobj=DataObject.from_encoding(d)
        blob = ValueObject(self.dobj.encode())
        self.dobj_key=blob.identity
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_put_get_delete(self):

        res = yield self.bsc.put_blob(self.dobj)
        self.assertEqual(res,self.dobj_key)
        

        res = yield self.bsc.get_blob(self.dobj_key)
        self.assertEqual(res,self.dobj)
        
        res = yield self.bsc.del_blob(self.dobj_key)
        self.assertEqual(res,'success')
        
        
        