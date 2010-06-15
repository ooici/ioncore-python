#!/usr/bin/env python

"""
@file ion/services/cei/test/test_provisioner_store.py
@author David LaBissoniere
@brief Test provisioner store behavior
"""

import logging
import uuid

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.services.cei.provisioner_store import ProvisionerStore, group_records
from ion.services.cei import states

class ProvisionerStoreTests(IonTestCase):
    """Testing the provisioner datastore abstraction
    """
    def setUp(self):
        self.store = ProvisionerStore()
    
    def tearDown(self):
        self.store = None
    
    @defer.inlineCallbacks
    def test_put_get_states(self):

        launch_id = new_id()
        
        records = [{'launch_id' : launch_id, 'node_id' : new_id(), 
            'state' : states.Requested} for i in range(5)]

        yield self.store.put_records(records)

        result = yield self.store.get_all()
        self.assertEqual(len(result), len(records))

        one_rec = records[0]
        yield self.store.put_record(one_rec, states.Pending)
        result = yield self.store.get_all()
        self.assertEqual(len(result), len(records)+1)
        
        result = yield self.store.get_all(launch_id, one_rec['node_id'])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['state'], states.Pending)
        self.assertEqual(result[1]['state'], states.Requested)

    def test_group_records(self):
        records = [
                {'site' : 'chicago', 'allocation' : 'big', 'name' : 'sandwich'},
                {'name' : 'pizza', 'allocation' : 'big', 'site' : 'knoxville'},
                {'name' : 'burrito', 'allocation' : 'small', 'site' : 'chicago'}
                ]

        groups = group_records(records, 'site')
        self.assertEqual(len(groups.keys()), 2)
        chicago = groups['chicago']
        self.assertTrue(isinstance(chicago, list))
        self.assertEqual(len(chicago), 2)
        self.assertEqual(len(groups['knoxville']), 1)

        groups = group_records(records, 'site', 'allocation')
        self.assertEqual(len(groups.keys()), 3)
        chicago_big = groups[('chicago','big')]
        self.assertEqual(chicago_big[0]['allocation'], 'big')
        self.assertEqual(chicago_big[0]['site'], 'chicago')
        for group in groups.itervalues():
            self.assertEqual(len(group), 1)

def new_id():
    return str(uuid.uuid4())
