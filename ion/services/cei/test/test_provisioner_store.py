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

from ion.services.cei.provisioner_store import ProvisionerStore
from ion.services.cei import states

class ProvisionerStoreTests(IonTestCase):
    """Testing interaction patterns between Provisioner and SensorAggregator.
    """
    def setUp(self):
        self.store = ProvisionerStore()
    
    def tearDown(self):
        self.store = None
    
    @defer.inlineCallbacks
    def test_put_get_states(self):
        
        record = {'a' : 'fooo', 'b' : 'fooooop'}
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        
        yield self.store.put_state(id1, states.Pending, record)
        yield self.store.put_state(id1, states.Pending, record)
        top_state = yield self.store.put_state(id1, states.Running, record)
        yield self.store.put_state(id1, states.Pending, record)
        
        yield self.store.put_state(id2, states.Pending, record)
        
        records = yield self.store.get_states(id1)
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0][0], top_state)
