#!/usr/bin/env python

"""
@file ion/services/coi/test/test_datastore.py
@author Michael Meisinger
@test Protocol+factory test of datastore
"""


from twisted.trial import unittest
import logging

from io.services.coi.datastore import DatastoreService

class DatastoreTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

    def tearDown(self):
        pass

    def test_putget(self):
        pass
