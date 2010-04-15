#!/usr/bin/env python
import logging
from twisted.trial import unittest

class NoOpTests(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.WARN, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

    def tearDown(self):
        pass

    def test_one(self):
        self.failUnless(1 > 0)

    def test_two(self):
        self.failUnlessEqual(2, 2)
