#!/usr/bin/env python

from twisted.trial import unittest
import logging
logging = logging.getLogger(__name__)

from ion.data import resource

types=resource.types

class TimestampTest(unittest.TestCase):
    
    def setUp(self):
        
        self.now = resource.TimeStampResource()
        self.longago = resource.TimeStampResource(timestamp=10000.0)
        
    def test_print(self):
        logging.info('Print Time Stamp:'+str(self.longago))
        
    def test_equality(self):
        self.assertNot(self.now == self.longago)
        self.assert_(self.now == self.now)

    def test_encode_eq_decode(self):
        encoded= self.now.encode()
        
        decoded = resource.ResourceObject.decode(encoded,types)
        
        self.assert_(decoded == self.now)
        
        
class UniqueTest(unittest.TestCase):
    
    def setUp(self):
        
        self.a = resource.UniqueResource()
        self.b = resource.UniqueResource()
        
    def test_print(self):
        logging.info('Print Unique Resource:'+str(self.a))
        
    def test_equality(self):
        self.assertNot(self.a == self.b)
        self.assert_(self.a == self.a)

    def test_encode_eq_decode(self):
        encoded= self.a.encode()
        
        decoded = resource.ResourceObject.decode(encoded,types)
        
        self.assert_(decoded == self.a)
        
        
class IdentityTest(unittest.TestCase):
    
    def setUp(self):
        
        self.a = resource.IdentityResource(username='dstuebe', email='dstuebe@asascience.com', firstname='David', lastname='Stuebe')
        self.b = resource.IdentityResource(username='draymer', email='deldotdr@gmail.com', firstname='Dorian', lastname='Raymer')
        
    def test_print(self):
        logging.info('Print Identity Resource:'+str(self.a))
        
    def test_equality(self):
        self.assertNot(self.a == self.b)
        self.assert_(self.a == self.a)

    def test_encode_eq_decode(self):
        encoded= self.a.encode()
        
        decoded = resource.ResourceObject.decode(encoded,types)
        self.assert_(decoded == self.a)