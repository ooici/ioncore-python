#!/usr/bin/env python

from twisted.trial import unittest
import logging
logging = logging.getLogger(__name__)

from ion.data import resource
from twisted.python import reflect


class TestDeviceResource(unittest.TestCase):
    
    def setUp(self):
        
        res = resource.DeviceResource()
        res.mfg = 'seabird'
        res.serial = 10
        res.voltage = 3.14159
        self.res = res
        self.res_type = reflect.fullyQualifiedName(resource.DeviceResource)
        self.encoded=[('voltage', 'float\x003.14159'), ('serial', 'int\x0010'), ('mfg', 'str\x00seabird')]
     
    def testPrintResource(self):
                
        logging.info(self.res)
        
    def testEncode(self):
        """
        """
        enc = self.res.encode()
        self.assertEqual(self.encoded,enc)
        
    def testDecode(self):
        
        dec = resource.BaseResource.decode(self.res_type,self.encoded)()
        
        print self.res
        print dec
        print 'SAME ++++++',dec == self.res
        
        self.assertEqual(self.res,dec)