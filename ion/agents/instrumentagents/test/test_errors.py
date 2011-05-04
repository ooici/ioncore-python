#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/test/test_errors.py
@brief Test cases for instrument and driver error checking.
@author Edward Hunter
"""


from ion.test.iontest import IonTestCase
from ion.agents.instrumentagents.instrument_constants import InstErrorCode




class TestInstrumentErrors(IonTestCase):
    """
    Test that the InstErrorCode class correctly recognized OK and error values
    in various formats as they may be unintentionally modified by the framework.
    """
    
    # Example OK value formats to test.
    ok_vals = [
        'OK',
        ['OK'],
        ['OK',],
        ('OK',)
        
        
    ]

    # Example error value formats to test.
    error_vals = [
        ['ERROR_EXE_DEVICE','Could not execute device command.'],
        ('ERROR_EXE_DEVICE','Could not execute device command.'),        
        ['ERROR_EXE_DEVICE','Could not execute device command.',],
        ('ERROR_EXE_DEVICE','Could not execute device command.',)        
    ]
    
    
    
    def setUp(self):
        """
        """
        pass
                





    def tearDown(self):
        """
        """
        pass
        




    def test_errors(self):
        """
        """
        
        
        for item in self.ok_vals:
            self.assert_(InstErrorCode.is_ok(item))
            self.assert_(InstErrorCode.is_equal(item,InstErrorCode.OK))
            
        for item in self.error_vals:            #print item
            self.assert_(InstErrorCode.is_error(item))
            self.assert_(InstErrorCode.is_equal(item,InstErrorCode.EXE_DEVICE_ERR))
        

        for item in InstErrorCode.list():
            self.assert_(InstErrorCode.is_ok(item) or \
                         InstErrorCode.is_error(item))
            tupitem = tuple(item)
            self.assert_(InstErrorCode.is_ok(item) or \
                         InstErrorCode.is_error(item))





