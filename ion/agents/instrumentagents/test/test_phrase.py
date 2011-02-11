#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_phrase.py
@brief This test file exercises the phrase and action structures that are
    to be used with instrument agents. This file does NOT address the use
    of structures by the agent. Those tests are in the instrument agent
    test cases.
@author Steve Foley
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.trial import unittest
from ion.test.iontest import IonTestCase
from ion.agents.instrumentagents.phrase import Phrase, SetAction, GetAction, ExecuteAction

class TestPhrase(IonTestCase):
    
    def setUp(self):
        self.ga = GetAction(Phrase.device, ["param1", "param2"])
        self.sa = SetAction(Phrase.observatory, {('chan1','param1'):'value1'})
        self.ea = ExecuteAction(Phrase.device, ["command", "arg1"])

    def tearDown(self):
        pass
    
    def test_action(self):
        """
        Simple checks to make sure actions are doing what they should...holding
        data in a specific way.
        """
        ga = GetAction(Phrase.device, ["param1", "param2"])
        self.assertTrue(isinstance(ga, GetAction))
        self.assertEqual(ga.struct, ["param1", "param2"])
        self.assertEqual(ga.destination, Phrase.device)
        
        sa = SetAction(Phrase.observatory, {('chan1','param1'):'value1'})
        self.assertTrue(isinstance(sa, SetAction))
        self.assertEqual(sa.struct, {('chan1','param1'):'value1'})
        self.assertEqual(sa.destination, Phrase.observatory)

        ea = ExecuteAction(Phrase.device, ["command", "arg1"])
        self.assertTrue(isinstance(ea, ExecuteAction))
        self.assertEqual(ea.struct, ["command", "arg1"])
        self.assertEqual(ea.destination, Phrase.device)
        
    def test_phrase_ops(self):
        """
        Test some simple operations with phrases and actions and their failure
        mechanisms.
        """
        phrase = Phrase()
        self.assertEquals(phrase.is_expired(), False)
        self.assertEquals(phrase.is_complete(), False)
        self.assertEquals(phrase.length(), 0)
        
        self.assertEquals(phrase.add(self.ga), True)
        self.assertEquals(phrase.add(self.sa), False)
        self.assertEquals(phrase.add(self.ga), True)
        self.assertEquals(phrase.length(), 2)
        self.assertEquals(phrase.contents(), [self.ga, self.ga])
        phrase.end()
        self.assertEquals(phrase.is_complete(), True)
            
    def test_phrase_timeout(self):
        """
        Verify phrase times out okay
        """
        raise unittest.SkipTest("Timeouts not implemented yet")
        
