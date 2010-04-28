#!/usr/bin/env python

"""
@file ion/test/iontest.py
@author Michael Meisinger
@brief test case for ION integration and system test cases (and some unit tests)
"""

import logging

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, DeferredQueue

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.core import ioninit
import ion.util.procutils as pu

class IonTestCase(unittest.TestCase):
    """
    Extension of python unittest.TestCase and trial unittest.TestCase for the
    purposes of supporting ION tests with a container/AMQP based execution
    environment
    """
    
    def setUp(self):
        pass