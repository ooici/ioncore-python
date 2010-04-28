#!/usr/bin/env python

"""
@file ion/data/dataobject.py
@author Michael Meisinger
@brief module for ION structured data object definitions
"""

import logging
import random
from twisted.internet import defer

class DataObject(object):
    """Base class for all structured data objects in the ION system. Data
    objects can be persisted, transported, versioned, composed.
    """
    
    # *static* Keeps sequence counter for object instances
    isSeqCnt = 0
    
    def __init__(self):
        #global isSeqCnt
        self.identity = DataObject.createUniqueId()
    
    @classmethod
    def createUniqueId(cls):
        cls.isSeqCnt += 1
        return str(cls.isSeqCnt) + str(random.randint(10**8, 10**9-1))
        