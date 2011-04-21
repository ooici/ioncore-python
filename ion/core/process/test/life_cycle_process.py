#!/usr/bin/env python

"""
@file ion/core/process/test/life_cycle_process.py
@author David Stuebe
@brief test case for process base class
"""

from twisted.trial import unittest
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.process.process import Process, ProcessFactory

from ion.util import state_object



class LifeCycleObject(state_object.BasicLifecycleObject):
    
    def on_initialize(self, *args, **kwargs):
        log.info('LifeCycleObject on_initialize')

    def on_activate(self, *args, **kwargs):
        log.info('LifeCycleObject on_activate')

    def on_deactivate(self, *args, **kwargs):
        log.info('LifeCycleObject on_deactivate')

    def on_terminate(self, *args, **kwargs):
        log.info('LifeCycleObject on_terminate')

    def on_error(self, *args, **kwargs):
        log.info('LifeCycleObject on_error')



class LCOProcess(Process):
    """
    This process is for testing only. Do not pass an object to the init of a process.
    """

    def __init__(self, lco, spawnargs=None):
        """
        Initialize a process with an additional Life Cycle object
        """
        Process.__init__(self, spawnargs)
        
        self.add_life_cycle_object(lco)
        
        
        #self.register_life_cycle_object(lco)

class LCOProcessAddingObjects(Process):
    def plc_init(self):
        self._obj_init = LifeCycleObject()

        self.register_life_cycle_object(self._obj_init)

    def plc_activate(self):
        self._obj_activate = LifeCycleObject()

        self.register_life_cycle_object(self._obj_activate)
        
        
# Spawn of the process using the module name
factory = ProcessFactory(LCOProcess)