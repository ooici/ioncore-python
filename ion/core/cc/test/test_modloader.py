#!/usr/bin/env python

"""
@file ion/core/cc/test/test_modloader.py
@author Michael Meisinger
@brief test cases for module loader
"""

import logging

from ion.core import ioninit
from ion.core.cc.modloader import ModuleLoader
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class ModuleLoaderTest(IonTestCase):
    """
    Tests the module loader.
    """
       
    def test_modloader(self):
        ml = ModuleLoader()

        # Single module syntax
        ml.load_modules(['ion.core.cc.test.test_modloader'])

        # Single package syntax
        ml.load_modules(['ion.core.cc.test.*'])

        # Tree of packages syntax
        ml.load_modules(['ion.core.cc.**'])

        # Load configured modules
        ml.load_modules()
