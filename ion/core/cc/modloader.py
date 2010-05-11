#!/usr/bin/env python

"""
@file ion/core/cc/modloader.py
@author Michael Meisinger
@brief loads modules in given list of 
"""

import logging
import os
import os.path

from ion.core import ioninit
from ion.util.config import Config
import ion.util.procutils as pu

CONF = ioninit.config(__name__)
CF_load_modules = CONF['load_modules']
CF_modules_cfg = Config(CONF.getValue('modules_cfg')).getObject()


class ModuleLoader(object):
    """
    Loads all modules in given list of modules and packages
    """

    def load_modules(self, mods=None):
        """
        Loads modules, such that static code gets executed
        @todo Should this be twisted friendly generator?
        """
        if not mods:
            mods = CF_modules_cfg
        elif not CF_load_modules:
            # This should only apply if called with default arguments
            return
        
        for mod in mods:
            if not type(mod) is str:
                raise RuntimeError("Entries in module list must be str")
            elif mod.endswith('.**'):
                self._load_package(mod[:len(mod)-3], True)
            elif mod.endswith('.*'):
                self._load_package(mod[:len(mod)-2], False)
            else:
                self._load_module(mod)

    def _load_module(self, mod):
        #logging.info('Loading Module {0}'.format(mod))
        modo = pu.get_module(mod)
        
    def _load_package(self, pack, recurse=False):
        #logging.info('Loading Package {0}'.format(pack))
        packo = pu.get_module(pack)
        ppath = packo.__path__
        for path1 in ppath:
            dirList=os.listdir(path1)
            for fname in dirList:
                if fname.endswith('.py') and fname != '__init__.py':
                    self._load_module(pack+'.'+fname[:len(fname)-3])
                elif os.path.isdir(os.path.join(path1,fname)) and recurse:
                    self._load_package(pack+'.'+fname)
