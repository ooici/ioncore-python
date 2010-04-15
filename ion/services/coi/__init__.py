"""
@file __init__.py
@author Dorian Raymer
@brief Docs for Magnet

@mainpage
Welcome to Magnet, the way to use AMQP from OOI code. 

"""
# This library has a minimum required python version
import os
import sys
if not hasattr(sys, "version_info") or sys.version_info < (2,5):
    raise RuntimeError("Magnet requires Python 2.5 or later.")
del sys

from magnet._version import version
__version__ = version.short()


import magnet
# Spec file is loaded from the egg bundle. Hard code 0-8 for now...
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')
