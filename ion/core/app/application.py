#!/usr/bin/env python

"""
@file ion/core/app/app_loader.py
@author Michael Meisinger
@brief Interface for an application
"""

from zope.interface import implements, Interface
from zope.interface import Attribute

class IAppModule(Interface):
    """
    The module of an application 
    """
