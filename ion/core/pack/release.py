#!/usr/bin/env python

"""
@file ion/core/app/app_loader.py
@author Michael Meisinger
@brief Management tools for Capability Container releases
@see OTP design principles: releases
"""

from twisted.internet import defer

from zope.interface import implements, Interface
from zope.interface import Attribute

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.exception import ConfigurationError
from ion.util.config import Config

class ReleaseLoader(object):
    """
    Loader for rel files, CC release definitions.
    """

    @classmethod
    def load_rel_definition(cls, filename):
        rel_def = Config(filename).getObject()
        if not rel_def or \
                not type(rel_def) is dict or \
                not rel_def.get('type',None) == 'release':
            raise ConfigurationError('Not a release configuration')

        newrel = ReleaseDefinition(**rel_def)
        return newrel

class ReleaseDefinition(object):
    """
    Represents a CC release definition object
    """
    def __init__(self, **kwargs):
        if not 'name' in kwargs :
          raise ConfigurationError('Invalid rel configuration: Name missing')

        self.__dict__.update(kwargs)

        if not hasattr(self, "version"):
            self.version = ""
        if not hasattr(self, "description"):
            self.description = ""
        if not hasattr(self, "ioncore"):
            self.ioncore = ""
        if not hasattr(self, "apps"):
            self.apps = []
