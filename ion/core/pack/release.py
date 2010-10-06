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

class RelLoader(object):
    """
    Loader for rel files, CC release definitions.
    """

    @classmethod
    def load_rel_definition(cls, filename):
        rel_def = Config(filename).getObject()
        if not rel_def or \
                not type(rel_def) is dict or \
                not rel_def.get('type',None) == 'release':
            raise ConfigurationError('Not an rel configuration')

        newrel = RelDefinition(name, **rel_def)
        return newrel

class RelDefinition(object):
    """
    Represents a CC release definition object
    """
    def __init__(self, **kwargs):
        if not 'name' in kwargs :
          raise ConfigurationError('Invalid rel configuration: Name missing')

        self.__dict__.update(kwargs)


