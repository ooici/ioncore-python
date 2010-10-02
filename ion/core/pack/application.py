#!/usr/bin/env python

"""
@file ion/core/app/app_loader.py
@author Michael Meisinger
@brief Management tools for Capability Container applications
@see OTP design principles: applications
"""

from twisted.internet import defer

from zope.interface import implements, Interface
from zope.interface import Attribute

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.exception import ConfigurationError
from ion.util.config import Config

class AppLoader(object):
    """
    Loader for app files, CC application definitions.
    """

    @classmethod
    def load_app_definition(cls, filename):
        app_def = Config(filename).getObject()
        if not app_def or \
                not type(app_def) is dict or \
                not app_def.get('type',None) == 'application':
            raise ConfigurationError('Not an app configuration')

        newapp = Application(**app_def)
        return newapp

class Application(object):
    """
    Represents a CC application definition object
    """
    def __init__(self, **kwargs):
        if not 'name' in kwargs or \
              not 'version' in kwargs or \
              not 'modules' in kwargs or \
              not 'mod' in kwargs:
          raise ConfigurationError('Invalid app configuration')

        #self.__dict__.update(kwargs)
        self.name = kwargs['name']
        self.description = kwargs.get('description', "")
        self.name = kwargs['version']
        self.modules = kwargs.get('modules', [])
        self.registered = kwargs.get('registered', [])
        self.applications = kwargs.get('applications', [])
        self.mod = kwargs['mod']

class Release(object):
    pass

class IAppModule(Interface):
    """
    The module of an application
    """
