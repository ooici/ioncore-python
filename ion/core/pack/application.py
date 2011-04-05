#!/usr/bin/env python

"""
@file ion/core/app/app_loader.py
@author Michael Meisinger
@brief Management tools for Capability Container applications
@see OTP design principles: applications
"""

from twisted.internet import defer
from twisted.python.reflect import namedAny

from zope.interface import implements, Interface
from zope.interface import Attribute

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.exception import FatalError
from ion.core.cc.container_api import IContainer
from ion.core.exception import ConfigurationError, StartupError
from ion.util.config import Config

START_PERMANENT = "permanent"

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

        newapp = AppDefinition(**app_def)
        return newapp

    @classmethod
    @defer.inlineCallbacks
    def start_application(cls, container, appdef):
        assert IContainer.providedBy(container)
        assert isinstance(appdef, AppDefinition)

        modname = appdef.mod[0]
        modargs = appdef.mod[1]

        appmod = namedAny(modname)
        if not (hasattr(appmod, "start") and hasattr(appmod, "stop")):
            raise ConfigurationError("App module malformed")

        log.debug("Application '%s' starting" % appdef.name)
        try:
            res = yield defer.maybeDeferred(appmod.start,
                                container, START_PERMANENT, appdef, *modargs)
        except Exception, ex:
            log.exception("Application %s start failed" % appdef.name)
            appdef._state = None
            raise FatalError("Application %s start failed" % appdef.name)

        if res and type(res) in (list,tuple) and len(res) == 2:
            (appdef._supid, appdef._state) = res
        else:
            raise ConfigurationError("Application %s start() result invalid: %s" %(
                    appdef.name, res))

        appdef._mod_loaded = appmod
        log.info("Application '%s' started successfully. Root sup-id=%s" % (
                appdef.name, appdef._supid))

    @classmethod
    @defer.inlineCallbacks
    def stop_application(cls, container, appdef):
        assert IContainer.providedBy(container)
        assert isinstance(appdef, AppDefinition)

        log.debug("Application '%s' stopping" % appdef.name)
        try:
            yield defer.maybeDeferred(appdef._mod_loaded.stop,
                                      container, appdef._state)
        except Exception, ex:
            log.exception("Application %s stop failed" % appdef.name)

class AppDefinition(object):
    """
    Represents a CC application definition object
    """
    def __init__(self, **kwargs):
        if not 'name' in kwargs :
          raise ConfigurationError('Invalid app configuration: Name missing')

        self.__dict__.update(kwargs)

        if not hasattr(self, "description"):
            self.description = ""
        if not hasattr(self, "version"):
            self.version = ""
        if not hasattr(self, "mod"):
            self.mod = None
        if not hasattr(self, "modules"):
            self.modules = []
        if not hasattr(self, "registered"):
            self.registered = []
        if not hasattr(self, "applications"):
            self.applications = []

class IAppModule(Interface):
    """
    The module of an application
    """

    def start(container, starttype, *args, **kwargs):
        """
        @brief Starts the application
        @param container instance of the container
        @param starttype one of permanent, transient, temporary
        @param args, kwargs for additional supplied start arguments
        @retval Deferred, resolving to value of PID of top level supervisor
            and an optional state list []
        """

    def stop(container, state):
        """
        @brief Stops the application
        @param container instance of the container
        @param state the state as returned by the start function
        @retval Deferred
        """
