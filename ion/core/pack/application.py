#!/usr/bin/env python

"""
@file ion/core/app/app_loader.py
@author Michael Meisinger
@brief Management tools for Capability Container applications
@see OTP design principles: applications
"""

import os.path

from twisted.internet import defer
from twisted.python.reflect import namedAny

from zope.interface import implements, Interface
from zope.interface import Attribute

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import FatalError
from ion.core.cc.container_api import IContainer
from ion.core.exception import ConfigurationError, FatalError, StartupError
from ion.core.ioninit import ion_config
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
    def start_application(cls, container, appdef, app_manager=None, app_config=None, app_args=None):
        assert IContainer.providedBy(container)
        assert isinstance(appdef, AppDefinition)

        modname = appdef.mod[0]
        modargs = appdef.mod[1] if len(appdef.mod) >= 2 else []
        modkwargs = appdef.mod[2] if len(appdef.mod) >= 3 else {}
        if app_args and type(modkwargs) is dict and type(app_args) is dict:
            modkwargs.update(app_args)

        appmod = namedAny(modname)
        if not (hasattr(appmod, "start") and hasattr(appmod, "stop")):
            raise ConfigurationError("App module malformed")

        # @todo The backward reference to the app_manager is not nice at all

        # Load dependent apps
        #if appdef.applications and app_manager:
        #    if type(appdef.applications) in (list, tuple):
        #        for new_appname in appdef.applications:
        #            print app_manager.applications
        #            if app_manager.is_app_started(new_appname):
        #                continue
        #            log.debug("Loading dependent app %s" % new_appname)
        #            app_file_name = "%s/%s.app" % (CF_app_dir_path, new_appname)
        #            if not os.path.isfile(app_file_name):
        #                log.error("App dependency %s in file %s not found" % (
        #                    new_appname, app_file_name))
        #                continue
        #
        #            # Recursive call to startapp
        #            # @todo Detect cycles.
        #            yield app_manager.start_app(app_file_name)
        #    else:
        #        raise ConfigurationError("Application %s app config not a list: %s" %(
        #            appdef.name, type(appdef.applications)))

        # Overriding ion configuration with config entries
        if appdef.config:
            if type(appdef.config) is dict:
                log.debug("Applying app '%s' configuration" % appdef.name)
                ion_config.update(appdef.config)
            else:
                raise ConfigurationError("Application '%s' app config not a dict: %s" %(
                    appdef.name, type(appdef.config)))

        if app_config:
            if type(app_config) is dict:
                log.debug("Overriding app '%s' configuration" % appdef.name)
                ion_config.update(app_config)
            else:
                log.warn("Application '%s' app config not a dict: %s" %(
                    appdef.name, type(appdef.config)))

        log.debug("Application '%s' starting" % appdef.name)
        try:
            res = yield defer.maybeDeferred(appmod.start,
                                container, START_PERMANENT, appdef,
                                *modargs, **modkwargs)
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
        if not hasattr(self, "config"):
            self.config = {}
        if not hasattr(self, "args"):
            self.args = {}

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
