#!/usr/bin/env python

"""
@author Michael Meisinger
@brief Capability Container application manager
"""

import os.path

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ConfigurationError, StartupError
from ion.core.pack.application import AppLoader, AppDefinition
from ion.core.pack.release import ReleaseLoader
from ion.util.state_object import BasicLifecycleObject

from ion.util.path import adjust_dir


CONF = ioninit.config(__name__)
CF_app_dir_path = CONF['app_dir_path']
CF_rel_dir_path = CONF['rel_dir_path']

class AppManager(BasicLifecycleObject):
    """
    Manager class for capability container applications.
    """

    def __init__(self, container):
        BasicLifecycleObject.__init__(self)
        self.container = container

        # List of started applications (name -> AppDefinition)
        self.applications = []

    # Life cycle

    def on_initialize(self, config, *args, **kwargs):
        """
        """
        self.config = config
        return defer.succeed(None)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """

        # Bootstrap the container/ION core system
        if not ioninit.testing:
            filename = ioninit.adjust_dir(CONF['ioncore_app'])
            yield self.start_app(filename)

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """

        # Stop apps in reverse order of startup
        for app in reversed(self.applications):
            yield AppLoader.stop_application(self.container, app)

    def on_error(self, *args, **kwargs):
        #raise RuntimeError("Illegal state change for AppManager")
        self.container.error(*args, **kwargs)

    def is_app_started(self, appname):
        for app in self.applications:
            if app.name == appname:
                return True
        return False

    # API

    @defer.inlineCallbacks
    def start_rel(self, rel_filename):
        """
        @brief Start a Capability Container release from a .rel file.
        @see OTP design principles, releases
        @retval Deferred
        """
        log.info("Starting release: %s" % rel_filename)

        reldef = ReleaseLoader.load_rel_definition(rel_filename)

        if not type(reldef.apps) in (list,tuple):
            raise ConfigurationError("Release config apps entry malformed: %s" % reldef.apps)

        for app_def in reldef.apps:
            if not type(app_def) is dict:
                raise ConfigurationError("Release app config entry malformed: %s" % app_def)

            app_name = app_def.get('name', None)
            app_ver = app_def.get('version', None)
            app_config = app_def.get('config', None)
            app_args = app_def.get('args', None)
            process_app = app_def.get('processapp', None)
            mult = app_def.get('mult', False)

            if app_config and not type(app_config) is dict:
                raise ConfigurationError("Release app config entry malformed: %s" % app_def)

            if process_app and not type(process_app) in (list, tuple):
                raise ConfigurationError("Release app processapp entry malformed: %s" % app_def)

            yield self.start_app(None,
                                 app_name=app_name,
                                 app_version=app_ver,
                                 app_config=app_config,
                                 app_args=app_args,
                                 start_mult=mult,
                                 process_app=process_app)

    def start_app(self, app_filename, app_name=None, app_version=None,
                  app_config=None, app_args=None, start_mult=False, process_app=None):
        """
        @brief Start a Capability Container application from an .app file.
        @see OTP design principles, applications
        @retval Deferred
        """
        if process_app is not None:
            appdef = self.create_processapp_def(process_app, app_name, app_version,
                                                app_config, app_args)
        else:
            # Generate path to app file from app name
            if app_name is not None and app_filename is None:
                app_filename = "%s/%s.app" % (CF_app_dir_path, app_name)
                log.debug("Locating app '%s' in file: '%s'" % (app_name, app_filename))
            else:
                log.info("Starting app: '%s'" % app_filename)

            app_filename = adjust_dir(app_filename)

            if app_filename is None or not os.path.isfile(app_filename):
                raise StartupError("App file '%s' not found" % (
                        app_filename))

            appdef = AppLoader.load_app_definition(app_filename)

        if not start_mult and (self.is_app_started(appdef.name)):
            log.warn("Application '%s' already started" % appdef.name)
            return

        # Check app version if given

        self.applications.append(appdef)
        d = AppLoader.start_application(self.container, appdef, app_manager=self,
                                        app_config=app_config, app_args=app_args)
        return d

    def create_processapp_def(self, process_app,
                                    app_name=None, app_version=None,
                                    app_config=None, app_args=None):
        """
        @bried boilerplate for creating apps in release files without an app file.
        """
        app_dict = {
            "type":"application",
            "name":app_name,
            "description": "Automatic process app for %s" % (app_name),
            "version": app_version,
            "mod": ("ion.core.pack.processapp", process_app, app_args),
        }
        if app_config:
            app_dict.update(app_config)

        newapp = AppDefinition(**app_dict)

        return newapp
