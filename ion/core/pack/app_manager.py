#!/usr/bin/env python

"""
@author Michael Meisinger
@brief Capability Container application manager
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ConfigurationError, StartupError
from ion.core.pack.application import AppLoader
from ion.util.state_object import BasicLifecycleObject

CONF = ioninit.config(__name__)

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
        raise RuntimeError("Illegal state change for AppManager")

    # API

    def start_app(self, app_filename):
        """
        Start a Capability Container application from an .app file.
        @see OTP design principles, applications
        @retval Deferred
        """
        log.info("Starting app: %s" % app_filename)

        appdef = AppLoader.load_app_definition(app_filename)
        for app in self.applications:
            if app.name == appdef.name:
                raise StartupError('Application %s already started' % appdef.name)

        self.applications.append(appdef)
        d = AppLoader.start_application(self.container, appdef)
        return d
