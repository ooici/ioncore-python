#!/usr/bin/env python

"""
@author Dorian Raymer
@author Michael Meisinger
@brief Python Capability Container Twisted application plugin for twistd
"""

import os
import sys
import fcntl

from twisted.application import service
from twisted.internet import defer
from twisted.persisted import sob
from twisted.python import usage

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.cc import container
from ion.util.path import adjust_dir

class Options(usage.Options):
    """
    Extra arg for file of "program"/"module" to run.
    This class must be named Options with the usage.Options base class for the
    Twisted ServiceMaker to find it.
    """
    synopsis = "[ION Capability Container options]"

    longdesc = """This starts a capability container."""

    optParameters = [
                ["sysname", "s", None, "System name for group of capability containers" ],
                ["broker_host", "h", "localhost", "Message space broker hostname"],
                ["broker_port", "p", 5672, "Message space broker port"],
                ["broker_vhost", "v", "/", "Message space..."],
                ["broker_heartbeat", None, 0, "Heartbeat rate [seconds]"],
                ["broker_username", None, "guest", "Username to log into virtual host as"],
                ["broker_password", None, "guest", ""],
                ["broker_credfile", None, None, "File containing broker username and password"],
                ["boot_script", "b", None, "Boot script (python source)."],
                ["lockfile", None, None, "Lockfile used to denote container startup completion"],
                ["args", "a", None, "Additional startup arguments such as sysname=me" ],
                    ]
    optFlags = [
                ["no_shell", "n", "Do not start shell"],
                ["no_history", "i", "Do not read/write history file"],
                    ]

    def __init__(self):
        usage.Options.__init__(self)
        self['scripts'] = None

    def opt_version(self):
        from ion import version
        print "ION Capability Container version:", version.short()
        sys.exit(0)

    def parseArgs(self, *args):
        """
        Gets a list of apps/rels/scripts to run as additional arguments to the container.
        @see CapabilityContainer.start_scripts
        """
        self['scripts'] = args

# Keep a reference to the CC service instance
cc_instance = None

class CapabilityContainer(service.Service):
    """
    This Twisted service is the ION Python Capability Container runtime
    environment.
    """

    def __init__(self, config):
        """
        This service expects the config object to hold specific options.
        use phases to do things in order and wait for success/fail
        """
        self.config = config
        self.container = None
        ioninit.testing = False

        # calls back when the CC service starts up - anyone may attach to this callback and
        # use it for whatever is needed.
        self.defer_started = defer.Deferred()

        self.lockfile = None
        if 'lockfile' in self.config:
            self.lockfile = open(self.config['lockfile'], 'w')
            result = fcntl.fcntl(self.lockfile, fcntl.LOCK_EX, os.O_NDELAY)
            #assert(result == 0)

    @defer.inlineCallbacks
    def startService(self):
        """
        This is the main boot up point.

        - start container which connects to broker
        - start container agent which notifies message space of presence
        - start any designated progs
        - start shell if appropriate
        """
        service.Service.startService(self)

        log.info("ION Capability Container Boot...")
        yield self.start_container()
        log.info("Container started.")

        d = self.do_start_actions()
        d.addErrback(self.container.fatalError)
        yield d

        if not self.config['no_shell']:
            self.start_shell()

        log.info("All startup actions completed.")

        # signal successful container start
        if self.lockfile:
            fcntl.fcntl(self.lockfile, fcntl.LOCK_EX, os.O_NDELAY)
            self.lockfile.close()
            # The spawning process must cleanup the lockfile to avoid race conditions

        self.defer_started.callback(True)

    @defer.inlineCallbacks
    def stopService(self):
        yield self.container.terminate()
        service.Service.stopService(self)

        log.info("Container stopped.")

    @defer.inlineCallbacks
    def start_container(self):
        """
        When deferred done, fire next step
        @retval Deferred
        """
        log.info("Starting Container/broker connection...")
        self.container = container.create_new_container()
        yield self.container.initialize(self.config)
        yield self.container.activate()

    @defer.inlineCallbacks
    def do_start_actions(self):
        if self.config['boot_script']:
            yield self.run_boot_script()

        if self.config['scripts']:
            yield self.start_scripts()

    @defer.inlineCallbacks
    def start_scripts(self):
        """
        given the path to a file, open that file and exec the code.
        The file may be an .app, a .rel, or a python code script.
        """

        # Try two script locations, one for IDEs and another for shell.
        for script in self.config['scripts']:
            script = adjust_dir(script)
            if not os.path.isfile(script):
                log.error('Bad startup script path: %s' % script)
            else:
                if script.endswith('.app'):
                    yield self.container.start_app(app_filename=script)
                elif script.endswith('.rel'):
                    yield self.container.start_rel(rel_filename=script)
                else:
                    log.info("Executing script %s ..." % script)
                    execfile(script, {})

    def run_boot_script(self):
        """
        """
        variable = 'boot' # have to have it...
        file_name = os.path.abspath(self.config['boot_script'])
        if os.path.isfile(file_name):
            boot = sob.loadValueFromFile(file_name, variable)
            return boot()
        raise RuntimeError('Bad boot script path')

    def start_shell(self):
        """
        Start CC shell (a prog itself)
        """
        log.info("Starting Shell...")
        from ion.core.cc.shell import control
        control.start(self)

def makeService(config):
    """
    Twisted plugin service instantiation.
    Required by Twisted; IServiceMaker interface
    """
    global cc_instance
    cc_instance = CapabilityContainer(config)
    return cc_instance
