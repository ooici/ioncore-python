"""
@author Dorian Raymer
@author Michael Meisinger
@brief Python Capability Container Twisted plugin for twistd
"""

import os
import sys

from twisted.application import service
from twisted.internet import defer
from twisted.internet import reactor
from twisted.persisted import sob
from twisted.python import log
from twisted.python import usage

from ion.core.cc import spawnable
from ion.core.cc import container

class Options(usage.Options):
    """
    Extra arg for file of "program"/"module" to run
    """
    synopsis = "[ION Capability Container options]"

    longdesc = """This starts a capability container."""

    optParameters = [
                ["broker_host", "h", "localhost", "Message space broker hostname"],
                ["broker_port", "p", 5672, "Message space broker port"],
                ["broker_vhost", "v", "/", "Message space..."],
                ["broker_heartbeat", None, 0, "Heartbeat rate [seconds]"],
                ["boot_script", "b", None, "Boot script (python source)."],
                ["args", "a", None, "Additional startup arguments"],
                    ]
    optFlags = [
                ["no_shell", "n", "Do not start shell"],
                ]

    def opt_version(self):
        from ion.core.ioninit import version
        print "ION Capability Container version:", version.short()
        sys.exit(0)

    def parseArgs(self, script=None):
        """name of prog (module) file to run
        @todo better name; this is a first draft
        """
        self['script'] = script

class Phase(object):
    """State machine thing;
    Does something (e.g. start service) during boot up.
    Waits for thing to finish before proceeding
    """


class CapabilityContainer(service.Service):
    """This service is the capability container runtime environment.

    """

    def __init__(self, config):
        """this service expects the config object to hold specific options

        use phases to do things in order and wait for success/fail
        """
        self.config = config


    @defer.inlineCallbacks
    def startService(self):
        """This is the main boot up point.

        - start container which connects to broker
        - start container agent which notifies message space of presence
        - start any designated progs
        - start shell if appropriate

        """
        service.Service.startService(self)
        print "ION Capability Container Boot..."
        yield self.startContainer()
        print 'Container started.'
        if self.config['boot_script']:
            yield self.runBootScript()
        if self.config['script']:
            self.startScript()
        if not self.config['no_shell']:
            self.startShell()


    def stopService(self):
        service.Service.stopService(self)

    def startContainer(self):
        """when deferred done, fire next step
        """
        print 'Starting Container/broker connection...'
        d = container.startContainer(self.config)
        return d

    def startMonitor(self):
        """
        """

    def startScript(self):
        """
        given the path to a file, open that file and exec the code.
        Assume the file contains Python source code.
        """
        script = os.path.abspath(self.config['script'])
        if os.path.isfile(script):
            print "Executing script %s ..." % self.config['script']
            execfile(script, {})


    def startShell(self):
        """start shell (a prog itself)
        """
        print 'Starting Shell...'
        from ion.core.cc.shell import control
        control.start(self)

    def runBootScript(self):
        """
        """
        variable = 'boot' # have to have it...
        file_name = os.path.abspath(self.config['boot_script'])
        if os.path.isfile(file_name):
            boot = sob.loadValueFromFile(file_name, variable)
            return boot()
        raise RuntimeError('Bad boot script path')

def makeService(config):
    """
    process configuration
    initialize service
    """
    cc = CapabilityContainer(config)
    return cc


def Main(receiver):
    """Create a service out of a spawnable program.
    """

    return main
