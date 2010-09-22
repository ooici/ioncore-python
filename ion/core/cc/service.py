#!/usr/bin/env python

"""
@author Dorian Raymer
@author Michael Meisinger
@brief Python Capability Container Twisted plugin for twistd
"""

import os
import sys

from twisted.application import service
from twisted.internet import defer
from twisted.persisted import sob
from twisted.python import usage

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

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
                ["args", "a", None, "Additional startup arguments such as sysname=me" ],
                    ]
    optFlags = [
                ["no_shell", "n", "Do not start shell"],
                ]

    def opt_version(self):
        from ion.core.ionconst import VERSION
        log.info("ION Capability Container version: "+ VERSION)
        sys.exit(0)

    def parseArgs(self, script=None):
        """
        name of prog (module) file to run
        @todo better name; this is a first draft
        """
        self['script'] = script

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

        yield self.do_start_actions()

        if not self.config['no_shell']:
            self.start_shell()

    def stopService(self):
        service.Service.stopService(self)

    def startMonitor(self):
        """
        @todo What is this for? Twisted?
        """

    def start_container(self):
        """
        When deferred done, fire next step
        @retval Deferred
        """
        log.info("Starting Container/broker connection...")
        cont = container.create_new_container()
        cont.initialize(self.config)
        d = cont.activate()
        return d

    @defer.inlineCallbacks
    def do_start_actions(self):
        if self.config['boot_script']:
            yield self.run_boot_script()

        if self.config['script']:
            self.start_script()

        # @todo At this point, can signal successful container start

    def start_script(self):
        """
        given the path to a file, open that file and exec the code.
        Assume the file contains Python source code.
        """
        script = os.path.abspath(self.config['script'])
        if os.path.isfile(script):
            log.info("Executing script %s ..." % self.config['script'])
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
    Required by Twisted
    """
    global cc_instance
    cc_instance = CapabilityContainer(config)
    return cc_instance


#def Main(receiver):
#    """Create a service out of a spawnable program.
#    """
#    return main
