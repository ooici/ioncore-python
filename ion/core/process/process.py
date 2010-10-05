#!/usr/bin/env python

"""
@file ion/core/process/process.py
@author Michael Meisinger
@brief base classes for processes within a capability container
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.id import Id
from ion.core.messaging.receiver import ProcessReceiver
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject

# @todo HACK: Dict of process "alias" to process declaration
processes = {}

class IProcess(Interface):
    """
    Interface for all capability container processes
    """

    def spawn():
        pass

class ProcessDesc(BasicLifecycleObject):
    """
    Class that encapsulates attributes about a spawnable process; can spawn
    and init processes.
    """
    def __init__(self, **kwargs):
        """
        Initializes ProcessDesc instance with process attributes.
        Also acts as a weak proxy object for use by the parent process.
        @param name  name label of process
        @param module  module name of process module
        @param class  or procclass is class name in process module (optional)
        @param node  ID of container to spawn process on (optional)
        @param spawnargs  dict of additional spawn arguments (optional)
        """
        BasicLifecycleObject.__init__(self)
        self.proc_name = kwargs.get('name', None)
        self.proc_module = kwargs.get('module', None)
        self.proc_class = kwargs.get('class', kwargs.get('procclass', None))
        self.proc_node = kwargs.get('node', None)
        self.spawn_args = kwargs.get('spawnargs', None)
        self.proc_id = None
        self.no_activate = False

    # Life cycle

    @defer.inlineCallbacks
    def spawn(self, parent=None, container=None, activate=True):
        """
        Boilerplate for initialize()
        @param parent the process instance that should be set as supervisor
        """
        #log.info('Spawning name=%s on node=%s' %
        #             (self.proc_name, self.proc_node))
        self.sup_process = parent
        self.container = container or ioninit.container_instance
        pid = yield self.initialize(activate)
        if activate:
            self.no_activate = activate
            yield self.activate()
        defer.returnValue(pid)

    @defer.inlineCallbacks
    def on_initialize(self, activate=False, *args, **kwargs):
        """
        Spawns this process description with the initialized attributes.
        @retval Deferred -> Id with process id
        """
        # Note: If this fails, an exception will occur and be passed through
        self.proc_id = yield self.container.spawn_process(
                procdesc=self,
                parent=self.sup_process,
                node=self.proc_node,
                activate=activate)

        log.info("Process %s ID: %s" % (self.proc_class, self.proc_id))

        defer.returnValue(self.proc_id)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        if self.no_activate:
            self.no_activate = True
        else:
            headers = yield self.container.activate_process(parent=self.sup_process, pid=self.proc_id)

    def shutdown(self):
        return self.terminate()

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        headers = yield self.container.terminate_process(parent=self.sup_process, pid=self.proc_id)

    def on_error(self, cause=None, *args, **kwargs):
        if cause:
            log.error("ProcessDesc error: %s" % cause)
            pass
        else:
            raise RuntimeError("Illegal state change for ProcessDesc")


class IProcessFactory(Interface):

    def build(spawnargs, container):
        """
        """

class ProcessFactory(object):
    """
    This protocol factory returns receiver instances used to spawn processes
    from a module. This implementation creates process class instances together
    with the receiver. This is a standard implementation that can be used
    in the code of every module containing a process. This factory also collects
    process declarations alongside.
    """
    implements(IProcessFactory)

    receiver = ProcessReceiver

    def __init__(self, pcls, name=None, args=None):
        self.process_class = pcls
        self.name = name or pcls.__name__
        self.args = args or {}

        if self.process_class and not IProcess.implementedBy(self.process_class):
            raise RuntimeError("Class does not implement IProcess")

        # Collecting the declare static class variable in a process class
        if pcls and hasattr(pcls, 'declare') and type(pcls.declare) is dict:
            procdec = pcls.declare.copy()
            procdec['class'] = pcls
            procname = pcls.declare.get('name', pcls.__name__)
            if procname in processes:
                raise RuntimeError('Process already declared: '+str(procname))
            processes[procname] = procdec

    def build(self, spawnargs=None, container=None):
        """
        Factory method to return a process instance from given arguments.
        """
        spawnargs = spawnargs or {}
        container = container or ioninit.container_instance

        #log.debug("ProcessFactory.build(name=%s, args=%s)" % (self.name,spawnargs))

        # Create a process receiver
        procname = spawnargs.get('proc-name', self.name)
        procid = spawnargs.get('proc-id', self.name)
        spawnargs['proc-group'] = self.name

        # Instantiate the IProcess class
        process = self.process_class(spawnargs=spawnargs)

        return process

class ProcessInstantiator(object):
    """
    Instantiator for IProcess instances. Relies on an IProcessFactory
    to instantiate IProcess instances from modules.
    """

    idcount = 0

    @classmethod
    def create_process_id(cls, container=None):
        container = container or ioninit.container_instance
        cls.idcount += 1
        if container:
            containerid = container.id
        else:
            # Purely for tests to avoid a _start_container() in setUp()
            containerid = "TEST-CONTAINER-ID"
        return Id(cls.idcount, containerid)

    @classmethod
    @defer.inlineCallbacks
    def spawn_from_module(cls, module, space=None, spawnargs=None, container=None, activate=True):
        """
        @brief Factory method to spawn a BaseProcess instance from a Python module.
                By default, spawn includes an activate
        @param module A module (<type 'module'>) with a ProcessFactory factory
        @param space MessageSpace instance
        @param spawnargs argument dict given to the factory on spawn
        @retval Deferred which fires with the IProcess instance
        """
        spawnargs = spawnargs or {}
        container = container or ioninit.container_instance

        if not hasattr(module, 'factory'):
            raise RuntimeError("Must define factory in process module to spawn")

        if not IProcessFactory.providedBy(module.factory):
            raise RuntimeError("Process model factory must provide IProcessFactory")

        procid = ProcessInstantiator.create_process_id(container)
        spawnargs['proc-id'] = procid.full

        process = yield defer.maybeDeferred(module.factory.build, spawnargs)
        if not IProcess.providedBy(process):
            raise RuntimeError("ProcessFactory returned non-IProcess instance")

        # Give a callback to the process to initialize and activate (if desired)
        try:
            yield process.initialize()
            if activate:
                yield process.activate()
        except Exception, ex:
            log.exception("Error spawning process from module")
            raise ex

        defer.returnValue(process)
