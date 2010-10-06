#!/usr/bin/env python

"""
@file ion/core/process/proc_manager.py
@author Michael Meisinger
@brief Process Manager for capability container
"""

import types

from twisted.internet import defer
from twisted.python.reflect import namedAny

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ConfigurationError
from ion.core.intercept.interceptor import Interceptor
from ion.core.process import process
from ion.core.process.cprocess import ContainerProcess, IContainerProcess, Invocation
from ion.util.state_object import BasicLifecycleObject
import ion.util.procutils as pu

class InterceptorSystem(Interceptor):
    """
    Container interceptor system class.
    """

    def __init__(self):
        BasicLifecycleObject.__init__(self)

        self.interceptors = {}
        self.paths = {}

    # Life cycle

    @defer.inlineCallbacks
    def on_initialize(self, config, *args, **kwargs):
        """
        """
        #log.debug("Initializing InterceptorSystem from config: %s" % config)
        yield self._init_system(config)
        log.info("InterceptorSystem initialized: %s interceptors %s, %s paths loaded: %s" % (
            len(self.interceptors), self.interceptors.keys(), len(self.paths), self.paths.keys()))

    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        return defer.succeed(None)

    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        return defer.succeed(None)

    def on_error(self, cause= None, *args, **kwargs):
        if cause:
            log.error("Process error: %s" % cause)
            pass
        else:
            raise RuntimeError("Illegal InterceptorSystem state change")
            # TODO: A path is not a list, but like a FSM
            # have priorities and alternative routes

    # API
    @defer.inlineCallbacks
    def process(self, invocation):
        """
        @param invocation container object for parameters
        @retval invocation instance, may be modified
        """
        pathname = invocation.path
        path = self.paths.get(pathname, None)
        if not path:
            raise RuntimeError("Path %s unknown" % invocation.path)
        for path_element in path:
            invocation.path = pathname
            intc = path_element['interceptor_instance']
            #log.debug("Process path %s step %s" % (invocation.path, path_element['name']))
            try:
                invocation = yield defer.maybeDeferred(intc.process, invocation)
            except Exception, ex:
                log.exception("Error in interceptor path %s step %s" % (
                    invocation.path, path_element['name']))
                invocation.error(str(ex))
                raise ex
        defer.returnValue(invocation)

    # Helpers

    @defer.inlineCallbacks
    def _init_system(self, config):
        if not config or not type(config) is dict:
            raise ConfigurationError("InterceptorSystem config invalid %r" % config)
        if not 'interceptors' in config:
            raise ConfigurationError("InterceptorSystem config must have 'interceptors'")
        interceptors = config['interceptors']
        for intname in interceptors:
            int_config = interceptors[intname]
            intcls = yield self._create_interceptor(intname, int_config)
            if intname in self.interceptors:
                raise ConfigurationError("InterceptorSystem interceptor %s already configured" % intname)
            self.interceptors[intname] = intcls

        if 'stack' in config:
            istack = config['stack']
            out_path = self._create_intercept_path(istack)
            in_path = self._reversed_intercept_path(out_path)
            self.paths[Invocation.PATH_OUT] = out_path
            self.paths[Invocation.PATH_IN] = in_path

        if 'paths' in config:
            raise NotImplementedError("Not implemented")

    @defer.inlineCallbacks
    def _create_interceptor(self, name, config):
        #log.debug("Create Interceptor '%s' from config: %s" % (name, config))
        intcls_name = config['classname']
        intcls = namedAny(intcls_name)
        if not IContainerProcess.implementedBy(intcls):
            raise ConfigurationError("Interceptor '%s' class %s not an interceptor" % (name, intcls_name))
        intc = intcls(name)
        intcargs = config.get('args', {})
        if not type(intcargs) is dict:
            raise ConfigurationError("Interceptor '%s' args must be dict %r" % (name, intcargs))
        yield intc.initialize(**intcargs)
        yield intc.activate()
        #log.debug("Interceptor '%s' created and activated: %r" % (name, intc))
        defer.returnValue(intc)

    def _create_intercept_path(self, config):
        if not type(config) in (list, tuple):
            raise ConfigurationError("InterceptorSystem stack must be list %r" % (config))
        path = []
        for path_elem in config:
            if not type(path_elem) is dict:
                raise ConfigurationError("InterceptorSystem stack element must be dict %r" % (path_elem))
            if not 'interceptor' in path_elem:
                raise ConfigurationError("InterceptorSystem stack element must have interceptor %r" % (path_elem))
            pe_intc = path_elem['interceptor']
            intc = self.interceptors.get(pe_intc, None)
            if not intc:
                raise ConfigurationError("InterceptorSystem stack element interceptor %s not found" % (pe_intc))

            path_elem['interceptor_instance'] = intc
            path_elem['name'] = path_elem.get('name', None) or intc.name
            path.append(path_elem)
        return path

    def _reversed_intercept_path(self, int_path):
        assert type(int_path) is list
        return list(reversed(int_path))
