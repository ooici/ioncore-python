#!/usr/bin/env python

"""
@file ion/util/state_object.py
@author Michael Meisinger
@brief base classes for objects that are controlled by an underlying state machine
"""

from twisted.internet import defer
from twisted.python import failure
import traceback

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.util.fsm import FSM

class Actionable(object):
    """
    @brief Provides an object that supports the execution of actions as consequence
        of FSM transitions.
    """

    def _action(self, action, fsm):
        """
        @brief Execute action identified by argument
        @param action A str with the action to execute
        @param fsm the FSM instance that triggered the action.
        @retval Maybe a Deferred
        """
        raise NotImplementedError("Not implemented")

class StateObject(Actionable):
    """
    @brief Base class for an object instance that has an underlying FSM that
        determines which inputs are allowed at any given time; inputs trigger
        actions as defined by the FSM.
        This is the class that specialized classes inherit from.
        The underlying FSM can be set explicitly.
    """

    def __init__(self):
        # The FSM instance
        self.__fsm = None

    # Function prefix _so is for StateObject control functions

    def _so_set_fsm(self, fsm_inst):
        """
        @brief Set the "engine" FSM that drives the calling of the _action functions
        """
        assert not self.__fsm, "FSM already set"
        assert isinstance(fsm_inst, FSM), "Given object not a FSM"
        self.__fsm = fsm_inst

    def _so_process(self, event, *args, **kwargs):
        """
        @brief Trigger the FSM with an event. Leads to action functions being called.
            The complication is to make it deferred and non-deferred compliant.
            The downside is now a dependency on Twisted. Alternative: subclass
        @retval Maybe a Deferred, or result of FSM process
        @todo Improve the error catching, forwarding and reporting
        """
        assert self.__fsm, "FSM not set"
        self.__fsm.input_args = args
        self.__fsm.input_kwargs = kwargs
        self.__fsm.error_cause = None
        try:
            # This is the main invocation of the FSM. It will lead to calls to
            # the _action function in normal configuration.
            res = self.__fsm.process(event)

            if isinstance(res, defer.Deferred):
                d_post = defer.Deferred()
                def _cb(result):
                    # Process to NEXT state was successful (after Deferred)
                    #log.debug("FSM post-state" + str(self._get_state()))
                    d_post.callback(result)
                def _err(result):
                    # Process to NEXT state failed (after Deferred) -> forward to ERROR
                    log.error("ERROR in StateObject process(event=%s), D:\n%s" % (event, result))
                    try:
                        res1 = self._so_error(result)
                        if isinstance(res1, defer.Deferred):
                            # FSM error action deferred too
                            def _cb1(result1):
                                # Process to ERROR state was successful (after Deferred)
                                d_post.errback(result)
                            def _err1(result1):
                                # Process to ERROR state failed (after Deferred)
                                log.error("Subsequent ERROR in StateObject error(), D-D:\n%s" % (result1))
                                d_post.errback(result)
                            res1.addCallbacks(_cb1,_err1)
                        else:
                            # Process to ERROR state was successful (no Deferred)
                            d_post.errback(result)
                    except Exception, ex:
                        # Process to ERROR state failed (no Deferred)
                        log.exception("Subsequent ERROR in StateObject error(), D-ND")
                        d_post.errback(result)
                res.addCallbacks(_cb,_err)
                res = d_post
            else:
                # Process to NEXT state was successful (no Deferred) -- YAY!
                pass

        except StandardError, ex:
            # Process to NEXT state failed (no Deferred) -> forward to ERROR
            log.exception("ERROR in StateObject process(event=%s)" % (event))
            try:
                res1 = self._so_error(ex)
                if isinstance(res1, defer.Deferred):
                    result = failure.Failure(ex)
                    d_post = defer.Deferred()
                    # FSM error action deferred
                    def _cb1(result1):
                        # Process to ERROR state was successful (after Deferred)
                        d_post.errback(result)
                    def _err1(result1):
                        # Process to ERROR state failed (after Deferred)
                        log.error("Subsequent ERROR in StateObject error(), ND-D:\n%s" % (result1))
                        d_post.errback(result)
                    res1.addCallbacks(_cb1,_err1)
                    res = d_post
                else:
                    raise ex
            except Exception, ex1:
                log.exception("Subsequent ERROR in StateObject error(), ND-ND")
                raise ex

        return res

    def _so_error(self, *args, **kwargs):
        """
        @brief Brings the StateObject explicitly into the error state, because
            of some action error.
        """
        error = args[0] if args else None
        self.__fsm.error_cause = error

        # Is it OK to override the original args?
        self.__fsm.input_args = args
        self.__fsm.input_kwargs = kwargs

        return self.__fsm.process(BasicStates.E_ERROR)

    def _action(self, action, fsm):
        """
        Generic action function that invokes.
        """
        func = getattr(self, action)
        args = self.__fsm.input_args
        kwargs = self.__fsm.input_kwargs
        if action == BasicStates.E_ERROR:
            res = func(self.__fsm.error_cause, *args, **kwargs)
        else:
            res = func(*args, **kwargs)
        return res

    def _get_state(self):
        assert self.__fsm, "FSM not set"
        return self.__fsm.current_state

class FSMFactory(object):
    """
    A factory for FSMs to be used in StateObjects
    """

    def _create_action_func(self, target, action):
        """
        @retval a function with a closure with the action name
        """
        def action_target(fsm):
            return target(action, fsm)
        return action_target

    def create_fsm(self, target, memory=None):
        """
        @param a StateObject that is the
        @param memory a state vector. if None will be set to empty list
        @retval basic FSM with initial state 'INIT' and no transitions, and an
            empty list as state vector
        """
        assert isinstance(target, Actionable)
        memory = memory or []
        fsm = FSM('INIT', memory)
        return fsm

class BasicStates(object):
    """
    @brief Defines constants for basic state and lifecycle FSMs.
    """
    # States
    # Note: The INIT state is active before the initialize input is received
    S_INIT = "INIT"
    S_READY = "READY"
    S_ACTIVE = "ACTIVE"
    S_TERMINATED = "TERMINATED"
    S_ERROR = "ERROR"

    # Input events
    E_INITIALIZE = "initialize"
    E_ACTIVATE = "activate"
    E_DEACTIVATE = "deactivate"
    E_TERMINATE = "terminate"
    E_ERROR = "error"

    # Actions - in general called the same as the triggering event
    A_ACTIVE_TERMINATE = "terminate_active"

class BasicFSMFactory(FSMFactory):
    """
    A FSM factory for FSMs with basic state model.
    """

    def _create_action_func(self, target, action):
        """
        @retval a function with a closure with the action name
        """
        def action_target(fsm):
            return target("on_%s" % action, fsm)
        return action_target

    def create_fsm(self, target, memory=None):
        fsm = FSMFactory.create_fsm(self, target, memory)

        actf = target._action

        actionfct = self._create_action_func(actf, BasicStates.E_INITIALIZE)
        fsm.add_transition(BasicStates.E_INITIALIZE, BasicStates.S_INIT, actionfct, BasicStates.S_READY)

        actionfct = self._create_action_func(actf, BasicStates.E_ACTIVATE)
        fsm.add_transition(BasicStates.E_ACTIVATE, BasicStates.S_READY, actionfct, BasicStates.S_ACTIVE)

        actionfct = self._create_action_func(actf, BasicStates.E_DEACTIVATE)
        fsm.add_transition(BasicStates.E_DEACTIVATE, BasicStates.S_ACTIVE, actionfct, BasicStates.S_READY)

        actionfct = self._create_action_func(actf, BasicStates.E_TERMINATE)
        fsm.add_transition(BasicStates.E_TERMINATE, BasicStates.S_READY, actionfct, BasicStates.S_TERMINATED)

        actionfct = self._create_action_func(actf, BasicStates.A_ACTIVE_TERMINATE)
        fsm.add_transition(BasicStates.E_TERMINATE, BasicStates.S_ACTIVE, actionfct, BasicStates.S_TERMINATED)

        actionfct = self._create_action_func(actf, BasicStates.E_ERROR)
        fsm.set_default_transition(actionfct, BasicStates.S_ERROR)

        return fsm

class BasicLifecycleObject(StateObject):
    """
    A StateObject with a basic life cycle, as determined by the BasicFSMFactory.
    @see BasicFSMFactory
    @todo Add precondition checker
    """

    def __init__(self):
        StateObject.__init__(self)
        factory = BasicFSMFactory()
        fsm = factory.create_fsm(self)
        self._so_set_fsm(fsm)

    def initialize(self, *args, **kwargs):
        return self._so_process(BasicStates.E_INITIALIZE, *args, **kwargs)

    def activate(self, *args, **kwargs):
        return self._so_process(BasicStates.E_ACTIVATE, *args, **kwargs)

    def deactivate(self, *args, **kwargs):
        return self._so_process(BasicStates.E_DEACTIVATE, *args, **kwargs)

    def terminate(self, *args, **kwargs):
        return self._so_process(BasicStates.E_TERMINATE, *args, **kwargs)

    def error(self, *args, **kwargs):
        return self._so_process(BasicStates.E_ERROR, *args, **kwargs)

    def on_initialize(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_activate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_deactivate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_terminate_active(self, *args, **kwargs):
        """
        @brief this is a shorthand delegating to on_terminate from the ACTIVE
            state. Subclasses can override this action handler with more specific
            functionality
        """
        return self.on_terminate(*args, **kwargs)

    def on_terminate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_error(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")
