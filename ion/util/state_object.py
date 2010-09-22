#!/usr/bin/env python

"""
@file ion/util/state_object.py
@author Michael Meisinger
@brief base class for objects that are controlled by an underlying state machine
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.util.fsm import FSM

class Actionable(object):

    def _action(self, action, fsm):
        raise NotImplementedError("Not implemented")

class StateObject(Actionable):
    """
    This is the class that specialized classes inherit from.
    """

    def __init__(self):
        self.__fsm = None

    def _so_set_fsm(self, fsm_inst):
        """
        Set the "engine" FSM that drives the calling of the _action functions
        """
        assert not self.__fsm, "FSM already set"
        assert isinstance(fsm_inst, FSM), "Given object not a FSM"
        self.__fsm = fsm_inst

    def _so_process(self, event, *args, **kwargs):
        """
        Trigger the FSM with an event. Leads to action functions being called.
        """
        assert  self.__fsm, "FSM not set"
        self.__fsm.input_args = args
        self.__fsm.input_kwargs = kwargs
        res = self.__fsm.process(event)
        return res

    def _action(self, action, fsm):
        fname = "on_%s" % action
        func = getattr(self, fname)
        args = self.__fsm.input_args
        kwargs = self.__fsm.input_kwargs
        res = func(*args, **kwargs)
        return res

class FSMFactory(object):
    """
    A factory for FSMs to be used in StateObjects
    """

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

class BasicFSMFactory(FSMFactory):
    """
    A FSM factory for FSMs with basic state model.
    """

    S_INIT = "INIT"
    S_READY = "READY"
    S_ACTIVE = "ACTIVE"
    S_TERMINATED = "TERMINATED"
    S_ERROR = "ERROR"

    E_INITIALIZE = "initialize"
    E_ACTIVATE = "activate"
    E_DEACTIVATE = "deactivate"
    E_TERMINATE = "terminate"
    E_ERROR = "error"

    def _create_action_func(self, target, action):
        """
        @retval a function with a closure with the action name
        """
        def action_target(fsm):
            return target(action, fsm)
        return action_target

    def create_fsm(self, target, memory=None):
        fsm = FSMFactory.create_fsm(self, target, memory)

        actf = target._action

        actionfct = self._create_action_func(actf, self.E_INITIALIZE)
        fsm.add_transition(self.E_INITIALIZE, self.S_INIT, actionfct, self.S_READY)

        actionfct = self._create_action_func(actf, self.E_ACTIVATE)
        fsm.add_transition(self.E_ACTIVATE, self.S_READY, actionfct, self.S_ACTIVE)

        actionfct = self._create_action_func(actf, self.E_DEACTIVATE)
        fsm.add_transition(self.E_DEACTIVATE, self.S_ACTIVE, actionfct, self.S_READY)

        actionfct = self._create_action_func(actf, self.E_TERMINATE)
        fsm.add_transition(self.E_TERMINATE, self.S_READY, actionfct, self.S_TERMINATED)
        fsm.add_transition(self.E_TERMINATE, self.S_ACTIVE, actionfct, self.S_TERMINATED)

        actionfct = self._create_action_func(actf, self.E_ERROR)
        fsm.set_default_transition (actionfct, self.S_ERROR)

        return fsm

class BasicLifecycleObject(StateObject):
    """
    A StateObject with a basic life cycle, as determined by the BasicFSMFactory.
    @see BasicFSMFactory
    """

    def __init__(self):
        StateObject.__init__(self)
        factory = BasicFSMFactory()
        fsm = factory.create_fsm(self)
        self._so_set_fsm(fsm)

    def initialize(self, *args, **kwargs):
        self._so_process(BasicFSMFactory.E_INITIALIZE, *args, **kwargs)

    def activate(self, *args, **kwargs):
        self._so_process(BasicFSMFactory.E_ACTIVATE, *args, **kwargs)

    def deactivate(self, *args, **kwargs):
        self._so_process(BasicFSMFactory.E_DEACTIVATE, *args, **kwargs)

    def terminate(self, *args, **kwargs):
        self._so_process(BasicFSMFactory.E_TERMINATE, *args, **kwargs)

    def on_initialize(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_activate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_deactivate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_terminate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_error(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")
