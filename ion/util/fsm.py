#!/usr/bin/env python

"""
This module implements a Finite State Machine (FSM). In addition to state
this FSM also maintains a user defined "memory". So this FSM can be used as a
Push-down Automata (PDA) since a PDA is a FSM + memory.

Documentation shortened. For full documentation, see original package.

Noah Spurrier 20020822
Extended by Michael Meisinger 2011
http://www.noah.org/python/FSM/
"""

from twisted.internet import defer

class ExceptionFSM(Exception):
    """This is the FSM Exception class."""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

class FSM(object):
    """This is a Finite State Machine (FSM).
    """

    def __init__(self, initial_state, memory=None, post_action=False):
        """
        This creates the FSM. You set the initial state here.
        The "memory" attribute is any object.
        """

        # Map (input_symbol, current_state) --> (action, next_state).
        self.state_transitions = {}
        # Map (current_state) --> (action, next_state).
        self.state_transitions_any = {}
        self.default_transition = None

        self.input_symbol = None
        self.initial_state = initial_state
        self.current_state = self.initial_state
        self.next_state = None
        self.action = None
        self.memory = memory
        # If True, the action will be executed after the state change
        self.post_action = post_action

    def reset(self):
        """
        This sets the current_state to the initial_state and sets
        input_symbol to None.
        """

        self.current_state = self.initial_state
        self.input_symbol = None

    def add_transition(self, input_symbol, state, action=None, next_state=None):
        """
        This adds a transition
        """

        if next_state is None:
            next_state = state
        self.state_transitions[(input_symbol, state)] = (action, next_state)

    def add_transition_list(self, list_input_symbols, state, action=None, next_state=None):
        """
        This adds the same transition for a list of input symbols.
        """

        if next_state is None:
            next_state = state
        for input_symbol in list_input_symbols:
            self.add_transition (input_symbol, state, action, next_state)

    def add_transition_any(self, state, action=None, next_state=None):
        """
        This adds a transition
        """

        if next_state is None:
            next_state = state
        self.state_transitions_any [state] = (action, next_state)

    def set_default_transition(self, action, next_state):
        """
        This sets the default transition.
        """

        self.default_transition = (action, next_state)

    def get_transition(self, input_symbol, state):
        """
        This returns (action, next state) given an input_symbol and state.
        """

        if self.state_transitions.has_key((input_symbol, state)):
            return self.state_transitions[(input_symbol, state)]
        elif self.state_transitions_any.has_key (state):
            return self.state_transitions_any[state]
        elif self.default_transition is not None:
            return self.default_transition
        else:
            raise ExceptionFSM('Transition is undefined: (%s, %s).' %
                (str(input_symbol), str(state)) )

    def process(self, input_symbol):
        """
        This is the main method that you call to process input.
        """

        self.input_symbol = input_symbol
        (self.action, self.next_state) = self.get_transition(self.input_symbol, self.current_state)

        res = None
        if self.post_action:
            self.current_state = self.next_state
            self.next_state = None

        if self.action is not None:
            res = self.action(self)

        if not self.post_action:
            if isinstance(res, defer.Deferred):
                def _cb(result):
                    self.current_state = self.next_state
                    self.next_state = None
                    return result
                def _err(result):
                    return result
                res.addCallbacks(_cb,_err)
            else:
                self.current_state = self.next_state
                self.next_state = None

        return res

    def process_list(self, input_symbols):
        """
        This takes a list and sends each element to process().
        """
        res = []
        for s in input_symbols:
            pres = self.process(s)
            res.append(pres)
        return res
