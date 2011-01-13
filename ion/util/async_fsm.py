#!/usr/bin/env/python

"""
@file ion/util/async_fsm.py
@author Dave Foster <dfoster@asascience.com>
@brief Asynchronous capable FSM
"""

from ion.util.fsm import FSM
from ion.util.task_chain import TaskChain
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class AsyncFSM(FSM):
    """
    A FSM derivative that performs its actions associated with state transitions asynchronously.

    The AsyncFSM does not change its state until the action calls back succesfully.
    The action need not return a deferred, it is wrapped in defer.maybeDeferred.

    An AsyncFSM offers additional improvements over a regular FSM:
    - Transition actions may or may not return deferreds (they are wrapped in defer.maybeDeferred)
    - Offers a way to perform many transitions at once without knowing all the input symbols 
      required to get to that state (see run_to_state). It computes the list of symbols
      via get_path.
    - Notice of state transitions: you may add callables (via add_state_change_notice) that
      get called on every successful state transition. The callable takes a single param,
      a reference to this AsyncFSM. This can be used to update interested parties when a
      multi stage transition via run_to_state is being run.

    """

    def __init__(self, initial_state, memory=None):
        FSM.__init__(self, initial_state, memory)

        self._state_change_notices = []

    def add_state_change_notice(self, notice):
        """
        Add a callable to be called back when the state of this AsyncFSM changes.

        @param notice   A callable which will be called with a reference to the AsyncFSM as its only param.
        """
        self._state_change_notices.append(notice)

    def del_state_change_notice(self, notice):
        try:
            self._state_change_notices.remove(notice)
        except ValueError:
            pass

    def process(self, input_symbol):
        """
        Takes an input and performs an action based on its current state and the input.

        Finds a transition from the current state taking the input and calls the action
        required. This action may or may not return a deferred. When the action completes,
        the state changes.
        """
        self.input_symbol = input_symbol
        (self.action, self.next_state) = self.get_transition(self.input_symbol, self.current_state)
        def_action = None
        if self.action is not None:
            def_action = defer.maybeDeferred(self.action) #, *[self])
            def_action.addCallbacks(self._action_cb, self._action_eb)
        else:
            self.current_state = self.next_state
            self.next_state = None
            def_action = defer.Deferred()
            def_action.callback()

        return def_action

    def process_list(self, input_symbols):
        """This takes a list and sends each element to process(). The list may
be a string or any iterable object. """
        chain = TaskChain()
        for sym in input_symbols:
            chain.append((self.process, [sym]))
        return chain.run()

    def run_to_state(self, goal_state):
        """
        Attempts to run from the current state to the goal state.

        Uses get_path then process_list. Returns the deferred from process_list.
        """
        return self.process_list(self.get_path(goal_state))

    def get_path(self, goal_state):
        """
        Gets a path of input symbols from the current state to the goal state.
        """
        remstates = [(self.current_state, [])]
        seenstates = []
        while len(remstates) > 0:
            (curstate, curinps) = remstates.pop()
            seenstates.append(curstate)

            # if this is the goal state, return the input list
            if curstate == goal_state:
                return curinps

            # get a list of all transitions in the current state
            matchingstates = [tup for tup in self.state_transitions.keys() if tup[1]==curstate]

            # now we have a filtered list of (inp, state) matching our cur state
            # see where they go, append them to remstates if not in seenstates
            for k in matchingstates:
                (act, newstate) = self.state_transitions[k]
                if not newstate in seenstates:
                    remstates.append((newstate, curinps + [k[0]]))

        # indicates no path to goal state
        return None

    def _action_cb(self, res):
        """
        Action successful callback.

        Calls notice callbacks and sets this AsyncFSM's current state.
        """
        self.current_state = self.next_state
        self.next_state = None

        # perform notices to callbacks
        # use maybe deferred becuase they may or may not be async, and we don't
        # really care about waiting for them
        for notice in self._state_change_notices:
            defer.maybeDeferred(notice, self)

    def _action_eb(self, failure):
        """
        Action unsuccessful errback.

        @todo more needed.
        """
        # TODO: what happens here? default transition?
        failure.trap(StandardError)
        failure.printBriefTraceback()
        log.error("AsyncFSM now in error")


