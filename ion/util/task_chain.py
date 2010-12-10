#!/usr/bin/env python

"""
@file ion/util/task_chain.py
@author Dave Foster <dfoster@asascience.com>
@brief TaskChain class for sequential execution of callables (deferreds and non-deferreds)
"""

from twisted.internet import defer

# Disabling MutableSequence for 2.5 compat -> deriving from list for now.
# search for MUTABLESEQUENCE to see what needs to be uncommented/fixed.
#class TaskChain(MutableSequence):
class TaskChain(list):
    """
    Used to set up a chain of tasks that run one after another.

    A task chain can be used to script a sequence of actions and have the Twisted
    reactor manage it all. The run method returns a deferred that is called back
    when all tasks in the chain complete. If any task errors, the chain is aborted
    and the errback is raised. The tasks are executed in order.

    The tasks should be callables that can either return deferreds or execute
    synchronously (and TaskChain will wrap them in deferreds). If any of them error,
    the chain is aborted and the errback is raised.

    A TaskChain is a list of either callables or tuples of 2 or 3 length, that consist
    of a callable, a list of arguments to be passed, and an optional dict of keyword
    arguments to be passed.

    Due to not having MutableSequence available in Python 2.5, type checking is done
    only at time of execution.
    """

    def __init__(self, *tasks):
        """
        Constructor.

        @param tasks  Takes a list of tasks that will be executed in order.
        """
        #self._list      = []        # implementation backend for MutableSequence methods to use        # MUTABLESEQUENCE

        self.extend(tasks)

        self._donetasks = []
        self._results   = []
        self._running   = False
        self._curtask   = None
        self._curtask_def = None
        self._deferred  = defer.Deferred()

        self._lenprocs  = len(self)

    def __str__(self):
        """
        Returns a string representation of a TaskChain and its status.
        """
        return "TaskChain (running=%s, %d/%d)" % (str(self._running), len(self._donetasks), len(self) + len(self._donetasks))

    def _check_type(self, obj):
        """
        Internal safety mechanism that append, insert, and extend all flow through.
        It makes sure the types being added to the list are expected.
        """
        if isinstance(obj, tuple):
            if not (len(obj) == 2 or len(obj) == 3):
                raise ValueError("Invalid number of arguments in tuple: (callback, list of args, optional dict of kwargs) expected.")
            if not callable(obj[0]):
                raise ValueError("First item of tuple not a callable: (callback, list of args, optional dict of kwargs) expected.")
            if not isinstance(obj[1], list):
                raise ValueError("Second item of tuple not a list of args: (callback, list of args, optional dict of kwargs) expected.")
            if len(obj) == 3 and not isinstance(obj[2], dict):
                raise ValueError("Third item of tuple not a dict of kwargs: (callback, list of args, optional dict of kwargs) expected.")
        else:
            if not callable(obj):
                raise ValueError("Item must be a callable")

    # MUTABLESEQUENCE
    #def __getitem__(self, index):
    #    return self._list.__getitem__(index)

    #def __setitem__(self, index, value):
    #    self._check_type(value)
    #    self._list.__setitem(index, value)

    #def __delitem__(self, index):
    #    self._list.__delitem__(index)

    #def insert(self, index, value):
    #    self._check_type(value)
    #    self._list.insert(index, value)

    #def __len__(self):
    #    return len(self._list)
    # END MUTABLESEQUENCE

    def run(self):
        """
        Starts running the chain of tasks.

        @returns A deferred which will callback when the tasks complete.
        """
        log.debug("TaskChain starting")
        self._running = True
        self._run_one()
        return self._deferred

    def _run_one(self):
        """
        Runs the next task.
        """

        # if we have no more tasks to run, or we shouldn't be running anymore,
        # fire our callback
        if len(self) == 0 or not self._running:
            self._fire(True)
            return

        log.debug(self.__str__() + ":running task")

        self._curtask = self.pop(0)
        args = []
        kwargs = {}

        # make sure this is legit - we have no way of checking on insert right now due to not being a MutableSequence
        self._check_type(self._curtask)

        if isinstance(self._curtask, tuple):
            aslist = list(self._curtask)
            self._curtask = aslist.pop(0)
            args = aslist.pop(0)
            if len(aslist):
                kwargs = aslist.pop()

        # possibly not a deferred at all!
        self._curtask_def = defer.maybeDeferred(self._curtask, *args, **kwargs)
        self._curtask_def.addCallbacks(self._proc_cb, self._proc_eb)

    def _proc_cb(self, result):
        """
        Callback on single task success.
        """
        self._results.append(result)
        self._donetasks.append(self._curtask)
        self._curtask = None
        self._curtask_def = None

        log.debug(self.__str__() + ":task finished")

        self._run_one()

    def _proc_eb(self, failure):
        """
        Errback on single failure.
        """
        failure.trap(StandardError)
        failure.printBriefTraceback()

        log.debug(self.__str__() + ":task ERROR")

        self._results.append(failure.value)
        self._fire(False)

    def _fire(self, success):
        """
        Calls the task chain's callback or errback as per the success parameter.
        This method builds the task/result list to pass back through either mechanism.
        """

        # we're no longer running, indicate as such
        self._running = False

        log.debug(self.__str__() + ":terminating, success=%s" % str(success))

        res = zip(self._donetasks, self._results)
        if success:
            self._deferred.callback(res)
        else:
            self._deferred.errback(StandardError(res))

    def close(self):
        """
        Shuts down the current chain of tasks.
        The current executing task will have its cancel method called on its deferred.
        It is the responsibility of the deferred creator to set up the canceller argument
        when the deferred is constructed.
        """

        log.debug(self.__str__() + ":close")

        if not self._running:
            # someone could call close after we've already fired, so don't fire again
            if not self._deferred.called:
                self._fire(True)
            return self._deferred

        self._running = False

        if self._curtask_def:
            self._curtask_def.cancel()

        return self._deferred


