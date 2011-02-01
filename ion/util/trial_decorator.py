"""
@file ion/util/trial_decorator.py
@author Paul Hubbard
@author David Stuebe
@date 2/1/11
@brief Unit test decorator that skips unit/integration tests based on the
ion configuration file. This lets us separate integration tests for buildbot.
@note Experimental code!

"""

import ion.util.ionlog

class itv(object):

    def __init__(self, config):
        self.config = config

    def __call__(self, func):

        # Look up in config file, default is to skip the test
        run_test = self.config.getValue(func.__name__, False)

        if run_test:
            my_func = func

        else:
            def my_func(*args, **kwargs):
                # We want the logger associated with the wrapped function
                log = ion.util.ionlog.getLogger(func.__name__)

                strng = 'Skipping the %s integration test.' % func.__name__
                log.info(strng)

        return my_func
