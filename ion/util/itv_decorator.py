#!/usr/bin/env python

"""
@file ion/util/itv_decorator.py
@author David Stuebe
@brief A decorator class to skip unit tests not called out in your local config

@TODO Make the skip specific to a class within a test module?
"""

from twisted.trial import unittest

class itv(object):

    def __init__(self, config):
        self.config = config

    def __call__(self, func):
            
        run_test = self.config.getValue(func.__name__, False)
        
        if run_test:
            my_func = func
            
        else:
            def my_func(*args, **kwargs):
                strng = 'Skipping the %s integration test.' % func.__name__
                raise unittest.SkipTest(strng)
                
        return my_func


