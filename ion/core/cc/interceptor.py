#!/usr/bin/env python

"""
@file ion/core/cc/interceptor.py
@author Michael Meisinger
@brief capability container interceptor system (security, governance)
"""

import logging

from magnet.container import InterceptorSystem

def _gettransform(purpose):
    def pass_transform(msg):
        """Trivial identity transform -- pass all
        """
        #logging.info('pass_transform ' + purpose + ' ' +str(msg))
        return msg
    return pass_transform

class BaseInterceptorSystem(InterceptorSystem):
    """Custom capability container interceptor system for secutiry and
    governance purposes.
    """
    def __init__(self):
        InterceptorSystem.__init__(self)
        self.registerIdmInterceptor(_gettransform('IdM'))
        self.registerPolicyInterceptor(_gettransform('Policy'))
        self.registerGovernanceInterceptor(_gettransform('Gov'))
        logging.info("Initialized ION BaseInterceptorSystem")

