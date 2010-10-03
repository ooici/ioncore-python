#!/usr/bin/env python

"""
@file ion/core/exception.py
@author Michael Meisinger
@brief module for exceptions
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class IONError(StandardError):
    pass

    # @todo Some better str output

class ConfigurationError(IONError):
    pass

class StartupError(IONError):
    pass

class IllegalStateError(IONError):
    pass
