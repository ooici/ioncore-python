#!/usr/bin/env python

"""
@file ion/core/ionconst.py
@author Michael Meisinger
@brief definitions of ION packages wide constants
"""

# Name of central logginf configuration file
LOGCONF_FILENAME = 'res/logging/ionlogging.conf'

# Name of central ION configuration file (not to be changed)
ION_CONF_FILENAME = 'res/config/ion.config'

# Name of local ION config override file (can be changed locally)
ION_LOCAL_CONF_FILENAME = 'res/config/ionlocal.config'

# ION master version
# @todo use magnet version system
VERSION = "0.2.1"

# Minimum version of Magnet required
MIN_MAGNET = "0.3.5"
