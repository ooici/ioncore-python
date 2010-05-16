#!/usr/bin/env python

"""
@file ion/core/ioninit.py
@author Michael Meisinger
@brief definitions and code that needs to run for any use of ion
"""

import logging.config

from ion.core import ionconst as ic
from ion.util.config import Config

print "ION (Integrated Observatory Network) packages initializing ("+ic.VERSION+")"

# Configure logging system (console, logfile, other loggers)
logging.config.fileConfig(ic.LOGCONF_FILENAME)

# Load configuration properties for any module to access
ion_config = Config(ic.ION_CONF_FILENAME)

# Arguments given to the container (i.e. the python process executing this code)
cont_args = {}

def config(name):
    """
    Get a subtree of the global configuration, typically for a module
    """
    return Config(name, ion_config)

def get_config(confname, conf=None):
    """
    Returns a Config instance referenced by the file name in the system config's
    entry
    @param confname  entry in the conf configuration
    @param conf None for system config or different config
    """
    if conf == None:
        conf = ion_config
    return Config(conf.getValue(confname)).getObject()
