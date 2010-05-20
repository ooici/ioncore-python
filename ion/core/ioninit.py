#!/usr/bin/env python

"""
@file ion/core/ioninit.py
@author Michael Meisinger
@brief definitions and code that needs to run for any use of ion
"""

import logging.config
import magnet
import re

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

def check_magnet_version():
    minmv = ic.MIN_MAGNET.split('.')
    mv = magnet.__version__.split('.')
    mv[2] = mv[2].partition('+')[0]
    if mv[0]<minmv[0] or mv[1]<minmv[1] or mv[2]<minmv[2]:
        logging.error("*********** ATTENTION! Magnet %s required. Is %s ***********" % (ic.MIN_MAGNET, magnet.__version__))

check_magnet_version()