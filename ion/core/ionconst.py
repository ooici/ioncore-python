#!/usr/bin/env python

"""
@file ion/core/ionconst.py
@author Michael Meisinger
@brief definitions of constants and static code that should be executed in any system run
"""
import logging
import logging.config

from ion.util.config import Config

LOGCONF_FILENAME = 'res/ionlogging.conf'
logging.config.fileConfig(LOGCONF_FILENAME)

ION_CONF_FILENAME = 'res/ion.config'
ion_config = Config(ION_CONF_FILENAME)

def config(name):
    return Config(name, ion_config)
