#!/usr/bin/env python

"""
@file ion/core/ionconst.py
@author Michael Meisinger
@brief definitions of constants and static code that should be executed in any system run
"""
import logging
import logging.config

LOGCONF_FILENAME = 'res/ionlogging.conf'
logging.config.fileConfig(LOGCONF_FILENAME)


SVC_LIST_ALL = 'res/config/ionservices.cfg'