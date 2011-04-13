#!/usr/bin/env python

"""
@file ion/__init__.py
@author Michael Meisinger

@mainpage
@section intro Introduction

The lcaarch project is a GIT source code repository and integrated Python code
base that provides some basic framework code for bootstrapping a CI-wide
prototype and for running prototype services, and the actual service
implementations themselves together with unit level and system level
test cases. The scope of the services is the full functionality of the
OOI CI deliverable of release 1 transcending any subsystem prototypes.
Initially, all services start out as stubs with their message sending and
receiving interface only and have the potential to grow into a fully functional
operational architecture prototype as required for LCA.


@see http://www.oceanobservatories.org/spaces/display/CIDev/LCAARCH+Development+Project

"""

# Make sure the init code in ioninit (e.g. logging settings) executes before
# anything else in the system

from ion.core import ioninit
from ion.core import ionconst


#__version__ = ionconst.VERSION
