#!/usr/bin/env python

"""
@file twisted/plugins/cc.py
@author Dorian Raymer
@author Michael Meisinger
@brief Twisted plugin definition for the Python Capability Container
"""

from twisted.application.service import ServiceMaker

CC = ServiceMaker(
        name="ION CapabilityContainer",
        module="ion.core.cc.service",
        description="ION Capability Container",
        tapname='cc')
