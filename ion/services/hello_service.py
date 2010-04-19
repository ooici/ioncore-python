#!/usr/bin/env python

"""
@file ion/services/hello_service.py
@author Michael Meisinger
@package ion.services  example service definition that can be used as template
"""

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

class HelloService(BaseService):
    """Service implementation
    """
