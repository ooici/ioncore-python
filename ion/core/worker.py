#!/usr/bin/env python

"""
@file ion/services/coi/exchange_registry.py
@author Michael Meisinger
@brief service for registering exchange names
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class WorkerProcess(BaseService):
    """Worker process
    """
    
# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
receiver = Receiver(__name__, 'worker1')

instance = WorkerProcess(receiver)