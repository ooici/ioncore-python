#!/usr/bin/env python

"""
@file ion/services/sa/data_processing.py
@author Michael Meisinger
@brief service for data processing
"""

import logging
log = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class DataProcessingService(BaseService):
    """Data processing service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_processing',
                                          version='0.1.0',
                                          dependencies=[])

    def op_define_process(self, content, headers, msg):
        """
        Service operation: Create or update a data process. A data process
        works on data messages and is assumed to have parameterizable input
        and output
        """
        self.reply_err(msg,"Not yet implemented")

    def op_schedule_processing(self, content, headers, msg):
        """
        Service operation: Defines processing based on schedule or event
        trigger, given a data process and required input and output streams.
        """
        self.reply_err(msg,"Not yet implemented")

    def op_cancel_processing(self, content, headers, msg):
        """
        Service operation: Remove scheduled processing.
        """
        self.reply_err(msg,"Not yet implemented")

# Spawn of the process using the module name
factory = ProtocolFactory(DataProcessingService)
