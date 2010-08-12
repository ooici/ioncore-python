#!/usr/bin/env python

"""
@file ion/services/dm/distribution/consumers/logging_consumer.py
@author David Stuebe
@brief The logging consumer process - it logs a message about whatever data it
receives.
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

import logging
logging = logging.getLogger(__name__)

from pydap.model import DatasetType

class LoggingConsumer(base_consumer.BaseConsumer):
    """
        This is an simple consumer to loggin messages on a queue/topic
        """
    def ondata(self, data, notification, timestamp):
        
        if isinstance(data,str):
            info = 'String Dataset: ' +data
        elif isinstance(data, DatasetType):
            info = 'Dap Dataset Name:'+  data.name
        elif isinstance(data, dict):
            info = 'Dict Dataset Name:' + data.get('name','No name attribute in data dictionary?')
        elif data == None:
            info = 'Received empty Data Message'
        else:
            info = 'Unknown dataset type' + str(data)

        logging.info('LoggingConsumer recieved a data message: \n %s' % info)
        if notification:
            logging.info('Data Message Notification:\n %s' % notification)
        else:
            logging.debug('Data Message Notification Empty!\n')
            
        logging.info('Data Message Timestamp:%s' % timestamp)
        
        logging.debug('Data Message Data:'+str(data)) # Something more useful?
        

# Spawn of the process using the module name
factory = ProtocolFactory(LoggingConsumer)
