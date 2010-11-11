#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/inst_consumer.py
@author David Stuebe
@brief Don't know yet 
receives.
"""

from ion.services.dm.distribution import base_consumer

from ion.core.process.process import ProcessFactory

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

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

        log.info('InstrumentConsumer recieved a data message: \n %s' % info)
        if notification:
            log.info('Data Message Notification:\n %s' % notification)
        else:
            log.debug('Data Message Notification Empty!\n')
            
        log.info('Data Message Timestamp:%s' % timestamp)
        
        log.debug('Data Message Data:'+str(data)) # Something more useful?
        

# Spawn of the process using the module name
factory = ProcessFactory(LoggingConsumer)

