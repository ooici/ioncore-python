#!/usr/bin/env python


"""
@file ion/services/dm/presentation/twitter_consumer.py
@author David Stuebe
@brief twitter the notification component of the data!
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

import logging
logging = logging.getLogger(__name__)

class GoogleChartConsumer(base_consumer.BaseConsumer):
    """
    As a demo, uses python twitter to send updates out via twitter. this
    * Gives us RSS, email and SMS notifications for free
    * Requires 2 lines of code
    * is free of cost.
    Nice.
    
    @see http://code.google.com/p/python-twitter/
    """

    def ondata(self, data, notification, timestamp, uname='', pword=''):
        """
        """
        logging.info('Updating google chart')
        
        

# Spawn of the process using the module name
factory = ProtocolFactory(GoogleChartConsumer)
