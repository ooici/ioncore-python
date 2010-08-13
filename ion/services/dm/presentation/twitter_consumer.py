#!/usr/bin/env python

"""
@file ion/services/dm/presentation.py
@author David Stuebe
@brief twitter the notification component of the data!
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

import logging
logging = logging.getLogger(__name__)

try:
    import twitter
except:
    class twitter():
        def Api(username='', password=''):
            pass
        def PostUpdate(string):
            pass

class TwitterConsumer(base_consumer.BaseConsumer):
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
        logging.info('Sending twitter notification...')
        
        if not uname:
            raise RuntimeError('No twitter user name given!')
            
        if not pword:
            raise RuntimeError('No twitter password given!')
            
        api = twitter.Api(username=uname, password=pword)
        api.PostUpdate(notification[:140])
        
        logging.info('Tweet sent')
        
        
#        try:
#            
#        except:
#           logging.exception('Twitter error!')
#
        
        

# Spawn of the process using the module name
factory = ProtocolFactory(TwitterConsumer)
