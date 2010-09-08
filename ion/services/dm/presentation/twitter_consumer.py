#!/usr/bin/env python

"""
@file ion/services/dm/presentation/twitter_consumer.py
@author David Stuebe
@brief twitter the notification component of the data!
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


try:
    import twitter
except:
    
    class twitter():
        """Bogus twitter class to prevent failures
        """
        @classmethod
        def Api(cls, username='', password=''):
            return cls()
        def PostUpdate(self, string):
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
        log.info('Sending twitter notification...')
        
        if not uname:
            raise RuntimeError('No twitter user name given!')
            
        if not pword:
            raise RuntimeError('No twitter password given!')
            
        api = twitter.Api(username=uname, password=pword)
        api.PostUpdate(notification[:140])
        
        log.info('Tweet sent')
        

# Spawn of the process using the module name
factory = ProtocolFactory(TwitterConsumer)
