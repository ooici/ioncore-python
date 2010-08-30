#!/usr/bin/env python

"""
@file ion/services/dm/distribution/consumers/example_consumer.py
@author David Stuebe
@brief example consumer process. Consumes a simple kind of dap data message
producing an event for samples less than 0 or greater than 100. The qaqc result
is sent to a separate queue. These destinations are set as part of the spawnargs
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

import logging
log = logging.getLogger(__name__)

from pydap.model import DatasetType

from ion.services.dm.util import dap_tools

class ExampleConsumer(base_consumer.BaseConsumer):
    """
    This is an example data consumer process. It applies a process to the data
    and sends the results to a 'qaqc' queue and an event queue. The send-to
    location is a parameter specified in the consumer class spawn args,
    'delivery queues' which is passed as **kwargs to ondata
    """
    def ondata(self, data, notification, timestamp, event_queue='', processed_queue=''):
        
        resdata = []
        tsdata = []
        # Process the array of data
        for ind in range(data.height.shape[0]):
            
            ts = data.time[ind]
            samp = data.height[ind]
            if samp<0 or samp>100:
                # Must convert pydap/numpy Int32 to int!
                if event_queue:
                    self.queue_result(event_queue,\
                                      {'event':(int(ts),'out_of_range',int(samp))},\
                                        'out_of_range')
                samp = 0
            qcsamp = samp
            # Must convert pydap/numpy Int32 to int!
            resdata.append(int(qcsamp))
            tsdata.append(int(ts))
        
        
        dset = dap_tools.simple_dataset(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':tsdata, 'height':resdata})
        
        # Messages contains a new dap dataset to send to send 
        if processed_queue:
            self.queue_result(processed_queue,dset,'Example processed data')
        

# Spawn of the process using the module name
factory = ProtocolFactory(ExampleConsumer)
