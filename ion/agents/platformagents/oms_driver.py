#!/usr/bin/env python
"""
@file ion/agents/platformagents/oms_driver.py
@author Derrick Cote (RSN)
@author Matthew Milcic (RSN)
@author Steve Foley
@brief Driver code for XML-RPC interaction with the OMS server
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from twisted.web.xmlrpc import Proxy

from ion.core.process.process import Process
from ion.core.process.process import ProcessFactory


class OMSDriver(Process):
    """
    This is the initial test code to talk to the OMS server via XML-RPC. It
    was initially built by the RSN team as a test stub and has been moved into
    the capability container by the IPAA team.
    
    Notes for the initial test stub implementation:
    
    This application should be run by first creating an instance of the class
    and then calling the connect() method on that instance.  

    The connect method will attempt to get the list of devices of the xmlrpc
    server defined in the __init__ constructor.  The connect method hard codes
    the string 'Server' as the class of devices to return to form the list.

    If the device list is returned successfully the connect method will start
    its callback routine for the two callbacks added in connect.  The first is 
    _updateValues, which sets the devices list for the class and initializes
    the device counter.  The next is _startConnections.

    _startConnections will use the device property to get one device from the list 
    returned in connect.  It will then loop the amount of times defined in 
    _maxConnections, which in this example is 20.  Each loop adds to the reactor
    queue one query for a device value.  So there will be 20 asynchronous calls
    added to the reactor queue.

    When the queries return the callback is fired and first _print value is called.
    Print value will simple print the value returned by the query.  The second 
    callback function is _getMoreData.  This will add 1 more query to the reactor 
    loop.

    Since each query adds another query at when it returns there will always be 
    20 requests to the database for data.
    """
    
    
#    def __init__(self, *args, **kwargs):
#        pass
    
    def plc_init(self):
        log.debug("OMSDriver.plc_init: spawn_args: %s", self.spawn_args)
        self._proxy = Proxy(self.spawn_args.get('serverurl'))
        self._maxConnections = self.spawn_args.get('maxconnections')
        self._devices = None

    @defer.inlineCallbacks
    def op_connect(self, content, headers, msg):
        """
        Queries the xmlrpc for devices and starts the data collection.

        Calls the xmlrpc server with the twisted proxy callRemote method.
        This will return a deferred which on success will callback with the
        list of devices. It uses a feature of deferreds which allows multiple
        callbacks methods to be called from the same deferred.

        For this prototype the 'Server' device class is hard coded for the
        query to ensure data is returned from the test OMS.
        """
        result = yield self._proxy.callRemote('getDeviceListByType', 'Server')
        log.debug("connect result: %s", result)
        yield self._make_single_connection(result)
        
    @defer.inlineCallbacks
    def _make_single_connection(self, devlist):
        log.debug("***starting single connect")
        for dev in devlist:
            result = yield self._proxy.callRemote('getDeviceAttribute', dev, 'if1Speed')
            log.debug("single connect result: %s", result)
        yield self._printValue(result)
        
    @defer.inlineCallbacks
    def _printValue(self, value, name):
        """
        Prints the value along with the name of which of the 20 asynchronous
        query's returned the value.
        """
        log.debug("Value %s returned from asynchronous call %d", (value, name))

# Spawn of the process using the module name
factory = ProcessFactory(OMSDriver)


