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
from twisted.internet import task
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

        For this prototype the 'Server' device class is the only one understood
        for the query to ensure data is returned from the test OMS.
        """
        assert(isinstance(content, (list, tuple)))
        assert(isinstance(content[0], str)) # type of device
        assert(isinstance(content[1], str)) # attribute name
        
        device_result = yield self._proxy.callRemote('getDeviceListByType', content[0])
        log.debug("Device list result: %s", device_result)
        
        #for device in device_result
        #   attr_result = yield self._get_single_attribute(device, content[1])
        result = {}
        finished = yield self._parallel_get_attribute(device_result,
                                                      10,
                                                      self._get_single_attribute,
                                                      content[1],
                                                      result)
        log.debug(finished)
        log.debug(result)
        yield self.reply_ok(msg, result)
        
    @defer.inlineCallbacks
    def _get_single_attribute(self, dev, attribute, result):
        """
        Make a connection to fetch a single attribute for the given list of
        devices.
        @param dev A list of strings indicating devices
            (ie ['1.2.3.4', '5.6.7.8']) to be queried for a given attribute.
        @param attribute The attribute to query for
        @param result the dict to add finished values to
        @retval A dictionary with server/attribute entities
            ie {'10.180.80.202': '606', '10.180.80.201': '353'}
        """
        log.debug("Starting single connect, devlist: %s, attr: %s", dev, attribute)
        log.debug("Asking for %s, %s", dev, attribute)
        devresult = yield self._proxy.callRemote('getDeviceAttribute', dev, attribute)
        log.debug("Single attribute result: %s", devresult)
        assert(isinstance(devresult, list))
        result[devresult[0][0]] = devresult[0][1]
    
        defer.returnValue(result)
        
    def _parallel_get_attribute(self, iterable, count, callable, *args, **named):
        """
        Get a number of attributes/hosts in parallel
        @param iterable The list to iterate over for the callable
        @param count The maximum number of simultaneous connections to track
        @param The function name to perform in parallel
        @param args Arguments to apply to the function being called
        @retval A list of deferred
        """
        coop = task.Cooperator()
        work = (callable(elem, *args, **named) for elem in iterable)
        return defer.DeferredList([coop.coiterate(work) for i in xrange(count)])
 

# Spawn of the process using the module name
factory = ProcessFactory(OMSDriver)


