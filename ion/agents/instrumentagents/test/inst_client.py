#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/test/inst_client.py
@brief This module is a simple client to test instruments/simulators, and is run
 as a stand alone command line application.
@author Bill Bollenbacher
@date 10/15/10
"""

from twisted.internet import stdio, reactor, protocol
import re

class DataForwardingProtocol(protocol.Protocol):
    """
    The DataForwardingProtocol class.  Gets data from stdio and forwards to 
    the TCP connection.
    """

    def __init__(self):
        self.output = None
        self.normalizeNewlines = False

    def dataReceived(self, data):
        """
        @brief data has been received from either the stdio or the tcp
        connection (I THINK)
        """
        if self.normalizeNewlines:
            data = re.sub(r"(\r\n|\n)", "\r\n", data)
        if self.output:
            self.output.write(data)

class StdioProxyProtocol(DataForwardingProtocol):
    """
    The StdioProxyProtocol class. Hooks the TCP and STDIO together.
    """
    def connectionMade(self):
        """
        @brief connection has been made: hook the TCP and stdio together.
        """
        inputForwarder = DataForwardingProtocol( )
        inputForwarder.output = self.transport
        inputForwarder.normalizeNewlines = True
        stdioWrapper = stdio.StandardIO(inputForwarder)
        self.output = stdioWrapper
        print "Connected to server.  Press ctrl-C to close connection."

class StdioProxyFactory(protocol.ClientFactory):
    """
    The StdioProxyFactory class. This is a ClientFactory, and sets 
    the protocol to the StdioProxyProtocol, which is the data forwarding
    protocol. 
    """
    protocol = StdioProxyProtocol

    def clientConnectionLost(self, transport, reason):
        if len(sys.argv) == 4:
           simulator.stop()
        print ""    # output LF to cleanup after ctrl-C


    def clientConnectionFailed(self, transport, reason):
        print reason.getErrorMessage( )
        reactor.stop( )
        

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 3 or len(sys.argv) > 4 or \
       (len(sys.argv) == 4 and not sys.argv[3].startswith('sim')):
        print "Usage: %s host port [sim_INSTRUMENT-NAME]" % __file__
        sys.exit(1)

    if len(sys.argv) == 4:
        try:
            exec ('from ion.agents.instrumentagents.simulators.{0} import Simulator'.format(sys.argv[3]))
        except:
            print 'ERROR while trying to import Simulator from module {0}'.format(sys.argv[3])
            sys.exit(1)
        simulator = Simulator(sys.argv[1], int(sys.argv[2]))
        simulator.start()

    reactor.connectTCP(sys.argv[1], int(sys.argv[2]), StdioProxyFactory( ))

    reactor.run( )

