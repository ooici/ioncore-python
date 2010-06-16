"""
Client to test Seabird SBE49 Instrument Simulator. 

@file client_SBE49_sim.py
@author Dave Everett 
@date 6/8/10
"""

from twisted.internet import stdio, reactor, protocol
from twisted.protocols import basic
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
        reactor.stop( )

    def clientConnectionFailed(self, transport, reason):
        print reason.getErrorMessage( )
        reactor.stop( )

if __name__ == '__main__':
    import sys
    if not len(sys.argv) == 3:
        print "Usage: %s host port" % __file__
        sys.exit(1)

    reactor.connectTCP(sys.argv[1], int(sys.argv[2]), StdioProxyFactory( ))

    reactor.run( )

