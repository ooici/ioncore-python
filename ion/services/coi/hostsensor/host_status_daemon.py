"""
@file ion/services/coi/hostsensor/host_status_daemon.py
@author Brian Fox
@brief Simple XMLRPC server for serving host status
"""
import logging
import encoders
from SimpleXMLRPCServer import SimpleXMLRPCServer
from readers import HostReader
from base_daemon import Daemon
 

class HostStatusRPCServer:
    """
    XMLRPC server for returning host status on request.  This wraps 
    the various classes in readers.py into an XMLRPC server.

    @todo Include interface ipv4 and ipv6 addresses 
    @todo Move memory from 'storage' to another location
    @todo Add daemon logging
    """

    def __init__(
                 self, 
                 snmpHost          = 'localhost', 
                 snmpPort          = 161,
                 snmpAgentName     = 'ooici',
                 snmpCommunityName = 'ooicinet',
                 rpcHost           = 'localhost',
                 rpcPort           = 9010  ):
        """
        Creates the RPC server
        """
        self.server = SimpleXMLRPCServer((rpcHost, rpcPort), allow_none=True)
        self.status = HostReader(
                                 snmpHost, 
                                 snmpPort, 
                                 snmpAgentName, 
                                 snmpCommunityName
                                 )
        self.server.register_function(self.getStatus)
        self.server.register_function(self.getStatusPrettyPrint)
        self.server.register_introspection_functions()
        logging.debug('host_status_daemon intialized')


    def run(self):
        """
        Puts the server in motion.  Blocks forever.
        """
        self.server.serve_forever()


        
    def getStatus(self):
        """
        Gets the status of this host (RPC registered function)
        """
        return encoders.encodeJSONToXMLRPC(self.status.getAll())


    def getStatusPrettyPrint(self):
        """
        Gets the status of this host (RPC registered function)
        """
        return encoders.encodeJSONToXMLRPC(self.status.getAllPrettyPrint())

        
 
class HostStatusDaemon(Daemon):
    """
    Runs a HostStatusRPCServer as a Unix daemon. 
    """    
    def run(self):
        server = HostStatusRPCServer()
        server.run()




class HostStatusNoDaemon():
    """
    Runs a HostStatusRPCServer as a process which stays in the
    foreground. 
    """   
    def __init__(self): 
        server = HostStatusRPCServer()
        server.run()




if __name__ == "__main__":
    # runAlways = HostStatusNoDaemon()
    daemon = HostStatusDaemon('/tmp/host_status_daemon.pid','/tmp/host_status_daemon.log')
    daemon.processCommandLine()
 
        