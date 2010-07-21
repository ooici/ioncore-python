from SimpleXMLRPCServer import SimpleXMLRPCServer
from readers import HostReader
import os,sys,encoders
from base_daemon import Daemon
 

class HostStatusRPCServer:
    """
    RPC server for returning host status on request. 

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
        self.server = SimpleXMLRPCServer((rpcHost, rpcPort))
        self.status = HostReader(
                                 snmpHost, 
                                 snmpPort, 
                                 snmpAgentName, 
                                 snmpCommunityName
                                 )
        self.server.register_function(self.getStatus)
        self.server.register_introspection_functions()



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
    daemon = HostStatusDaemon('/tmp/host_status_daemon.pid')
    daemon.processCommandLine()
 
        