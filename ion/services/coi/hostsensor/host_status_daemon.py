"""
@file ion/services/coi/hostsensor/host_status_daemon.py
@author Brian Fox
@brief Simple XMLRPC server for serving host status
"""
import ion.util.ionlog, sys
import encoders
from optparse import OptionParser
from SimpleXMLRPCServer import SimpleXMLRPCServer
from readers import HostReader
from base_daemon import Daemon


class HostStatusDaemon(Daemon):
    """
    XMLRPC server for returning host status on request.  This wraps
    the various classes in readers.py into an XMLRPC server.

    @todo Include interface ipv4 and ipv6 addresses
    @todo Move memory from 'storage' to another location
    @todo Add daemon logging
    """

    def __init__(
                 self,
                 pidfile,
                 logfile,
                 testmode = False,
                 snmpHost          = 'localhost',
                 snmpPort          = 161,
                 snmpAgentName     = 'ooici',
                 snmpCommunityName = 'ooicinet',
                 rpcHost           = 'localhost',
                 rpcPort           = 9010  ):
        """
        Creates the RPC server
        """
        Daemon.__init__(self, pidfile, logfile)
        self.hostreader = HostReader(
                                 snmpHost,
                                 snmpPort,
                                 snmpAgentName,
                                 snmpCommunityName
                                 )
        self.rpcPort = rpcPort
        self.rpcHost = rpcHost
        log.debug('host_status_daemon intialized')


    def run(self):
        """
        Puts the server in motion.  Blocks forever.
        """
        server = SimpleXMLRPCServer((self.rpcHost, self.rpcPort), allow_none=True)
        server.register_function(self.getStatus)
        server.register_function(self.getStatusString)
        server.register_introspection_functions()
        server.serve_forever()


    def getStatus(self,system):
        """
        Gets the status of this host (RPC registered function)
        """
        report = self.hostreader.get(system)
        return encoders.encodeJSONToXMLRPC(report)


    def getStatusString(self,system):
        """
        Gets the status of this host (RPC registered function)
        """
        report = self.hostreader.get(system)
        report = self.hostreader.pformat(report)
        return report



class _OptionParser(OptionParser):
    """
    Subclassed only to create a more meaningful error message.
    """
    def error(self,message):
        print "error: " + str(message)
        print "try -h for help"
        sys.exit(-1)


def main():
    usage = "usage: %prog [options] command\n\ncommand = [start,stop,restart,status]"
    parser = _OptionParser(usage=usage)
    parser.add_option("-t", "--test",
                      action="store_true",
                      dest="test_flag",
                      default=False,
                      help="place the daemon in test mode (faster but with limited SNMP queries)")
    (options, args) = parser.parse_args()

    if len(args) != 1 or args[0] not in ['start','stop','restart','status']:
        parser.error("you must specify a command: start,stop,restart,status")

    # runAlways = HostStatusNoDaemon()
    daemon = HostStatusDaemon(
        '/tmp/host_status_daemon.pid',
        '/tmp/host_status_daemon.log',
        testmode=False # testmode=options['test_flag']
    )
    daemon.doCommand(args[0])

if __name__ == '__main__':
    main()
