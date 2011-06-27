"""
@file ion/services/coi/hostsensor/host_status_daemon.py
@author Brian Fox
@brief Simple XMLRPC server for serving host status
"""
import encoders
from optparse import OptionParser
from readers import HostReader

try:
    import json
except:
    import simplejson as json


class HostReport:
    """
    Simple SNMP command line utility which returns host status 
    (RFC1213, RFC2790, python system info) on request.  This wraps
    the various classes in readers.py.

    This utility prints a JSON string.  It is meant to be parsed
    programmatically by the Twisted ProcessProtocol class.
    
    @todo Include interface ipv4 and ipv6 addresses
    @todo Move memory from 'storage' to another location
    """

    def __init__(
                 self,
                 snmpHost          = 'localhost',
                 snmpPort          = 161,
                 snmpAgentName     = 'ooici',
                 snmpCommunityName = 'public',
                 ):
        """
        Creates the SNMP client
        """
        self.hostreader = HostReader(
                                 snmpHost,
                                 snmpPort,
                                 snmpAgentName,
                                 snmpCommunityName
                                 )
        self.snmpHost = snmpHost
        self.snmpPort = snmpPort
        self.snmpAgentName = snmpAgentName,
        self.snmpCommunityName = snmpCommunityName


    def getStatus(self,system):
        """
        Gets the status of this host
        """
        report = self.hostreader.get(system)
        return encoders.encodeJSONToXMLRPC(report)


    def getStatusString(self,system):
        """
        Gets the status of this host
        """
        report = self.hostreader.get(system)
        report = self.hostreader.pformat(report)
        return report


def main():
    usage = "usage: %prog <report>\n\treport = [all,base,network,cpu,storage]"
    parser = OptionParser(usage=usage)
    (options, args) = parser.parse_args()
    if len(args) != 1 or args[0] not in ['all','base','network','cpu','storage']:
        parser.error("A report must be specified.")
        
    client = HostReport()
    report = client.getStatus(args[0])
    print json.dumps(report , sort_keys = False, indent = 4)
    print HostReader.pformat(report)
    
if __name__ == '__main__':
    main()
