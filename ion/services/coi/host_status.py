from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib    
try:
    from pysnmp.entity.rfc3413.oneliner import cmdgen
    PysnmpImported = True
except ImportError:
    PysnmpImported = False


class HostStatusRPCServer:
    """
    RPC server for returning host status on request. 
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
        Test
        """
        self.server = SimpleXMLRPCServer((rpcHost, rpcPort))
        self.status = HostStatus(
                                 snmpHost, 
                                 snmpPort, 
                                 snmpAgentName, 
                                 snmpCommunityName
                                 )
        self.server.register_function(self.getStatus)
        self.server.register_introspection_functions()
        self.server.serve_forever()
        
    def getStatus(self):
        return self.status.getAll()
        
        
class HostStatusRPCClient:
    """
    RPC client for testing only. 
    """

    def __init__(
                 self, 
                 rpcHost           = 'localhost',
                 rpcPort           = 9010  ):
        """
        Test
        """
        self.client = xmlrpclib.ServerProxy('http://%s:%d'%(rpcHost, rpcPort))
        
    def getStatus(self):
        return self.status.getAll()

class HostStatus:
    """
    Represents the status of the local host as retrieved using
    SNMP and RFC1213 and RFC2790 SNMP MIB definitions.
    """
    def __init__(self, host, port, agentName, communityName):
        self.reader = SnmpReader(host, port, agentName, communityName)
        

    """
    Produces a dictionary from the getNetworkInterfaces, getStorage,
    and getProcesses methods.
    """
    def getAll(self):
        ret = {}
        ret['NetworkInterfaces'] = self.getNetworkInterfaces()
        ret['Storage']           = self.getStorage()
        ret['Processes']         = self.getProcesses()
        return ret

        
    """
    Gets information about the host's network interfaces.
    """
    def getNetworkInterfaces(self):
        return self.reader.getTable(
                Rfc1213Mib.interfaces_ifTable,
                [
                    ('Descr'     , Rfc1213Mib.interfaces_ifTable_ifDescr)
                    ,('Speed'     , Rfc1213Mib.interfaces_ifTable_ifSpeed)
                    ,('InOctets'  , Rfc1213Mib.interfaces_ifTable_ifInOctets)
                    ,('InErrors'  , Rfc1213Mib.interfaces_ifTable_ifInErrors)
                    ,('OutOctets' , Rfc1213Mib.interfaces_ifTable_ifOutOctets) 
                    ,('OutErrors' , Rfc1213Mib.interfaces_ifTable_ifOutErrors)                 
                ]
           )

    
    def getStorage(self):
        """
        Gets information about the host's storage, including disk drives and memory.
        """
        return self.reader.getTable(
                Rfc2790Mib.hrStorageTable,
                [
                    ('Descr'               , Rfc2790Mib.hrStorageDescr)
                    ,('AllocationUnits'     , Rfc2790Mib.hrStorageAllocationUnits)
                    ,('StorageSize'         , Rfc2790Mib.hrStorageSize)
                    ,('StorageUse'          , Rfc2790Mib.hrStorageUsed)
                    ,('AllocationFailuers'  , Rfc2790Mib.hrStorageAllocationFailures)
                ]
           )


    def getProcesses(self):
        """
        Gets information about processes currently running on the host.
        """
        runtable = self.reader.getTable(
                Rfc2790Mib.hrSWRunTable,
                [
                    ('RunIndex'      , Rfc2790Mib.hrSWRunIndex)
                    ,('RunName'       , Rfc2790Mib.hwSWRunName)
                    ,('RunID'         , Rfc2790Mib.hrSWRunID)
                    ,('RunPath'       , Rfc2790Mib.hrSWRunPath)
                    ,('RunParameters' , Rfc2790Mib.hrSWRunParameters)
                    ,('RunType'       , Rfc2790Mib.hrSWRunType)
                    ,('RunStatus'     , Rfc2790Mib.hrSWRunStatus)
                ],
                True
           )
        
        perftable = self.reader.getTable(
                Rfc2790Mib.hrSWRunPerfTable,
                [
                    ('CPU' , Rfc2790Mib.hrSWRunPerfCPU)
                    ,('Mem' , Rfc2790Mib.hrSWRunPerfMem)                ],
                True
            )

        # not sure why SWRunPerf and SWRun tables are separate in 
        # the MIB.  They make more sense as one happy table.  So we'll
        # stitch them together.
        ret = [];
        for rkey in runtable:
            row = runtable[rkey]
            if perftable.has_key(rkey):
                for pkey in perftable[rkey]:
                    row[pkey] = perftable[rkey][pkey]
            ret.append(row)
            
        return ret
        

class SnmpReader:
    """
    Reads common SNMP data from the specified host.  RFC1213 and Rfc2790
    MIBs are specifically targeted, more information available here 
    http://www.ietf.org/rfc/rfc1213.txt and also here
    http://portal.acm.org/citation.cfm?id=Rfc2790Mib  
    """            

    def __init__(self, host, port, agentName, communityName):
        self.agentName = agentName
        self.communityName = communityName
        self.host = host
        self.port = port
        self.supportsHR = self.get(('Rfc2790Test',Rfc2790Mib.hrSystemNumUsers)) != None
        self.supportsRfc1213 = self.get(('Rfc1213Test',Rfc1213Mib.system_sysDescr)) != None
        self.supportsPysnmp = PysnmpImported
 

 
    def get(self, field):
        """ 
        Gets an SNMP single value and converts it into a more mainstream
        value (i.e. gets rid of ANS1).
        """
        shot = self._get(field[1])
        if shot[1] != 0:
            return None
        return {field[0]:shot[3][0][1]._value}
    
    
        
    def _get(self, object):
        """ 
        Implements SNMP's get function.
        """
        errorIndication,    \
        errorStatus,        \
        errorIndex,         \
        varBinds = cmdgen.CommandGenerator().getCmd(
            cmdgen.CommunityData(self.agentName, self.communityName, 1),
            cmdgen.UdpTransportTarget((self.host, self.port)),
            object
        )
        return errorIndication, errorStatus, errorIndex, varBinds



    def getTable(self, tableOid, fields, includeId = False):
        """
        Gets an SNMP table value and converts it into a more mainstream
        value (i.e. gets rid of ASN1 and converts key-value pairs into
        a list of dictionaries.  Result should be JSON ready.)
        """
        
        table = self._getNext(tableOid)
        
        # SNMP can return non-sequential row numbers.  So we check which
        # rows are available explicitly.
        oid = list(fields[0][1])
        ids = []
        for row in table:
            if row[:-1] == fields[0][1]:
                ids.append(row[-1])

        # Actually put all the loose values into a real table.  
        if includeId == True:
            ret = {}
        else:
            ret = []
        for i in ids:
            row = {}
            for f in fields:
                oid = list(f[1])
                oid.append(i)
                if (table.has_key(tuple(oid))):
                    row[f[0]] = table[tuple(oid)]._value
            if includeId:
                ret[i] = row
            else:
                ret.append(row)
        return ret



    def _getNext(self, object):
        """
        Implements the SNMP getNext function.
        """
        errorIndication,    \
        errorStatus,        \
        errorIndex,         \
        varBinds = cmdgen.CommandGenerator().nextCmd(
            cmdgen.CommunityData(self.agentName, self.communityName, 1),
            cmdgen.UdpTransportTarget((self.host, self.port)),
            object
        )
        ret = {}
        for row in varBinds:
            ret[eval(str(row[0][0]))] = row[0][1]
        return ret



class Rfc2790Mib:
    """
    RFC 2790 MIB OIDs which of are interest to the OOICI project.
    """
    hrSystemUptime    = (1,3,6,1,2,1,25,1,1,0)
    hrSystemDate      = (1,3,6,1,2,1,25,1,2,0)
    hrSystemNumUsers  = (1,3,6,1,2,1,25,1,5,0)
    hrSystemProcesses = (1,3,6,1,2,1,25,1,6,0)

    hrStorageTable              = (1,3,6,1,2,1,25,2,3)
    hrStorageDescr              = (1,3,6,1,2,1,25,2,3,1,3)                            
    hrStorageAllocationUnits    = (1,3,6,1,2,1,25,2,3,1,4)
    hrStorageSize               = (1,3,6,1,2,1,25,2,3,1,5)
    hrStorageUsed               = (1,3,6,1,2,1,25,2,3,1,6)
    hrStorageAllocationFailures = (1,3,6,1,2,1,25,2,3,1,7)

    hrSWRunPerfTable = (1,3,6,1,2,1,25,5,1)
    hrSWRunPerfCPU   = (1,3,6,1,2,1,25,5,1,1,2)
    hrSWRunPerfMem   = (1,3,6,1,2,1,25,5,1,1,1)
                        
    hrSWRunTable =      (1,3,6,1,2,1,25,4,2,1)
    hrSWRunIndex =      (1,3,6,1,2,1,25,4,2,1,1)
    hwSWRunName =       (1,3,6,1,2,1,25,4,2,1,2)
    hrSWRunID =         (1,3,6,1,2,1,25,4,2,1,3)
    hrSWRunPath =       (1,3,6,1,2,1,25,4,2,1,4)
    hrSWRunParameters = (1,3,6,1,2,1,25,4,2,1,5)
    hrSWRunType =       (1,3,6,1,2,1,25,4,2,1,6)
    hrSWRunStatus =     (1,3,6,1,2,1,25,4,2,1,7)


    
class Rfc1213Mib:
    """
    RFC 1213 MIB OIDs which of are interest to the OOICI project.
    """
    system_sysDescr =     (1,3,6,1,2,1,1,1,0)
    system_sysUpTime =    (1,3,6,1,2,1,1,3,0)
    system_sysContact =   (1,3,6,1,2,1,1,4,0)
    system_sysName =      (1,3,6,1,2,1,1,5,0)
    system_sysLocation =  (1,3,6,1,2,1,1,6,0)

    interfaces_ifNumber = (1,3,6,1,2,1,2,1,0)
    interfaces_ifTable =  (1,3,6,1,2,1,2,2)
    interfaces_ifTable_ifDescr = (1,3,6,1,2,1,2,2,1,2)
    interfaces_ifTable_ifSpeed = (1,3,6,1,2,1,2,2,1,5)
    interfaces_ifTable_ifInOctets  = (1,3,6,1,2,1,2,2,1,10)
    interfaces_ifTable_ifInErrors  = (1,3,6,1,2,1,2,2,1,14)
    interfaces_ifTable_ifOutOctets = (1,3,6,1,2,1,2,2,1,16)
    interfaces_ifTable_ifOutErrors = (1,3,6,1,2,1,2,2,1,20)



client = HostStatusRPCClient()
server = HostStatusRPCServer()
# all = rpc.status.getAll()
#r = HostStatus('bfoxooi.ucsd.edu', 161, 'ccagent', 'ooicinet')
#all = json.dumps(r.getAll(), indent=4)
# print all