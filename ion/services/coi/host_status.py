import json

try:
    from pysnmp.entity.rfc3413.oneliner import cmdgen
    PysnmpImported = True
except ImportError:
    PysnmpImported = False



      
class HostStatus:
    
    def __init__(self, host, port, agentName, communityName):
        self.reader = SnmpReader(host, port, agentName, communityName)
        self.refresh()
        
    def refresh(self):
        pass

    
    def getNetworkInterfaces(self):
        return self.reader.getTable(
                Rfc1213Mib.interfaces_ifTable,
                [
                    ('Descr'     , Rfc1213Mib.interfaces_ifTable_ifDescr),
                    ('Speed'     , Rfc1213Mib.interfaces_ifTable_ifSpeed),
                    ('InOctets'  , Rfc1213Mib.interfaces_ifTable_ifInOctets),
                    ('InErrors'  , Rfc1213Mib.interfaces_ifTable_ifInErrors),
                    ('OutOctets' , Rfc1213Mib.interfaces_ifTable_ifOutOctets), 
                    ('OutErrors' , Rfc1213Mib.interfaces_ifTable_ifOutErrors)                 
                ]
           )
    
    def getStorage(self):
        return self.reader.getTable(
                Rfc2790.hrStorageTable,
                [
                    ('Descr'               , Rfc2790.hrStorageDescr),
                    ('AllocationUnits'     , Rfc2790.hrStorageAllocationUnits),
                    ('StorageSize'         , Rfc2790.hrStorageSize),
                    ('StorageUse'          , Rfc2790.hrStorageUsed),
                    ('AllocationFailuers'  , Rfc2790.hrStorageAllocationFailures)
                ]
           )


class SnmpReader:
    """
    Reads common SNMP data from the specified host.  RFC1213 and RFC2790 
    MIBs are specifically targeted, more information available here 
    http://www.ietf.org/rfc/rfc1213.txt and also here
    http://portal.acm.org/citation.cfm?id=RFC2790  
    """            

    def __init__(self, host, port, agentName, communityName):
        self.agentName = agentName
        self.communityName = communityName
        self.host = host
        self.port = port
        self.supportsHR = self.get(('Rfc2790Test',Rfc2790.hrSystemNumUsers)) != None
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



    def getTable(self, tableOid, fields):
        """
        Gets an SNMP table value and converts it into a more mainstream
        value (i.e. gets rid of ASN1 and converts key-value pairs into
        a list of dictionaries.  Result should be JSON ready.
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
        ret = []
        for i in ids:
            row = {}
            for f in fields:
                oid = list(f[1])
                oid.append(i)
                if (table.has_key(tuple(oid))):
                    row[f[0]] = table[tuple(oid)]._value
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



class Rfc2790:
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




r = HostStatus('bfoxooi.ucsd.edu', 161, 'ccagent', 'ooicinet')

print r.reader.supportsPysnmp
print r.reader.supportsRfc1213
print r.reader.supportsHR

netif = json.dumps(r.getNetworkInterfaces(), indent=4)
print netif

storage = json.dumps(r.getStorage(), indent=4)
print storage
