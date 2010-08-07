"""
@file ion/services/coi/hostsensor/readers.py
@author Brian Fox
@brief Various readers for retrieving host status
"""

### This module requires pyasn1 and pysnmp


import os,datetime,operator    
import logging

try:
    from pysnmp.entity.rfc3413.oneliner import cmdgen
    PysnmpImported = True
except ImportError:
    PysnmpImported = False


class SnmpReaderException(Exception):
    """
    Differentiates exceptions that the SnmpReader might raise.
    """
    pass


class HostReader:
    """
    Reads the status of the local host as retrieved using
    SNMP and RFC1213 and RFC2790 SNMP MIB definitions as
    well as local API support for machine status.
    """

    def __init__(
                 self, 
                 host, 
                 port, 
                 agentName, 
                 communityName, 
                 timeout=1.5, 
                 retries=3 
                 ):
        self.timeout = timeout
        self.retries = retries
        self.reader = SnmpReader(
                                 host, 
                                 port, 
                                 agentName, 
                                 communityName, 
                                 timeout, 
                                 retries
                                 )

       
    def get(self, subsystem):
        """
        Produces a dictionary for the specified subsystem.  Valid subsystems
        may be a string ('all','base','network','storage','cpu','python','java') 
        or a list of said strings.
        """
        ret = {}
 
        if (subsystem in ['base','all']):       
            ret['base'] = self._getBase()
        if (subsystem in ['network','all']):       
            ret['network'] = self._getNetworkInterfaces()
        if (subsystem in ['storage','all']):       
            ret['storage'] = self._getStorage()
        if (subsystem in ['cpu','all']):       
            ret['cpu'] = self._getProcesses()
            
        return ret




    def pformat(self, subsystem):
        """
        Produces a dictionary for the specified subsystem.  Valid subsystems
        may be a string ('all','base','network','storage','cpu','python','java') 
        or a list of said strings.
        """
        ret = {}
 
        if (subsystem in ['base','all']):       
            ret['base'] = self._getBase()
        if (subsystem in ['network','all']):       
            ret['network'] = self._getNetworkInterfaces()
        if (subsystem in ['storage','all']):       
            ret['storage'] = self._getStorage()
        if (subsystem in ['cpu','all']):       
            ret['cpu'] = self._getProcesses()
        return SnmpReader.pformat(ret)    

    
    
    def _getBase(self):
        """
        Gets basic machine information.
        """
        uname = os.uname()

        ret = {}
        ret['SupportsSNMP']      = self.reader.supportsSNMP()
        ret['SupportsRFC1213']   = self.reader.supportsRFC1213()
        ret['SupportsRFC2790']   = self.reader.supportsRFC2790()
        ret['LocalTime']         = datetime.datetime.today().isoformat()

    
        ret['rfc1213_SystemDescr']    = self.reader.get(Rfc1213Mib.system_sysDescr)
        ret['rfc1213_SystemContact']  = self.reader.get(Rfc1213Mib.system_sysContact)
        ret['rfc1213_SystemName']     = self.reader.get(Rfc1213Mib.system_sysName)
        ret['rfc1213_SystemLocation'] = self.reader.get(Rfc1213Mib.system_sysLocation)
        ret['rfc1213_UpTime']         = self.reader.get(Rfc1213Mib.system_sysUpTime)
        ret['rfc2790_UpTime'] = self.reader.get(Rfc2790Mib.hrSystemUptime)

        return ret

    def _getPython(self):
        ret = {}
        ret['python_SystemName'] = uname[0]
        ret['python_NodeName']   = uname[1]
        ret['python_Release']    = uname[2]
        ret['python_Version']    = uname[3]
        ret['python_Machine']    = uname[4]


    def _getNetworkInterfaces(self):
        """
        Gets information about the host's network interfaces.
        """
        source = 'rfc1213_mib'
        fields = [                
                    Rfc1213Mib.interfaces_ifTable_ifDescr,
                    Rfc1213Mib.interfaces_ifTable_ifSpeed,
                    Rfc1213Mib.interfaces_ifTable_ifInOctets,
                    Rfc1213Mib.interfaces_ifTable_ifInErrors,
                    Rfc1213Mib.interfaces_ifTable_ifOutOctets, 
                    Rfc1213Mib.interfaces_ifTable_ifOutErrors                 
                 ]
        cols = []
        for f in fields:
            cols.append(f[1])
        rows = self.reader.getTable(Rfc1213Mib.interfaces_ifTable, fields)

        return {
                'source'  : source,
                'cols' : cols,
                'rows' : rows
                }
                

    
    def _getStorage(self):
        """
        Gets information about the host's storage, including disk drives and memory.
        """
        source = 'rfc2790_mib'
        fields = [                
                    Rfc2790Mib.hrStorageDescr,                   
                    Rfc2790Mib.hrStorageAllocationUnits,
                    Rfc2790Mib.hrStorageSize,
                    Rfc2790Mib.hrStorageUsed,
                    Rfc2790Mib.hrStorageAllocationFailures          
                ]
        cols = []
        for f in fields:
            cols.append(f[1])
        rows = self.reader.getTable(Rfc2790Mib.hrStorageTable, fields)

        return {
                'source'  : source,
                'cols' : cols,
                'rows' : rows
                }


    def _getProcesses(self):
        """
        Gets information about processes currently running on the host.
        """
        
        # This is really a two-part table that needs to be assembled.
        # We'll be pulling data from two distinct snmp tables.

        source = 'rfc1213_mib'
        oid1 = Rfc2790Mib.hrSWRunTable
        fields1 = [                
                    Rfc2790Mib.hrSWRunIndex,
                    Rfc2790Mib.hrSWRunName,
                    Rfc2790Mib.hrSWRunID,
                    Rfc2790Mib.hrSWRunPath,
                    Rfc2790Mib.hrSWRunParameters,
                    Rfc2790Mib.hrSWRunType,
                    Rfc2790Mib.hrSWRunStatus
                 ]
        oid2 =     Rfc2790Mib.hrSWRunPerfTable
        fields2 = [                
                    Rfc2790Mib.hrSWRunPerfCPU,
                    Rfc2790Mib.hrSWRunPerfMem
                 ]

        cols = []
        for f in fields1:
            cols.append(f[1])
        for f in fields2:
            cols.append(f[1])
            
        rows = self.reader.stitchTables([oid1,oid2], [fields1,fields2])

        return {
            'source' : 'RFC2790MIB',
            'cols' : cols,
            'rows' : rows
            }

    @staticmethod
    def _isTable(object):
        return \
            isinstance(object,dict) \
            and object.has_key('cols') \
            and object.has_key('rows') 


    @staticmethod
    def pprint(report):
        """
        Prints a pretty string representation of get(subsystem)
        """ 
        print(HostReader.pformat(report)) 

    @staticmethod
    def pformat(report):
        if not isinstance(report,dict):
            return "Object (type: %s) does not seem" + \
                " to be a proper report.\n%s"          \
                %(type(report).__name__,str(object))
        ret = ""
        for key in report:
            val = report[key]
            if HostReader._isTable(val):
                ret += str(HostReader._pftable(key,val))
            else: 
                ret += str(HostReader._pfother(key,val))
        return ret


    @staticmethod
    def _pftable(name,table):
        col_len = []
        col_names = table['cols']
        align = []

        for name in col_names:
            col_len.append(len(name))
            align.append('')

        for row in table['rows']:
            for i in range(0,len(row)):
                col_len[i] = max(col_len[i],len(str(row[i])))
                if not str(row[i]).isdigit():
                    align[i] = '-'

        rowformat = ''
        headerformat = ''
        colsep = ''
        for i in range(0,len(col_names)):
            rowformat = rowformat + '%' + align[i] + str(col_len[i]) + 's  '
            headerformat = headerformat + '%-' + str(col_len[i]) + 's  '
            colsep = colsep + '-'*col_len[i]+'  '
        ret = 'TABLE: %s\n\n'%name
        ret += headerformat%tuple(col_names) + '\n'
        ret += colsep + '\n'
        for row in table['rows']:
            ret += rowformat%tuple(row) + '\n'
        ret += '\n'    
        return ret    


            
    @staticmethod
    def _pfother(name,val):
        if (isinstance(val,dict)):
            ret = 'HASH: %s\n\n'%name
            keylen = 0
            vallen = 0
            skeys = []
            for key in val:
                skeys.append(key)
                keylen = max(keylen,len(str(key)))
                vallen = max(vallen,len(str(val[key])))
            formatstr = '%-' + str(keylen + 1) + 's   %-' + str(vallen) + 's\n'
            skeys.sort()
            for key in skeys:
                ret += formatstr%(key + ':',val[key]) 
            ret += '\n\n'
        if (isinstance(val,list)):
            ret = 'LIST: %s\n\n'%name
            ret += str(val) + '\n'
        return ret    
 



class SnmpReader:
    """
    Reads common SNMP data from the specified host.  RFC1213 and Rfc2790
    MIBs are specifically targeted, more information available here 
    http://www.ietf.org/rfc/rfc1213.txt and also here
    http://portal.acm.org/citation.cfm?id=Rfc2790Mib  
    """            

    def __init__(self, host, port, agentName, communityName, timeout=1.5, retries=3):
        self.agentName = agentName
        self.communityName = communityName
        self.host = host
        self.port = port
        self.timeout = timeout
        self.retries = 3
        self._supportsSNMP = True
        logging.debug('Supports Pysnmp - ' + str(PysnmpImported))
        if not PysnmpImported:
            self._supportsPysnmp = False
            self._supportsRfc2790 = False
            self._supportsRfc1213 = False
        else:    
            self._supportsRfc2790 = self.get(Rfc2790Mib.hrSystemNumUsers) != None
            self._supportsRfc1213 = self.get(Rfc1213Mib.system_sysDescr) != None
            self._supportsPysnmp = True
        self._supportsSNMP = PysnmpImported and (self._supportsRfc1213 or self._supportsRfc2790)  
 

    def supportsSNMP(self):
        return self._supportsSNMP

    def supportsRFC2790(self):
        return self._supportsRfc2790
    
    def supportsRFC1213(self):
        return self._supportsRfc1213
    
    
    def _toTuple(self, oid):
        array = []
        for dec in oid[1:].split("."):
            array.append(int(dec))
        return tuple(array)
            
            
    def get(self, oid):
        """ 
        Gets an SNMP single value and converts it into a more mainstream
        value (i.e. gets rid of ASN.1).
        """
        if not self._supportsSNMP:
            return None
        
        tupleoid = self._toTuple(oid[0])
        shot = self._get(tupleoid)
        try:
            return shot[3][0][1]._value
        except:
            return None

    
    def stitchTables(self, tableOidList, fieldsList):
        """
        Stitches multiple tables together joined by table ids
        """
        # This is a bit tricky.  Since some SNMP tables are related by ids,
        # but the ids are fleeting and very temporal, the stitching has to
        # assume information will be missing.
        #
        # The stitch then becomes something like a SQL full outer join
        # Weird errors?  Look here first.


        # query all the tables        
        tables = []
        for i in range(0,len(tableOidList)):
            next = self.getTable(tableOidList[i],fieldsList[i],includeId=True)
            tables.append(next)

        # this is our working value, a hash for easy id retrieval
        work = {}

        # we're going to need this to properly pad new rows
        totalFields = 0
        for fields in fieldsList:
            totalFields += len(fields)

        # simple offset
        currentField = 0
        for i in range(0,len(tables)):
            for id in tables[i]:

                # Create a fully populated row of Nones.
                if not work.has_key(id):
                    newrow = []
                    for j in range(0,totalFields):
                        newrow.append(None)
                    work[id] = newrow

                # Populate data now that a row is guaranteed to exist    
                for j in range(0,len(tables[i][id])):
                    work[id][j + currentField] = tables[i][id][j]
                
            currentField += len(fieldsList[i])

        # Strip off the ids, they're no longer necessary            
        final = []
        for key in work:
            final.append(work[key])

        return final
        
        
        
    def getTable(self, tableOid, fields, includeId = False):
        """
        Gets an SNMP table and converts it into a more mainstream
        value (i.e. gets rid of ASN.1 and converts key-value pairs into
        a list of dictionaries.  Result should be JSON ready.)
        """        
        if not self._supportsSNMP:
            return []

        # MIB OID manipulation.  
        tupleoid = self._toTuple(tableOid[0])
        tuplefields = []
        for field in fields:
            tuplefields.append(self._toTuple(field[0])) 
        
        table = self._getNext(tupleoid)
        
        
        # SNMP can return non-sequential row numbers.  So we check which
        # rows are available explicitly.
        ids = set()
        for row in table:
            if row[:-1] in tuplefields:
                ids.add(row[-1])
        ids = list(ids)
        ids.sort()
        
        
        # Assemble the table in a more user-friendly format than the
        # oid.row = value model that snmp uses. 
        if includeId:
            ret = {}
        else:
            ret = []
        
        for i in ids:
            row = []
            for field in tuplefields:
                # table keys are made up of the table field oid plus
                # the row number.
                key =  list(field)
                key.append(i)
                key = tuple(key)
                
                if table.has_key(key):
                    row.append(table[key]._value)
                else:
                    # place holder   
                    row.append(None)

            if includeId:
                ret[i] = row
            else:    
                ret.append(row)

        return ret



    def _get(self, object):
        """ 
        Implements SNMP's get function.
        """
        
        errorIndication,    \
        errorStatus,        \
        errorIndex,         \
        varBinds = cmdgen.CommandGenerator().getCmd(
            cmdgen.CommunityData(self.agentName, self.communityName, 1),
            cmdgen.UdpTransportTarget(
                                      (self.host, self.port), 
                                      timeout=self.timeout, 
                                      retries=self.retries ),
            object
        )
        return errorIndication, errorStatus, errorIndex, varBinds



    def _getNext(self, object):
        """
        Implements the SNMP getNext function.
        """
        errorIndication,    \
        errorStatus,        \
        errorIndex,         \
        varBinds = cmdgen.CommandGenerator().nextCmd(
            cmdgen.CommunityData(self.agentName, self.communityName, 1),
            cmdgen.UdpTransportTarget((self.host, self.port), timeout=self.timeout, retries=self.retries),
            object
        )
        ret = {}
        for row in varBinds:
            ret[eval(str(row[0][0]))] = row[0][1]
        return ret



# A note about OIDS
# -----------------
# The MIB OIDs quickly become a headache.  Should they be represented 
# as a list, a string, or a tuple?
#  
# list -   most versatile and easiest to manipulate in python
# string - most common representation for the world of snmp 
# tuple -  pysnmp's preferred format, required
#
# This will be a constant struggle.  In the end, we're going to use the
# string representation.  Manipulations will involve a conversion to 
# list (for appending) and ultimately to tuple (because pysnmp said so).
# The processor hit should be very manageable but if not, it can be 
# addressed later.

class Rfc2790Mib:
    """
    RFC 2790 MIB OIDs which of are interest to the OOICI project.
    """
    
    hrSystemUptime    = ('.1.3.6.1.2.1.25.1.1.0', 'SystemUptime')
    hrSystemDate      = ('.1.3.6.1.2.1.25.1.2.0', 'SystemDate')
    hrSystemNumUsers  = ('.1.3.6.1.2.1.25.1.5.0', 'SystemNumUsers')
    hrSystemProcesses = ('.1.3.6.1.2.1.25.1.6.0', 'SystemProcesses')

    hrStorageTable              = ('.1.3.6.1.2.1.25.2.3', 'StorageTable')
    hrStorageDescr              = ('.1.3.6.1.2.1.25.2.3.1.3', 'StorageDesc')                            
    hrStorageAllocationUnits    = ('.1.3.6.1.2.1.25.2.3.1.4', 'StorageAllocationUnits')
    hrStorageSize               = ('.1.3.6.1.2.1.25.2.3.1.5', 'StorageSize')
    hrStorageUsed               = ('.1.3.6.1.2.1.25.2.3.1.6', 'StorageUsed')
    hrStorageAllocationFailures = ('.1.3.6.1.2.1.25.2.3.1.7', 'StorageAllocationFailures')

    hrSWRunPerfTable = ('.1.3.6.1.2.1.25.5.1', 'SWRunPerfTable')
    hrSWRunPerfCPU   = ('.1.3.6.1.2.1.25.5.1.1.2', 'SWRunPerfCPU')
    hrSWRunPerfMem   = ('.1.3.6.1.2.1.25.5.1.1.1', 'SWRunPerfMem')
                        
    hrSWRunTable =      ('.1.3.6.1.2.1.25.4.2.1',   'SWRunTable')
    hrSWRunIndex =      ('.1.3.6.1.2.1.25.4.2.1.1', 'SWRunIndex')
    hrSWRunName =       ('.1.3.6.1.2.1.25.4.2.1.2', 'SWRunName')
    hrSWRunID =         ('.1.3.6.1.2.1.25.4.2.1.3', 'SWRunID')
    hrSWRunPath =       ('.1.3.6.1.2.1.25.4.2.1.4', 'SWRunPath')
    hrSWRunParameters = ('.1.3.6.1.2.1.25.4.2.1.5', 'SWRunParameters')
    hrSWRunType =       ('.1.3.6.1.2.1.25.4.2.1.6', 'SWRunType')
    hrSWRunStatus =     ('.1.3.6.1.2.1.25.4.2.1.7', 'SWRunStatus')

    
class Rfc1213Mib:
    """
    RFC 1213 MIB OIDs which of are interest to the OOICI project.
    """
    system_sysDescr =     ('.1.3.6.1.2.1.1.1.0', 'SysDesc')
    system_sysUpTime =    ('.1.3.6.1.2.1.1.3.0', 'SysUpTime')
    system_sysContact =   ('.1.3.6.1.2.1.1.4.0', 'SysContact')
    system_sysName =      ('.1.3.6.1.2.1.1.5.0', 'SysName')
    system_sysLocation =  ('.1.3.6.1.2.1.1.6.0', 'SysLocation')

    interfaces_ifNumber = ('.1.3.6.1.2.1.2.1.0', 'IfNumber')
    interfaces_ifTable =  ('.1.3.6.1.2.1.2.2',   'IfTable')
    interfaces_ifTable_ifDescr = ('.1.3.6.1.2.1.2.2.1.2',      'IfDescr')
    interfaces_ifTable_ifSpeed = ('.1.3.6.1.2.1.2.2.1.5',      'IfSpeed')
    interfaces_ifTable_ifInOctets  = ('.1.3.6.1.2.1.2.2.1.10', 'IfInOctets')
    interfaces_ifTable_ifInErrors  = ('.1.3.6.1.2.1.2.2.1.14', 'IfInErrors')
    interfaces_ifTable_ifOutOctets = ('.1.3.6.1.2.1.2.2.1.16', 'IfOutOctets')
    interfaces_ifTable_ifOutErrors = ('.1.3.6.1.2.1.2.2.1.20', 'IfOutErrors')
