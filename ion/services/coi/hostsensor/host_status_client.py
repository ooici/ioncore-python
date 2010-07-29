"""
@file ion/services/coi/hostsensor/host_status_client.py
@author Brian Fox
@brief Simple XMLRPC client for retrieving host status
"""

from twisted.web import xmlrpc
s = xmlrpc.Proxy('http://localhost:9010')
r = s.callRemote("getStatusPrettyPrint")
print r