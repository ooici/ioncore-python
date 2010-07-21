"""
@file ion/services/coi/hostsensor/host_status_client.py
@author Brian Fox
@brief Simple XMLRPC client for retrieving host status
"""

import xmlrpclib, json

s = xmlrpclib.ServerProxy('http://localhost:9010')
print json.dumps(s.getStatus(), indent=4)
print s.system.listMethods()