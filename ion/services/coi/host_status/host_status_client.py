import xmlrpclib, json

s = xmlrpclib.ServerProxy('http://localhost:9010')
print json.dumps(s.getStatus(), indent=4)

# Print list of available methods
print s.system.listMethods()