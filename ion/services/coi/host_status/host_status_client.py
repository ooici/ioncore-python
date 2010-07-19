import xmlrpclib

s = xmlrpclib.ServerProxy('http://localhost:9010')
print s.getStatus()

# Print list of available methods
print s.system.listMethods()