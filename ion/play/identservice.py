"""
run this from lcaarch like this:
    twistd -n magnet -h amoeba.ucsd.edu ion/play/identservice.py
"""

from twisted.internet import defer
from twisted.python import log

from ion.core import bootstrap
from ion.data import dataobject
from ion.resources import coi_resource_descriptions
dataobject.DataObject._types['IdentityResource'] = coi_resource_descriptions.IdentityResource
                                                   
@defer.inlineCallbacks
def main():
    
    bootstrap._set_container_args("{'sys-name':'mysys'}")
    messaging = {'identity':{'name_type':'worker', 'args':{'scope':'system'}}}
    yield bootstrap.declare_messaging(messaging)
    services = [
            {
                'name':'identity', 
                'module':'ion.services.coi.identity_registry',
                'class':'IdentityRegistryService',
                'spawnargs':{
                    'sys-name':'mysys',
                    'servicename':'identity',
                    'scope':'system'
                    }
                }
            ]
    yield bootstrap.spawn_processes(services)

main()

