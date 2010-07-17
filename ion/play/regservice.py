
from twisted.internet import defer
from twisted.python import log

from ion.core import bootstrap

@defer.inlineCallbacks
def main():
    bootstrap._set_container_args("{'sys-name':'mysys'}")
    messaging = {'registry':{'name_type':'worker', 'args':{'scope':'system'}}}
    yield bootstrap.declare_messaging(messaging)
    services = [
            {
                'name':'registry', 
                'module':'ion.services.coi.resource_registry',
                'class':'ResourceRegistryService',
                'spawnargs':{
                    'sys-name':'mysys',
                    'servicename':'registry',
                    'scope':'system'
                    }
                }
            ]
    yield bootstrap.spawn_processes(services)

main()

