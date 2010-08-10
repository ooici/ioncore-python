
from twisted.internet import defer
from twisted.python import log

from ion.core import bootstrap

from ion.resources import description_utility
description_utility.load_descriptions()

@defer.inlineCallbacks
def main():
    bootstrap._set_container_args("{'sys-name':'mysys'}")
    messaging = {'registry':{'name_type':'worker', 'args':{'scope':'system'}}}
    yield bootstrap.declare_messaging(messaging)
    services = [
            {
                'name':'registry', 
                'module':'ion.data.datastore.registry',
                'class':'RegistryService',
                'spawnargs':{
                    'sys-name':'mysys',
                    'servicename':'registry',
                    'scope':'system'
                    }
                }
            ]
    yield bootstrap.spawn_processes(services)

main()

