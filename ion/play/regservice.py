
from twisted.internet import defer
from twisted.python import log

from ion.core import bootstrap
from ion.resources import description_utility

from ion.resources import description_utility
description_utility.load_descriptions()

@defer.inlineCallbacks
def main():
    bootstrap._set_container_args("{'sys-name':'mysys'}")
    messaging = {'registry':{'name_type':'worker', 'args':{'scope':'system'}}}

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
    yield bootstrap.bootstrap(messaging, services)

main()
