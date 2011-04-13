"""

@file ion/play/identservice.py
@author Roger Unwin with help from Dorian Raymer
@brief service for registering and authenticating identities via a web interface


run this from lcaarch like this:
    twistd -n cc -n -h amoeba.ucsd.edu  ion/play/identservice.py
"""

from twisted.internet import defer
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from ion.core import bootstrap
from ion.data import dataobject
from ion.resources import coi_resource_descriptions
from ion.resources import description_utility

dataobject.DataObject._types['IdentityResource'] = coi_resource_descriptions.IdentityResource


@defer.inlineCallbacks
def main():
    log.debug('Starting')
    description_utility.load_descriptions()
    bootstrap._set_container_args("{'sys-name':'mysys'}")
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

"""
main() has to be called on start. this is a mated pair with identclient.py if you are going
to alter this line then justify yourself to Roger Unwin.
"""
#main()
