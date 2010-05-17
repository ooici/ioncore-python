
import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap

CONF = ioninit.config('startup.bootstrap1')

# Static definition of message queues
ion_messaging = ioninit.get_config('messaging_cfg', CONF)

# Static definition of service names
ion_core_services = ioninit.get_config('coreservices_cfg', CONF)
ion_services = ioninit.get_config('services_cfg', CONF)


@defer.inlineCallbacks
def main():
    """Main function of bootstrap. Starts system with static config
    """
    logging.info("ION SYSTEM bootstrapping now...")
    startsvcs = []
    #startsvcs.extend(ion_core_services)
    startsvcs.extend(ion_services)

    yield bootstrap.bootstrap(ion_messaging, startsvcs)

main()
