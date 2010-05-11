
import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config
import ion.util.procutils as pu

CONF = ioninit.config('ion.core.bootstrap1')

# Static definition of message queues
ion_messaging = Config(CONF.getValue('messaging_cfg')).getObject()

# Static definition of service names
ion_core_services = Config(CONF.getValue('coreservices_cfg')).getObject()
ion_services = Config(CONF.getValue('services_cfg')).getObject()


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
