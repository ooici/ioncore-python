# Python Capability Container start script.
# Starts container with Java Services.

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

ais_svcs = [
    {'name':'app_integration','module':'ion.integration.ais.app_integration_service','class':'AppIntegrationService'}
]

@defer.inlineCallbacks
def main():
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing... [AIS Test]")

    services = []
    services.extend(ais_svcs)

    # Start the processes
    sup = yield bootstrap.bootstrap(None, services)

main()
