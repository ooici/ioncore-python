
"""
@file res/scripts/servicereg.py
@author Matt Rodriguez
@brief main module for bootstrapping dm pubsub services test
This script tests using the agent registry service with a Cassandra
backend in EC2.
"""

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap

from ion.services.coi import service_registry

from ion.play import hello_service

CONF = ioninit.config('startup.servicereg')



# Static definition of message queues
ion_messaging = ioninit.get_config('messaging_cfg', CONF)

# Static definition of service names
coi_services = ioninit.get_config('services_cfg', CONF)

@defer.inlineCallbacks
def start():
    logging.info("ION AgentRegistry bootstrapping now...")
    startsvcs = []
    startsvcs.extend(coi_services)
    #startsvcs.extend(dm_services)
    sup = yield bootstrap.bootstrap(ion_messaging, startsvcs)

    logging.info('STARTSVCS ' + str(startsvcs))
    logging.info('ION_MESSAGING' + str(ion_messaging))
    logging.info('CONT_ARGS' + str(ioninit.cont_args))

    service_reg_client = service_registry.ServiceRegistryClient(proc=sup)

    play_desc = yield service_reg_client.register_service_definition(hello_service.HelloService)
    ref = play_desc.reference()
    svc_desc = yield service_reg_client.get_service_definition(ref)
    yield service_reg_client.clear_registry()



start()
