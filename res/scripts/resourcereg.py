
"""
@file res/scripts/resourcereg.py
@author Matt Rodriguez
@brief main module for bootstrapping resource services test
This script tests using the agent registry service with a Cassandra
backend in EC2.
"""

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap

from ion.services.coi import resource_registry
from ion.resources import coi_resource_descriptions


CONF = ioninit.config('startup.resourcereg')


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

    resource_reg_client = resource_registry.ResourceRegistryClient(proc=sup)


    res_to_describe = coi_resource_descriptions.IdentityResource

        # Registration creates the resource description from the class object
    res_description = yield resource_reg_client.register_resource_definition(res_to_describe)


    yield resource_reg_client.clear_registry()



start()
