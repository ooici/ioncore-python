
"""
@file res/scripts/agentreg.py
@author Matt Rodriguez
@brief main module for bootstrapping agent registry services test
This script tests using the agent registry service with a Cassandra
backend in EC2.
"""

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap

from ion.services.coi import agent_registry
from ion.resources.ipaa_resource_descriptions \
    import InstrumentAgentResourceDescription


CONF = ioninit.config('startup.agentreg')



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

    agent_reg_client = agent_registry.AgentRegistryClient(proc=sup)

    res_desc = \
            InstrumentAgentResourceDescription.create_new_resource()
    logging.info("resc_desc: " + str(res_desc))
    registered_agent_desc = yield agent_reg_client.register_agent_definition(res_desc)
    logging.info("registered_agent_desc: " +  str(registered_agent_desc))

    agent_reg_client.clear_registry()




start()
