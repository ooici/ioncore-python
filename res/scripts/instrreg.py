
"""
@file res/scripts/instrreg.py
@author Matt Rodriguez
@brief main module for bootstrapping instrument registry services test
This script tests using the agent registry service with a Cassandra
backend in EC2.
"""

import logging
from twisted.internet import defer

#from ion.core.base_process import BaseProcess, ProcessDesc
from ion.core import ioninit
from ion.core import bootstrap

from ion.services.sa import instrument_registry
from ion.resources import sa_resource_descriptions


CONF = ioninit.config('startup.instrreg')



# Static definition of message queues
ion_messaging = ioninit.get_config('messaging_cfg', CONF)

# Static definition of service names
sa_services = ioninit.get_config('services_cfg', CONF)

@defer.inlineCallbacks
def start():
    logging.info("ION AgentRegistry bootstrapping now...")
    startsvcs = []
    startsvcs.extend(sa_services)
    #startsvcs.extend(dm_services)
    sup = yield bootstrap.bootstrap(ion_messaging, startsvcs)
        
    logging.info('STARTSVCS ' + str(startsvcs))
    logging.info('ION_MESSAGING' + str(ion_messaging))
    logging.info('CONT_ARGS' + str(ioninit.cont_args))
    
    instrument_reg_client = instrument_registry.InstrumentRegistryClient(proc=sup)
    
    
    res = sa_resource_descriptions.InstrumentResource.create_new_resource()
    res = yield instrument_reg_client.register_instrument_type(res)

    yield instrument_reg_client.clear_registry()



start()
