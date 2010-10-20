
"""
@file res/scripts/prodreg.py
@author Matt Rodriguez
@brief main module for bootstrapping product registry services test
This script tests using the agent registry service with a Cassandra
backend in EC2.
"""

import logging
from twisted.internet import defer

#from ion.core.base_process import BaseProcess, ProcessDesc
from ion.core import ioninit
from ion.core import bootstrap

from ion.services.sa import data_product_registry
from ion.resources import sa_resource_descriptions

CONF = ioninit.config('startup.prodreg')



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
    
    product_reg_client = data_product_registry.DataProductRegistryClient(proc=sup)
    i = 0
    logging.info("Starting registry test")
    while i < 1:
        res = sa_resource_descriptions.DataProductResource.create_new_resource()
        res = yield product_reg_client.register_data_product(res)
        i = i + 1
        logging.info(str(i))
    logging.info("Ending registry test")    
    yield product_reg_client.clear_registry()



start()
