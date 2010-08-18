# Python Capability Container start script.
# Starts container with Java Services.

import logging
from twisted.internet import defer

from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator
from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config
from ion.resources.sa_resource_descriptions import InstrumentResource, DataProductResource
from ion.services.sa.instrument_registry import InstrumentRegistryClient
from ion.services.sa.data_product_registry import DataProductRegistryClient

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('ccagent_cfg', CONF)
demo_procs = [
    {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
    {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'},
    {'name':'agent_registry','module':'ion.services.coi.agent_registry','class':'ResourceRegistryService'},

    {'name':'instrument_registry','module':'ion.services.sa.instrument_registry','class':''},
    {'name':'data_product_registry','module':'ion.services.sa.data_product_registry','class':''},
    {'name':'instrument_management','module':'ion.services.sa.instrument_management','class':''},
    {'name':'service_registry','module':'ion.services.coi.service_registry','class':''},
    {'name':'registry','module':'ion.data.datastore.registry','class':'RegistryService', 'spawnargs':{'servicename':'registry'}},

    {'name':'javaint','module':'ion.demo.lca.javaint_service','class':'JavaIntegrationService'},
]

INSTRUMENT_ID  = "123"


@defer.inlineCallbacks
def main():
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing... [LCA Java Integration Demo]")

    processes = []
    processes.extend(agent_procs)
    processes.extend(demo_procs)

    # Start the processes
    sup = yield bootstrap.bootstrap(None, processes)

    simulator = Simulator(INSTRUMENT_ID, 9000)
    simulator.start()

    irc = InstrumentRegistryClient(proc=sup)

    ir1 = InstrumentResource.create_new_resource()
    ir1.serial_num = "12345"
    ir1.model = "SBE49"
    ir1 = yield irc.register_instrument_instance(ir1)
    ir1_ref = ir1.reference(head=True)

    ir2 = InstrumentResource.create_new_resource()
    ir2.serial_num = "54322"
    ir2.model = "SBE49"
    ir2 = yield irc.register_instrument_instance(ir2)

    dprc = DataProductRegistryClient(proc=sup)

    dp1 = DataProductResource.create_new_resource()
    dp1.dataformat = "binary"
    dp1.instrument_ref = ir1_ref
    dp1 = yield dprc.register_data_product(dp1)

    ia_procs = [
        {'name':'SBE49IA','module':'ion.agents.instrumentagents.SBE49_IA','class':'SBE49InstrumentAgent','spawnargs':{'instrument-id':INSTRUMENT_ID}},
    ]
    yield bootstrap.spawn_processes(ia_procs, sup=sup)

    ia_pid = sup.get_child_id('SBE49IA')
    iaclient = InstrumentAgentClient(proc=sup,target=ia_pid)
    yield iaclient.register_resource(INSTRUMENT_ID)


main()
