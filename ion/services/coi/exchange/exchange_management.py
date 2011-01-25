import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.services.coi.exchange.exchange_registry import ExchangeRegistryClient

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
from twisted.internet import defer


CONF = ioninit.config(__name__)

class ExchangeManagementService(ServiceProcess):

    declare = ServiceProcess.service_declare(name='exchange_management',
                                          version='0.1.0',
                                          dependencies=[])

    def slc_init(self):
        self.erc = ExchangeRegistryClient(proc=self)
        


class ExchangeManagementClient(ServiceClient):
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "exchange_registry"
        ServiceClient.__init__(self, proc, **kwargs)


factory = ProcessFactory(ExchangeManagementService)