
from twisted.internet import defer
from twisted.python import log

from magnet.container import Container

from ion.services.coi import resource_registry

@defer.inlineCallbacks
def main(ns={}):
    from ion.data.datastore import registry
    yield Container.configure_messaging('mysys.registry', {'name_type':'worker', 'args':{'scope':'system'}, 'scope':'system'})
    client = resource_registry.ResourceRegistryClient(target='mysys.registry')
    rd = registry.ResourceDescription()
    rd.name = 'thing'
    yield client.register_resource('123', rd)
    ns.update(locals())


