
from twisted.internet import defer

from ion.core import ioninit
from ion.core.cc.container import Container
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.resources import description_utility
description_utility.load_descriptions()

@defer.inlineCallbacks
def main(ns={}):
    from ion.data.datastore import registry
    from ion.data.dataobject import Resource
    yield ioninit.container_instance.configure_messaging('mysys.registry', {'name_type':'worker', 'args':{'scope':'system'}, 'scope':'system'})
    client = registry.RegistryClient(target='mysys.registry')
    r = Resource.create_new_resource()
    r.name = 'thing'
    yield client.register_resource(r)

    ref = r.reference()

    r2 = yield client.get_resource(ref)
    log.debug(rc)
    ns.update(locals())
