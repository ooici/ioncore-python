"""
run this from lcaarch like this:
    twistd -n --pidfile=m2 magnet -h amoeba.ucsd.edu ion/play/identclient.py
"""
from twisted.internet import reactor
from twisted.internet import defer
from twisted.python import log

from twisted.web import server
from twisted.web import resource


from magnet.container import Container

class IdentityWebResource(resource.Resource):

    def __init__(self, identity_service_client):
        resource.Resource.__init__(self)
        self.client = identity_service_client
        self.putChild('', self)
        self.putChild('get_user', GetUser(self.client))

    def render(self, request):
        return "IdentityRegistryService"

class GetUser(resource.Resource):

    def __init__(self, identity_service_client):
        resource.Resource.__init__(self)
        self.client = identity_service_client

    def render(self, request):
        def _cb(result):
            request.write(str(result))
        user = request.args['user'][0]
        print '$$$$$', user
        d = self.client.find_users(user)
        d.addCallback(_cb)
        return server.NOT_DONE_YET




@defer.inlineCallbacks
def main(ns={}):
    from ion.services.coi import identity_registry
    from ion.data import dataobject
    from ion.resources import coi_resource_descriptions as coi
    #yield Container.configure_messaging('mysys.identity', {'name_type':'worker', 'args':{'scope':'system'}, 'scope':'system'})
    client = identity_registry.IdentityRegistryClient(target='mysys.identity')
    #r = coi.IdentityResource.create_new_resource()
    #Hack.
    r = dataobject.Resource.create_new_resource()
    r.name = 'thing'
    yield client.register_user(r)
    
    ref = r.reference()
    
    r2 = yield client.get_user(ref)
    print r2
    ns.update(locals())
    webservice = IdentityWebResource(client)
    site = server.Site(webservice)
    reactor.listenTCP(8999, site)
    ns.update(locals())
main()

