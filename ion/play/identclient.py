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

from ion.services.coi.identity_registry import IdentityRegistryClient

from ion.resources import coi_resource_descriptions

from ion.services.coi.identity_registry import IdentityRegistryService, IdentityRegistryClient
from ion.resources.coi_resource_descriptions import IdentityResource

from ion.data import dataobject
from ion.resources import coi_resource_descriptions
dataobject.DataObject._types['IdentityResource'] = coi_resource_descriptions.IdentityResource
class IdentityWebResource(resource.Resource):

    def __init__(self, identity_service_client):
        resource.Resource.__init__(self)
        self.client = identity_service_client
        self.putChild('', self)
        self.putChild('get_user', GetUser(self.client))
        self.putChild('register_user', RegisterUser(self.client))

    def render(self, request):
        return "IdentityRegistryService"


def give_form(user, misc):
    from ion.data import dataobject
    from ion.services.coi import identity_registry
    from ion.data import dataobject
    from ion.resources import coi_resource_descriptions as coi
    page = '<html><body>' + \
           misc + \
           '<FORM NAME="input" ACTION="test.html" METHOD="post">\n' + \
           '<table border="1">\n' +\
           '<tr><th>Attribute</th><th>Value</th></tr>\n' + \
           'Search Field: <select name="field">\n'
    
    for field_name in user.attributes:
        if (field_name not in ("RegistryBranch","RegistryIdentity","RegistryCommit")):
            page += '<option value="' + field_name + '">' + field_name + '</option>'
    page += '</select> for \n'
    
    page += '<input type="text" name="search_pattern" value="" />'
    page += '<INPUT TYPE="submit" NAME="search" VALUE="Search" /><p/>\n'

    for field_name in user.attributes:
        field_value = str(getattr(user, field_name))
        if (field_name not in ("RegistryBranch","RegistryIdentity","RegistryCommit")):
            # need something here to count the linefeeds and turn it into a text area if it has 0 or more LF
            if (field_name == 'lifecycle'):
                page += '<tr><td>' + field_name + '</td><td>'
                page += '<SELECT NAME="lifecycle">'
                for lc_state in ('New', 'Active', 'Inactive', 'Commissioned', 'Decommissioned', 'Developed', 'Retired'):
                    if (field_value == lc_state.lower() ):
                        page += '<OPTION VALUE="' + lc_state + '" SELECTED>' + lc_state
                    else:
                        page += '<OPTION VALUE="' + lc_state + '">' + lc_state

                page += '</SELECT>'
                page += '</td></tr>\n'
            else:
                page += '<tr><td>' + field_name + '</td><td><input type="text" name="' + field_name + '" value="' + field_value + '" /></td></tr>\n'
                    
    page += '</table>\n'
    page += '<INPUT TYPE="submit" NAME="add" VALUE="Add" />\n'
    page += '<INPUT TYPE="submit" NAME="update" VALUE="Update" />\n'
    page += '</FORM>'
    page += '</body></html>'
        
    return page

class GetUser(resource.Resource):

    def __init__(self, identity_service_client):
        resource.Resource.__init__(self)
        self.client = identity_service_client
        
    
    
    
    
    def render(self, request):
        from ion.services.coi import identity_registry
        from ion.data import dataobject
        from ion.resources import coi_resource_descriptions as coi
        from ion.services.coi.identity_registry import IdentityRegistryClient
        from ion.resources import coi_resource_descriptions
        
        def _cb(result):
            request.write(str(result))
        #user = request.args['user'][0]
        #print '$$$$$', user
        
        #d = self.client.find_users(user)
        #d.addCallback(_cb)

        client = identity_registry.IdentityRegistryClient(target='mysys.identity')
        user = IdentityResource.create_new_resource()
        
        # initialize the user
        user.common_name = "Roger Unwin A13"
        user.country = "US" 
        user.trust_provider = "ProtectNetwork"
        user.domain_component = "cilogon"
        
        user.expiration_date = "Tue Jun 29 23:32:16 PDT 2010"
        # These are the fields we prompt the user for during registration
        user.first_name = "Roger"
        user.last_name = "Unwin"
        user.phone = "8588675309"
        user.fax = "6198675309"
        user.email = "unwin@sdsc.edu"
        user.organization = "University of California San Diego"
        user.department = "San Diego Supercomputing Center"
        user.title = "Deep Sea Submarine Captain"
        
        unknown = client.register_user(user)
        
        
        user_description = coi_resource_descriptions.IdentityResource()
        user_description.first_name = 'oger'
        
        users1 = client.find_users(user_description,regex=True)

        

       

        misc = "matching id = " + str(users1)

        return give_form(user, misc)
        

class RegisterUser(resource.Resource):

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


        
#@defer.inlineCallbacks
def main(ns={}):
    
    from ion.services.coi import identity_registry
    from ion.data import dataobject
    from ion.resources import coi_resource_descriptions as coi
                                #yield Container.configure_messaging('mysys.identity', {'name_type':'worker', 'args':{'scope':'system'}, 'scope':'system'})
    client = identity_registry.IdentityRegistryClient(target='mysys.identity')
                                #r = coi.IdentityResource.create_new_resource()
                                #Hack.
    #r = dataobject.Resource.create_new_resource()
    #r.name = 'thing'
    #yield client.register_user(r)
    
    #ref = r.reference()
    
    #r2 = yield client.get_user(ref)
    #print r2
    ns.update(locals())
    webservice = IdentityWebResource(client)
    site = server.Site(webservice)
    reactor.listenTCP(8999, site)
    ns.update(locals())

main()

