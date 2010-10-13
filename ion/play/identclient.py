"""

@file ion/play/identclient.py
@author Roger Unwin with help from Dorian Raymer
@brief client for registering and authenticating identities via a web interface

run this from lcaarch like this:
    twistd -n --pidfile=m2 cc -h amoeba.ucsd.edu ion/play/identclient.py
"""
from twisted.internet import reactor
from twisted.internet import defer
from twisted.python import log

from twisted.web import server
from twisted.web import resource


from ion.core.cc.container import Container

from ion.services.coi.identity_registry import IdentityRegistryClient

from ion.resources import coi_resource_descriptions

#from ion.services.coi.identity_registry import IdentityRegistryService, IdentityRegistryClient
from ion.resources.coi_resource_descriptions import IdentityResource

from ion.data import dataobject
from ion.resources import coi_resource_descriptions
dataobject.DataObject._types['IdentityResource'] = coi_resource_descriptions.IdentityResource


try:
   import json
except:
   import simplejson as json

class IdentityWebResource(resource.Resource):

    def __init__(self, identity_service_client):
        resource.Resource.__init__(self)
        self.client = identity_service_client
        self.putChild('', self)

        self.putChild('add_user', AddUser(self.client))
        self.putChild('find_user', FindUser(self.client))
        self.putChild('erase_registry', EraseAllUsers(self.client))

    def render(self, request):
        return "IdentityRegistryService"


def give_links():
    return '<br><a href="/add_user">Add User</a> ' + \
           '<a href="/find_user">Find User</a> ' \
           '<a href="/erase_registry">Erase All User</a> '


def wrap_html(html):
    return '<html><body>' + html +'</body></html>'

def wrap_form(html, path):
    return '<FORM NAME="input" ACTION="' + path + '" METHOD="post">\n' + html + '</FORM>'

def give_form(user):
    page = ""
    for field_name in user.attributes:
        field_value = str(getattr(user, field_name))
        if (field_name in ("RegistryBranch","RegistryIdentity","RegistryCommit")):
            page += '<input type="hidden" name="' + field_name + '" value="' + field_value + '" />'

    page += '<table border="1">\n' +\
           '<tr><th>Attribute</th><th>Value</th></tr>\n'

    for field_name in user.attributes:
        field_value = str(getattr(user, field_name))
        if (field_name not in ("RegistryBranch","RegistryIdentity","RegistryCommit", "certificate", "rsa_private_key")):






            # need something here to count the linefeeds and turn it into a text area if it has 0 or more LF
            if (field_name == 'lifecycle'):
                page += '<tr><td>' + field_name + '</td><td>'
                page += '<SELECT NAME="lifecycle">'
                for lc_state in ('*','new', 'active', 'inactive', 'commissioned', 'decomm', 'developed', 'retired'):
                    if (field_value == lc_state.lower() ):
                        page += '<OPTION VALUE="' + lc_state + '" SELECTED>' + lc_state
                    else:
                        page += '<OPTION VALUE="' + lc_state + '">' + lc_state

                page += '</SELECT>'
                page += '</td></tr>\n'
            else:
                page += '<tr><td>' + field_name + '</td><td><input type="text" name="' + field_name + '" value="' + field_value + '" /></td></tr>\n'
        else:
            if (field_name  in ("certificate", "rsa_private_key")):
                page += '<tr><td>' + field_name + '</td><td><textarea rows="2" cols="20" name="' + field_name + '"/>' + field_value + '</textarea></td></tr>\n'

    page += '</table>\n'

    return page

class AddUser(resource.Resource):

    def __init__(self, identity_service_client):
        resource.Resource.__init__(self)
        self.client = identity_service_client

    def render(self, request):

        def _cb(result):
            request.write('<html><body>')
            request.write("User " + result.first_name + " " + result.last_name + " ( " + result.name + " ) added.")
            user = IdentityResource.create_new_resource()
            request.write(wrap_form(give_form(user) + '<INPUT TYPE="submit" NAME="add" VALUE="Add User" /><p/>\n', "/add_user"))
            request.write(give_links())
            request.write('</body></html>')
            request.finish()

        #print str(request)

        if (len(request.args) > 0):
            new_user = coi_resource_descriptions.IdentityResource.create_new_resource()
            for field_name in new_user.attributes:
                if (field_name not in ("RegistryBranch","RegistryIdentity","RegistryCommit", "lifecycle")):
                    if (len(request.args[field_name]) > 0):
                        setattr(new_user, field_name, request.args[field_name][0])

            d = self.client.register_user(new_user)
            d.addCallback(_cb)
        else:
            user = IdentityResource.create_new_resource()
            return wrap_html(wrap_form(give_form(user) + '<INPUT TYPE="submit" NAME="add" VALUE="Add User" /><p/>\n', "/add_user") + give_links())

        return server.NOT_DONE_YET

class EraseAllUsers(resource.Resource):

    def __init__(self, identity_service_client):
        resource.Resource.__init__(self)
        self.client = identity_service_client

    def render(self, request):

        def _cb(result):
            request.write('<html><body>')
            request.write("Registry cleared.<br/>")
            user = IdentityResource.create_new_resource()
            request.write(wrap_form(give_form(user) + '<INPUT TYPE="submit" NAME="add" VALUE="Add User" /><p/>\n', "/erase_registry"))
            request.write(give_links())
            request.write('</body></html>')
            request.finish()

        if ('confirm' in request.args):
            d = self.client.clear_identity_registry()
            d.addCallback(_cb)
        else:
            request.write(wrap_html(wrap_form('<INPUT TYPE="submit" NAME="confirm" VALUE="Confirm" /> <INPUT TYPE="submit" NAME="abort" VALUE="Abort" /><p/>\n' + give_links(), "/erase_registry")))
            request.finish()
        return server.NOT_DONE_YET


class FindUser(resource.Resource):
    def __init__(self, identity_service_client):
        resource.Resource.__init__(self)
        self.client = identity_service_client

    def render(self, request):

        def show_array_cb(result):
            request.write('<html><body>')
            if ((len(result) == 0) or (result == None)):
                request.write("no matches (2)" + str(result) + "<br\>")
                user = IdentityResource.create_new_resource()
                request.write(wrap_form(give_form(user) + '<INPUT TYPE="submit" NAME="search" VALUE="Search" /><p/>\n' + give_links(), "/find_user"))
            else:
                request.write("Found " + str(len(result)) + " matches<br/>\n")

                for i in result:
                    request.write('<dd/><a href="/find_user?ooi_id=' +  i.RegistryIdentity + '&branch=' + i.RegistryBranch + '&commit=' + i.RegistryCommit +'">' + i.first_name + ' ' + i.last_name + '(' + i.name + ')</a><br/>\n')

                user = result[0]
                request.write("<hr>")
                request.write(wrap_form(give_form(user) + '<INPUT TYPE="submit" NAME="search" VALUE="Search" /><INPUT TYPE="submit" NAME="update" VALUE="Update" /><input type="submit" name="clear" value="Clear"><p/>\n', '/find_user'))
                request.write(give_links())
            request.write('</body></html>')
            request.finish()

        def show_single_cb(result):
            request.write('<html><body>')
            if (result == None):
                request.write("no matches (1)<br\>")
                user = IdentityResource.create_new_resource()
                request.write(wrap_form(give_form(user) + '<INPUT TYPE="submit" NAME="search" VALUE="Search" /><INPUT TYPE="submit" NAME="update" VALUE="Update" /><input type="submit" name="clear" value="Clear"><p/>\n', "/find_user"))
                request.write(give_links())
            else:
                request.write("<hr>")
                request.write(wrap_form(give_form(result) +  '<INPUT TYPE="submit" NAME="search" VALUE="Search" /><INPUT TYPE="submit" NAME="update" VALUE="Update" /><input type="submit" name="clear" value="Clear"><p/>\n', "/find_user"))
                request.write(give_links())
            request.write('</body></html>')
            request.finish()




        if ((len(request.args) > 0) and ('clear' not in request.args)) :
            if ('update' in request.args.keys()):
                updated_user = coi_resource_descriptions.IdentityResource()
                for field_name in updated_user.attributes:
                    if (field_name not in ("lifecycle")):
                        if (len(request.args[field_name]) > 0):
                            setattr(updated_user, field_name, request.args[field_name][0])

                foo = dataobject.LCState(state=request.args["lifecycle"][0].lower())
                updated_user.set_lifecyclestate(foo)

                d = self.client.update_user(updated_user)
                d.addCallback(show_single_cb)
            else:
                if ('ooi_id' in request.args.keys()):
                    user = IdentityResource.create_new_resource(id=request.args['ooi_id'][0],  branch=request.args['branch'][0]) # was supposed to need commit=request.args['commit'][0], but doesnt want it. need to ask davif
                    d = self.client.get_user(user.reference(head=True))
                    d.addCallback(show_single_cb)
                else:
                    user_description = coi_resource_descriptions.IdentityResource()

                    attnames = []
                    #print "************ " + str(request.args)
                    for field_name in user_description.attributes:
                        if (field_name not in ("RegistryBranch","RegistryIdentity","RegistryCommit", "lifecycle")):
                            if (len(request.args[field_name]) > 0) and (len(request.args[field_name][0]) > 0) :
                                setattr(user_description, field_name, request.args[field_name][0])
                                attnames.append(field_name)
                                #print "************************* " + field_name + " = '" + request.args[field_name][0] + "'"
                    #print "lifecycle = " + request.args["lifecycle"][0]
                    if (request.args["lifecycle"][0] != '*'):
                        attnames.append('lifecycle')
                        foo = dataobject.LCState(state=request.args["lifecycle"][0].lower())
                        user_description.set_lifecyclestate(foo)
                    #print "************ LOOKING FOR..... " + str(user_description)
                    #print "************ attnames ......... " + str(attnames)
                    d = self.client.find_users(user_description, regex=True, attnames=attnames) #, ignore_defaults=True)
                    d.addCallback(show_array_cb)
            return server.NOT_DONE_YET
        else:
            user = IdentityResource.create_new_resource()
            return wrap_html(wrap_form(give_form(user) + '<INPUT TYPE="submit" NAME="search" VALUE="Search" /><p/>\n', "/find_user") + give_links())





def main(ns={}):
    print "STARTING...."
    from ion.services.coi import identity_registry
    from ion.data import dataobject
    from ion.resources import coi_resource_descriptions as coi

    from ion.resources import description_utility
    description_utility.load_descriptions()

    client = identity_registry.IdentityRegistryClient(target='mysys.identity')


    ns.update(locals())
    webservice = IdentityWebResource(client)
    site = server.Site(webservice)
    reactor.listenTCP(8999, site)


    ns.update(locals())

#if __name__ == '__main__':
main() #main() has to be called on start. this is a maited pair with identservice.py if you are going to alter this line. justify yourself to Roger Unwin
