"""

@file ion/play/test_identity_web_ui.py
@author Roger Unwin 
@brief testing for the web UI. Requires the client and server be running at the same time.

run this from lcaarch like this:
    twistd -n --pidfile=m2 magnet -h amoeba.ucsd.edu ion/play/identclient.py
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest
from ion.test.iontest import IonTestCase
from twisted.web import client
from ion.core import bootstrap

#import ion.play.identservice
import ion.play.identclient as identclient
from twisted.internet import reactor
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

try:
   import json
except:
   import simplejson as json
import re
from ion.services.coi import identity_registry
from ion.data import dataobject
from ion.resources import coi_resource_descriptions as coi

from ion.resources import description_utility
from twisted.web import server
from twisted.internet import reactor
description_utility.load_descriptions()


class IdentityRegistryUITest(IonTestCase):
    """
    Testing client classes of User Registration
    """
    
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        services = [
            {
                'name':'identity', 
                'module':'ion.services.coi.identity_registry',
                'class':'IdentityRegistryService',
                'spawnargs':{
                    'sys-name':'mysys',
                    'servicename':'identity',
                    'scope':'system'
                    }
                }
            ]
        supervisor = yield self._spawn_processes(services)
                    
        
            
        client = identity_registry.IdentityRegistryClient(target='mysys.identity')
                               
    
        webservice = identclient.IdentityWebResource(client)
        site = server.Site(webservice)
        self.listening_port = reactor.listenTCP(8999, site)
        
        
    @defer.inlineCallbacks
    def tearDown(self):
        """
        """
        yield self._stop_container()
        yield self.listening_port.stopListening()
        
    @defer.inlineCallbacks
    def test_register_user(self):
        print "IN TEST_REGISTER_USER"
        actual_add_user_load = yield client.getPage("http://localhost:8999/add_user")
        
        p = re.compile('name="RegistryBranch" value="([^"]+)"')
        result =  p.findall(actual_add_user_load)
        base_args = "RegistryBranch=" + result[0]
        
        p = re.compile('name="RegistryIdentity" value="([^"]+)"')
        result =  p.findall(actual_add_user_load)
        base_args += "&RegistryIdentity=" + result[0]
        
        p = re.compile('name="RegistryCommit" value="([^"]+)"')
        result =  p.findall(actual_add_user_load)
        if (result):
            base_args += "&RegistryCommit=" + result[0]
        
        print "********************** BASE ARGS = " + base_args
        
        reference_add_user_load = """<html><body><FORM NAME="input" ACTION="/add_user" METHOD="post">
<input type="hidden" name="RegistryBranch" value="master" /><input type="hidden" name="RegistryCommit" value="" /><input type="hidden" name="RegistryIdentity" value="9fb233d1-322a-45f1-9561-f1f18e3ef8cb" /><table border="1">
<tr><th>Attribute</th><th>Value</th></tr>
<tr><td>rsa_private_key</td><td><textarea rows="2" cols="20" name="rsa_private_key"/></textarea></td></tr>
<tr><td>certificate</td><td><textarea rows="2" cols="20" name="certificate"/></textarea></td></tr>
<tr><td>department</td><td><input type="text" name="department" value="" /></td></tr>
<tr><td>first_name</td><td><input type="text" name="first_name" value="" /></td></tr>
<tr><td>organization</td><td><input type="text" name="organization" value="" /></td></tr>
<tr><td>country</td><td><input type="text" name="country" value="" /></td></tr>
<tr><td>fax</td><td><input type="text" name="fax" value="" /></td></tr>
<tr><td>name</td><td><input type="text" name="name" value="" /></td></tr>
<tr><td>last_name</td><td><input type="text" name="last_name" value="" /></td></tr>
<tr><td>email</td><td><input type="text" name="email" value="" /></td></tr>
<tr><td>domain_component</td><td><input type="text" name="domain_component" value="" /></td></tr>
<tr><td>trust_provider</td><td><input type="text" name="trust_provider" value="" /></td></tr>
<tr><td>expiration_date</td><td><input type="text" name="expiration_date" value="" /></td></tr>
<tr><td>lifecycle</td><td><SELECT NAME="lifecycle"><OPTION VALUE="*">*<OPTION VALUE="new" SELECTED>new<OPTION VALUE="active">active<OPTION VALUE="inactive">inactive<OPTION VALUE="commissioned">commissioned<OPTION VALUE="decomm">decomm<OPTION VALUE="developed">developed<OPTION VALUE="retired">retired</SELECT></td></tr>
<tr><td>common_name</td><td><input type="text" name="common_name" value="" /></td></tr>
<tr><td>phone</td><td><input type="text" name="phone" value="" /></td></tr>
<tr><td>title</td><td><input type="text" name="title" value="" /></td></tr>
</table>
<INPUT TYPE="submit" NAME="add" VALUE="Add User" /><p/>
</FORM><br><a href="/add_user">Add User</a> <a href="/find_user">Find User</a> <a href="/erase_registry">Erase All User</a> </body></html>"""
        
        
        
        
        
        #
        # Remove the RegistryIdentity from the output, as it will change each run.
        #
        pat = re.compile('name="RegistryIdentity" value="[^"]*"')
        reference_add_user_load = pat.sub( 'REDACTED', reference_add_user_load)
        actual_add_user_load = pat.sub( 'REDACTED', actual_add_user_load)
        #
        # Verify that the page loaded correctly
        #
        self.assertEqual(reference_add_user_load, actual_add_user_load)
        
        
        
        #
        # Test Adding a user
        #
        
        post = 'add=Add+User&name=Roger+Unwin&first_name=Roger&last_name=Unwin&rsa_private_key=test&certificate=test&' + \
                           'department=test&organization=test&country=test&fax=test&email=test&domain_component=test&trust_provider=test&' + \
                           'expiration_date=test&lifecycle=new&common_name=test&phone=test&title=test&' + base_args
        add_user_result = yield client.getPage("http://localhost:8999/add_user?" + post)
        
        
        
        self.failUnlessSubstring('User Roger Unwin ( Roger Unwin ) added.', add_user_result)
        
        
        #
        # Test loading the find user page
        #
        
        actual_find_user_load = yield client.getPage("http://localhost:8999/find_user")
      
        reference_find_user_load = """<html><body><FORM NAME="input" ACTION="/find_user" METHOD="post">
<input type="hidden" name="RegistryBranch" value="master" /><input type="hidden" name="RegistryCommit" value="" /><input type="hidden" name="RegistryIdentity" value="2dd17405-0f59-4e7e-a3f3-d487e2683526" /><table border="1">
<tr><th>Attribute</th><th>Value</th></tr>
<tr><td>rsa_private_key</td><td><textarea rows="2" cols="20" name="rsa_private_key"/></textarea></td></tr>
<tr><td>certificate</td><td><textarea rows="2" cols="20" name="certificate"/></textarea></td></tr>
<tr><td>department</td><td><input type="text" name="department" value="" /></td></tr>
<tr><td>first_name</td><td><input type="text" name="first_name" value="" /></td></tr>
<tr><td>organization</td><td><input type="text" name="organization" value="" /></td></tr>
<tr><td>country</td><td><input type="text" name="country" value="" /></td></tr>
<tr><td>fax</td><td><input type="text" name="fax" value="" /></td></tr>
<tr><td>name</td><td><input type="text" name="name" value="" /></td></tr>
<tr><td>last_name</td><td><input type="text" name="last_name" value="" /></td></tr>
<tr><td>email</td><td><input type="text" name="email" value="" /></td></tr>
<tr><td>domain_component</td><td><input type="text" name="domain_component" value="" /></td></tr>
<tr><td>trust_provider</td><td><input type="text" name="trust_provider" value="" /></td></tr>
<tr><td>expiration_date</td><td><input type="text" name="expiration_date" value="" /></td></tr>
<tr><td>lifecycle</td><td><SELECT NAME="lifecycle"><OPTION VALUE="*">*<OPTION VALUE="new" SELECTED>new<OPTION VALUE="active">active<OPTION VALUE="inactive">inactive<OPTION VALUE="commissioned">commissioned<OPTION VALUE="decomm">decomm<OPTION VALUE="developed">developed<OPTION VALUE="retired">retired</SELECT></td></tr>
<tr><td>common_name</td><td><input type="text" name="common_name" value="" /></td></tr>
<tr><td>phone</td><td><input type="text" name="phone" value="" /></td></tr>
<tr><td>title</td><td><input type="text" name="title" value="" /></td></tr>
</table>
<INPUT TYPE="submit" NAME="search" VALUE="Search" /><p/>
</FORM><br><a href="/add_user">Add User</a> <a href="/find_user">Find User</a> <a href="/erase_registry">Erase All User</a> </body></html>"""
        pat = re.compile('name="RegistryIdentity" value="[^"]*"')
        reference_find_user_load = pat.sub( 'REDACTED', reference_find_user_load)
        actual_find_user_load = pat.sub( 'REDACTED', actual_find_user_load)
        self.assertEqual(reference_find_user_load, actual_find_user_load)
        
        #
        # Test find user
        #
        
        
        post = 'search=Search&name=&first_name=o&last_name=&rsa_private_key=&certificate=&' + \
                'department=&organization=&country=&fax=&email=&domain_component=&trust_provider=&' + \
                'expiration_date=&lifecycle=*&common_name=&phone=&title=&' + base_args
        
        actual_find_user_result = yield client.getPage("http://localhost:8999/find_user?" + post)
        self.failUnlessSubstring('Found 1 matches', actual_find_user_result)
        
        #
        # Test loading the found user.
        #
        
        find_user_partial_url_pattern = re.compile('<a href="/find_user\?([^"]+)">')
        result =  find_user_partial_url_pattern.findall(actual_find_user_result)
        
        
        actual_load_user_result = yield client.getPage("http://localhost:8999/find_user?" + result[0])
        actual_load_user_result_save_for_update = actual_load_user_result
        
        reference_load_user = """<html><body><hr><FORM NAME="input" ACTION="/find_user" METHOD="post">
<input type="hidden" name="RegistryBranch" value="master" /><input type="hidden" name="RegistryCommit" value="f5851d73613cad06c6bd6a30cf9122b9c6eadede" /><input type="hidden" name="RegistryIdentity" value="76b2d350-8fd9-4943-866a-afecf0583df7" /><table border="1">
<tr><th>Attribute</th><th>Value</th></tr>
<tr><td>rsa_private_key</td><td><textarea rows="2" cols="20" name="rsa_private_key"/>test</textarea></td></tr>
<tr><td>certificate</td><td><textarea rows="2" cols="20" name="certificate"/>test</textarea></td></tr>
<tr><td>department</td><td><input type="text" name="department" value="test" /></td></tr>
<tr><td>first_name</td><td><input type="text" name="first_name" value="Roger" /></td></tr>
<tr><td>organization</td><td><input type="text" name="organization" value="test" /></td></tr>
<tr><td>country</td><td><input type="text" name="country" value="test" /></td></tr>
<tr><td>fax</td><td><input type="text" name="fax" value="test" /></td></tr>
<tr><td>name</td><td><input type="text" name="name" value="Roger Unwin" /></td></tr>
<tr><td>last_name</td><td><input type="text" name="last_name" value="Unwin" /></td></tr>
<tr><td>email</td><td><input type="text" name="email" value="test" /></td></tr>
<tr><td>domain_component</td><td><input type="text" name="domain_component" value="test" /></td></tr>
<tr><td>trust_provider</td><td><input type="text" name="trust_provider" value="test" /></td></tr>
<tr><td>expiration_date</td><td><input type="text" name="expiration_date" value="test" /></td></tr>
<tr><td>lifecycle</td><td><SELECT NAME="lifecycle"><OPTION VALUE="*">*<OPTION VALUE="new" SELECTED>new<OPTION VALUE="active">active<OPTION VALUE="inactive">inactive<OPTION VALUE="commissioned">commissioned<OPTION VALUE="decomm">decomm<OPTION VALUE="developed">developed<OPTION VALUE="retired">retired</SELECT></td></tr>
<tr><td>common_name</td><td><input type="text" name="common_name" value="test" /></td></tr>
<tr><td>phone</td><td><input type="text" name="phone" value="test" /></td></tr>
<tr><td>title</td><td><input type="text" name="title" value="test" /></td></tr>
</table>
<INPUT TYPE="submit" NAME="search" VALUE="Search" /><INPUT TYPE="submit" NAME="update" VALUE="Update" /><input type="submit" name="clear" value="Clear"><p/>
</FORM><br><a href="/add_user">Add User</a> <a href="/find_user">Find User</a> <a href="/erase_registry">Erase All User</a> </body></html>"""

        pat = re.compile('name="RegistryIdentity" value="[^"]*"')
        reference_load_user = pat.sub( 'REDACTED', reference_load_user)
        actual_load_user_result = pat.sub( 'REDACTED', actual_load_user_result)
        pat = re.compile('name="RegistryCommit" value="[^"]*"')
        reference_load_user = pat.sub( 'REDACTED', reference_load_user)
        actual_load_user_result = pat.sub( 'REDACTED', actual_load_user_result)
        self.assertEqual(reference_load_user, actual_load_user_result)


        #
        # Test update
        #       - Need to preserve base args for this test
        
        p = re.compile('name="RegistryBranch" value="([^"]+)"')
        result =  p.findall(actual_load_user_result_save_for_update)
        base_args = "RegistryBranch=" + result[0]
        
        p = re.compile('name="RegistryIdentity" value="([^"]+)"')
        result =  p.findall(actual_load_user_result_save_for_update)
        base_args += "&RegistryIdentity=" + result[0]
        
        p = re.compile('name="RegistryCommit" value="([^"]+)"')
        result =  p.findall(actual_load_user_result_save_for_update)
        if (result):
            base_args += "&RegistryCommit=" + result[0]
        
        
        post = 'update=Update&name=Roger+Unwin&first_name=Roger&last_name=Unwin&rsa_private_key=changed_test&certificate=changed_test&' + \
                'department=changed_test&organization=changed_test&country=changed_test&fax=changed_test&email=changed_test&' + \
                'domain_component=changed_test&trust_provider=changed_test&expiration_date=changed_test&lifecycle=new&' + \
                'common_name=changed_test&phone=changed_test&title=changed_test&' + base_args
        actual_update_user_result = yield client.getPage("http://localhost:8999/find_user?" + post)
        
        reference_update_user_result = """<html><body><hr><FORM NAME="input" ACTION="/find_user" METHOD="post">
<input type="hidden" name="RegistryBranch" value="master" /><input type="hidden" name="RegistryCommit" value="8c4aabb4c82edd0160ebb6ff46f6bd1fb0458c72" /><input type="hidden" name="RegistryIdentity" value="940c1be6-93ae-4b3c-89c7-f1aab0a211a9" /><table border="1">
<tr><th>Attribute</th><th>Value</th></tr>
<tr><td>rsa_private_key</td><td><textarea rows="2" cols="20" name="rsa_private_key"/>changed_test</textarea></td></tr>
<tr><td>certificate</td><td><textarea rows="2" cols="20" name="certificate"/>changed_test</textarea></td></tr>
<tr><td>department</td><td><input type="text" name="department" value="changed_test" /></td></tr>
<tr><td>first_name</td><td><input type="text" name="first_name" value="Roger" /></td></tr>
<tr><td>organization</td><td><input type="text" name="organization" value="changed_test" /></td></tr>
<tr><td>country</td><td><input type="text" name="country" value="changed_test" /></td></tr>
<tr><td>fax</td><td><input type="text" name="fax" value="changed_test" /></td></tr>
<tr><td>name</td><td><input type="text" name="name" value="Roger Unwin" /></td></tr>
<tr><td>last_name</td><td><input type="text" name="last_name" value="Unwin" /></td></tr>
<tr><td>email</td><td><input type="text" name="email" value="changed_test" /></td></tr>
<tr><td>domain_component</td><td><input type="text" name="domain_component" value="changed_test" /></td></tr>
<tr><td>trust_provider</td><td><input type="text" name="trust_provider" value="changed_test" /></td></tr>
<tr><td>expiration_date</td><td><input type="text" name="expiration_date" value="changed_test" /></td></tr>
<tr><td>lifecycle</td><td><SELECT NAME="lifecycle"><OPTION VALUE="*">*<OPTION VALUE="new" SELECTED>new<OPTION VALUE="active">active<OPTION VALUE="inactive">inactive<OPTION VALUE="commissioned">commissioned<OPTION VALUE="decomm">decomm<OPTION VALUE="developed">developed<OPTION VALUE="retired">retired</SELECT></td></tr>
<tr><td>common_name</td><td><input type="text" name="common_name" value="changed_test" /></td></tr>
<tr><td>phone</td><td><input type="text" name="phone" value="changed_test" /></td></tr>
<tr><td>title</td><td><input type="text" name="title" value="changed_test" /></td></tr>
</table>
<INPUT TYPE="submit" NAME="search" VALUE="Search" /><INPUT TYPE="submit" NAME="update" VALUE="Update" /><input type="submit" name="clear" value="Clear"><p/>
</FORM><br><a href="/add_user">Add User</a> <a href="/find_user">Find User</a> <a href="/erase_registry">Erase All User</a> </body></html>"""
        
        pat = re.compile('name="RegistryIdentity" value="[^"]*"')
        reference_update_user_result = pat.sub( 'REDACTED', reference_update_user_result)
        actual_update_user_result = pat.sub( 'REDACTED', actual_update_user_result)
        pat = re.compile('name="RegistryCommit" value="[^"]*"')
        reference_update_user_result = pat.sub( 'REDACTED', reference_update_user_result)
        actual_update_user_result = pat.sub( 'REDACTED', actual_update_user_result)
        self.assertEqual(reference_update_user_result, actual_update_user_result)
        
        #
        # Test erase all
        #
        erase_all_load = yield client.getPage("http://localhost:8999/erase_registry")
        reference_erase_all_load = """<html><body><FORM NAME="input" ACTION="/erase_registry" METHOD="post">
<INPUT TYPE="submit" NAME="confirm" VALUE="Confirm" /> <INPUT TYPE="submit" NAME="abort" VALUE="Abort" /><p/>
<br><a href="/add_user">Add User</a> <a href="/find_user">Find User</a> <a href="/erase_registry">Erase All User</a> </FORM></body></html>"""
        self.assertEqual(reference_erase_all_load, erase_all_load)
        
        #
        # Test confirm erase all
        #
        confirm_erase_all_load = yield client.getPage("http://localhost:8999/erase_registry?confirm=Confirm")
        self.failUnlessSubstring('Registry cleared.<br/>', confirm_erase_all_load)
        
        