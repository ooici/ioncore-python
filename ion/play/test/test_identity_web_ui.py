"""

@file ion/play/test_identity_web_ui.py
@author Roger Unwin
@brief testing for the web UI. Requires the client and server be running at the same time.

run this from lcaarch like this:
    twistd -n --pidfile=m2 cc -h amoeba.ucsd.edu ion/play/identclient.py

Reccomend this code be retired.

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
from ion.core.cc.container import Container
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

        #[ { 'name':'identity', 'module':'ion.services.coi.identity_registry' } ]
        services = [{'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService', 'spawnargs':{'servicename':'datastore'}},
                    {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService', 'spawnargs':{'datastore_service':'datastore'}},
                    {'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'}]
        supervisor = yield self._spawn_processes(services)



        client = identity_registry.IdentityRegistryClient()


        webservice = identclient.IdentityWebResource(client)
        site = server.Site(webservice)
        self.listening_port = yield reactor.listenTCP(9001, site) #8999


    @defer.inlineCallbacks
    def tearDown(self):
        """
        """
        yield self._stop_container()
        yield self.listening_port.stopListening()

    #@defer.inlineCallbacks
    def test_register_user(self):
        """
        Deactivated this test, as to omuch has changed, for this test to be of value.
        In reality, the new identity service only tracks certificates and rsa_private_keys.  This was designed for a time when there were many more fields.  
        Also, a certificate/key will never be passed around as form elements.
        The code remains as potential example code.
        """
        #actual_add_user_load = yield client.getPage("http://localhost:9001/add_user") #8999

        #p = re.compile('name="RegistryBranch" value="([^"]+)"')
        #result =  p.findall(actual_add_user_load)
        #base_args = "RegistryBranch=" + result[0]

        #p = re.compile('name="RegistryIdentity" value="([^"]+)"')
        #result =  p.findall(actual_add_user_load)
        #base_args += "&RegistryIdentity=" + result[0]

        #p = re.compile('name="RegistryCommit" value="([^"]+)"')
        #result =  p.findall(actual_add_user_load)
        #if (result):
        #    base_args += "&RegistryCommit=" + result[0]



        #
        # Test Adding a user
        #
        

        #post = 'add=Add+User&name=Roger+Unwin&rsa_private_key=test&certificate=test&' + \
        #                   'country=test&domain_component=test&trust_provider=test&' + \
        #                   'expiration_date=test&lifecycle=new&common_name=test&' + base_args
        #add_user_result = yield client.getPage("http://localhost:9001/add_user?" + post) #8999

        


        #self.failUnlessSubstring('User Roger Unwin added', add_user_result)

        


        #
        # Test loading the find user page
        #

        #actual_find_user_load = yield client.getPage("http://localhost:9001/find_user") #8999
        #self.failUnlessSubstring('<FORM NAME="input" ACTION="/find_user" METHOD="post">', actual_find_user_load)

        #
        # Test find user
        #

        

        #post = 'search=Search&common_name=&rsa_private_key=&certificate=&' + \
        #        'country=&domain_component=&trust_provider=&' + \
        #        'expiration_date=&lifecycle=*&common_name=&' + base_args

        #actual_find_user_result = yield client.getPage("http://localhost:9001/find_user?" + post) #8999
        
        
        #self.failUnlessSubstring('Roger Unwin', actual_find_user_result)
        
        #
        # Test loading the found user.
        #

        #find_user_partial_url_pattern = re.compile('<a href="/find_user\?([^"]+)">')
        #result =  find_user_partial_url_pattern.findall(actual_find_user_result)


        #actual_load_user_result = yield client.getPage("http://localhost:9001/find_user?" + result[0]) #8999
        #self.failUnlessSubstring('Roger Unwin', actual_load_user_result)
        
        
        #
        # Test update
        #


        #post = 'update=Update&common_name=Roger+Unwin&rsa_private_key=changed_test&certificate=changed_test&' + \
        #        'country=changed_test&' + \
        #        'domain_component=changed_test&trust_provider=changed_test&expiration_date=changed_test&lifecycle=new&' + \
        #        'common_name=changed_test&' + base_args
        #
        #update_user_result = yield client.getPage("http://localhost:9001/find_user?" + post) #8999
        

        #self.failUnlessSubstring('changed_test', update_user_result)
