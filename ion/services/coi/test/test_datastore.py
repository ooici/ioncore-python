#!/usr/bin/env python

"""
@file ion/services/coi/test/test_hello.py
@author David Stuebe
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.coi.datastore import DataStoreServiceClient
from ion.test.iontest import IonTestCase

from net.ooici.play import addressbook_pb2


class DataStoreTest(IonTestCase):
    """
    Testing example hello service.
    """
    FileLocation = '/Users/dstuebe/Dropbox/OOICI/Proto2David/01184000_0.protostruct'


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        """
        services = [
            {'name':'ds1','module':'ion.core.process.process','class':'Process',
             'spawnargs':{'proc-name':'ps1'}
                },
            {'name':'ds2','module':'ion.core.process.process','class':'Process',
             'spawnargs':{'proc-name':'ps2'}
                }
        ]
        """

        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'ps1'}
                },
            {'name':'ds2','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'ps2'}
                }
        ]


        self.sup = yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_push(self):


        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        proc_ds1 = self._get_procinstance(child_ds1)
        
        child_ds2 = yield self.sup.get_child_id('ds2')
        log.debug('Process ID:' + str(child_ds2))
        proc_ds2 = self._get_procinstance(child_ds2)

        repo, ab = proc_ds1.workbench.init_repository(addressbook_pb2.AddressLink,'addressbook')

                        
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '123 456 7890'
        
        ab.owner = p
            
        ab.person.add()
        ab.person[0] = p
        
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='John'
        p.id = 222
        p.email = 'd222@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '321 456 7890'
    
        ab.person.add()
        ab.person[1] = p    
    
        repo.commit()

        print 'BEFORE PUSH!'

        print 'PROC_DS1 HASHED OBJECTS', proc_ds1.workbench._hashed_elements.keys()
        print 'PROC_DS2 HASHED OBJECTS', proc_ds2.workbench._hashed_elements.keys()


        response, ex = yield proc_ds1.push('ps2','addressbook')

        self.assertEqual(response, proc_ds1.ION_SUCCESS)

        print 'AFTER PUSH!'
        print 'PROC_DS1 HASHED OBJECTS', proc_ds1.workbench._hashed_elements.keys()
        print 'PROC_DS2 HASHED OBJECTS', proc_ds2.workbench._hashed_elements.keys()


        name = repo.repository_key
        
        repo_ds2 = proc_ds2.workbench.get_repository(name)
        
        
        self.assertEqual(repo_ds2._dotgit, repo._dotgit)
        
        ab_2 = repo_ds2.checkout('master')
        
        self.assertEqual(ab_2, ab)
        
        
        # Test to make sure pushing a non existent workbench fails
        
        response, ex = yield proc_ds1.push('ps2','addressbooksss')

        self.assertNotEqual(response, proc_ds1.ION_SUCCESS)
        
        
        
        
    @defer.inlineCallbacks
    def test_clone(self):


        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        proc_ds1 = self._get_procinstance(child_ds1)
        
        child_ds2 = yield self.sup.get_child_id('ds2')
        log.debug('Process ID:' + str(child_ds2))
        proc_ds2 = self._get_procinstance(child_ds2)

        repo, ab = proc_ds1.workbench.init_repository(addressbook_pb2.AddressLink,'addressbook')

                        
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '123 456 7890'
        
        ab.owner = p
            
        ab.person.add()
        ab.person[0] = p
        
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='John'
        p.id = 222
        p.email = 'd222@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '321 456 7890'
    
        ab.person.add()
        ab.person[1] = p    
    
        repo.commit()

        print 'BEFORE CLONE!'

        print 'PROC_DS1 HASHED OBJECTS', proc_ds1.workbench._hashed_elements.keys()
        print 'PROC_DS2 HASHED OBJECTS', proc_ds2.workbench._hashed_elements.keys()


        response, ex = yield proc_ds2.clone('ps1','addressbook','addressbook')

        self.assertEqual(response, proc_ds1.ION_SUCCESS)

        print 'AFTER CLONE!'
        print 'PROC_DS1 HASHED OBJECTS', proc_ds1.workbench._hashed_elements.keys()
        print 'PROC_DS2 HASHED OBJECTS', proc_ds2.workbench._hashed_elements.keys()

        
        repo_ds2 = proc_ds2.workbench.get_repository('addressbook')
        
        
        self.assertEqual(repo_ds2._dotgit, repo._dotgit)
        
        ab_2 = repo_ds2.checkout('master')
        
        self.assertEqual(ab_2, ab)


    @defer.inlineCallbacks
    def test_load_data(self):

        

        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        proc_ds1 = self._get_procinstance(child_ds1)
        
        child_ds2 = yield self.sup.get_child_id('ds2')
        log.debug('Process ID:' + str(child_ds2))
        proc_ds2 = self._get_procinstance(child_ds2)
        
        f = open(self.FileLocation, "rb")
        
        wb = proc_ds1.workbench
        dataset = wb.unpack_structure(f.read())
        f.close()
        
        print 'dataset:',dataset
        
        print 'rootGroup',dataset.rootGroup
        
        
        

