#!/usr/bin/env python

"""
@file ion/services/coi/test/test_hello.py
@author David Stuebe
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.test.iontest import IonTestCase

from net.ooici.play import addressbook_pb2
from ion.util import procutils as pu

class DataStoreTest(IonTestCase):
    """
    Testing example hello service.
    """
    
    # This is a temporary way to test communication between python and java using GPBs...
    FileLocation = '/Users/dstuebe/Dropbox/OOICI/Proto2David/01184000_0.protostruct'
    #FileLocation = '/Users/dstuebe/Dropbox/OOICI/Proto2David/grid.protostruct'

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

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
    def test_merge_push(self):
            
        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        proc_ds1 = self._get_procinstance(child_ds1)
            
        child_ds2 = yield self.sup.get_child_id('ds2')
        log.debug('Process ID:' + str(child_ds2))
        proc_ds2 = self._get_procinstance(child_ds2)
            
        repo1, ab1 = proc_ds1.workbench.init_repository(addressbook_pb2.AddressLink,'addressbook')
            
           
        pa1 = repo1.create_wrapped_object(addressbook_pb2.Person)
        pa1.name='David'
        pa1.id = 5
        pa1.email = 'd@s.com'
        ph = pa1.phone.add()
        ph.type = pa1.WORK
        ph.number = '123 456 7890'
            
        ab1.owner = pa1
            
        ab1.person.add()
        ab1.person[0] = pa1
            
        pb1 = repo1.create_wrapped_object(addressbook_pb2.Person)
        pb1.name='John'
        pb1.id = 222
        pb1.email = 'd222@s.com'
        ph = pb1.phone.add()
        ph.type = pb1.WORK
        ph.number = '321 456 7890'
            
        ab1.person.add()
        ab1.person[1] = pb1    
            
        repo1.commit()
            
        response, ex = yield proc_ds1.push('ps2','addressbook')
            
        self.assertEqual(response, proc_ds1.ION_SUCCESS)
            
        # Get the uuid for the repository
        repo_key = repo1.repository_key
            
        # Get the repository out on the second process
        repo2 = proc_ds2.workbench.get_repository(repo_key)
            
        self.assertEqual(repo2._dotgit, repo1._dotgit)
            
        ab2 = repo2.checkout('master')
            
        self.assertEqual(ab2, ab1)
            
        # Now modify and commit on both data stores! - Divergence!
            
        pa2 = ab2.owner
        pa2.email = 'process2@gmail.com'
            
        # Show off that it changed in both places - it is a real DAG!
        self.assertIdentical(ab2.owner, ab2.person[0])
        # Commit on repo2    
        repo2.commit()
            
        # wait one second to make sure that the commits can be sorted by time stamp
        yield pu.asleep(1)
        # Modify repo1
        pa1.email = 'process1@gmail.com'
        repo1.commit()
            
            
        response, ex = yield proc_ds2.push('ps1',repo_key)
            
        self.assertEqual(response, proc_ds2.ION_SUCCESS)
            
        # Assert that the Divergence was recorded!
        self.assertEqual(len(repo1.branches[0].commitrefs),2)
            
        # Merge on Read
        ab1 = repo1.checkout('master')
            
        # Assert that the Divergence was repaired!
        self.assertEqual(len(repo1.branches[0].commitrefs),1)
        self.assertEqual(ab1.owner.email, 'process1@gmail.com')
        
        # Now push back to ps2 and show that the state is repaired in both locations
        response, ex = yield proc_ds1.push('ps2',repo_key)
            
        self.assertEqual(response, proc_ds2.ION_SUCCESS)
        # Assert that the Divergence was repaired!
        self.assertEqual(len(repo2.branches[0].commitrefs),1)
        # Checkout the current state
        ab2 = repo2.checkout('master')
        
        # The state is repaired here too
        self.assertEqual(ab2.owner.email, 'process1@gmail.com')
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        
        
    @defer.inlineCallbacks
    def test_pull(self):


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

        print 'BEFORE PULL!'

        print 'PROC_DS1 HASHED OBJECTS', proc_ds1.workbench._hashed_elements.keys()
        print 'PROC_DS2 HASHED OBJECTS', proc_ds2.workbench._hashed_elements.keys()


        response, ex = yield proc_ds2.pull('ps1','addressbook')

        self.assertEqual(response, proc_ds1.ION_SUCCESS)

        print 'AFTER PULL!'
        print 'PROC_DS1 HASHED OBJECTS', proc_ds1.workbench._hashed_elements.keys()
        print 'PROC_DS2 HASHED OBJECTS', proc_ds2.workbench._hashed_elements.keys()

        
        repo_ds2 = proc_ds2.workbench.get_repository(repo.repository_key)
        
        
        self.assertEqual(repo_ds2._dotgit, repo._dotgit)
        
        ab_2 = repo_ds2.checkout('master')
        
        self.assertEqual(ab_2, ab)


    @defer.inlineCallbacks
    def test_load_data(self):
        """
        This is an easy way to make a test for loading data from a file...
        """
        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        proc_ds1 = self._get_procinstance(child_ds1)
        
        
        try:
            f = open(self.FileLocation, "rb")
        except IOError as ex:
            log.info('Test data file not found: %s'\
                % self.FileLocation)
            return
        
        wb = proc_ds1.workbench
        dataset = wb.unpack_structure(f.read())
        f.close()
        
        log.info('dataset: \n' + str(dataset))
        
        log.info('rootGroup: \n' +str(dataset.rootGroup))
        
        def log_atts(atts, tab=''):
            
            for att in atts:
                attstring = tab+'Attribute:\n %s \n %s \n' % (tab+str(att), tab+str(att.array))
                log.info(attstring)        
        
        def log_dims(dims, tab=''):
            for dim in dims:
                dimstring = tab+'Dimension:\n %s \n' % (tab+str(dim))
                log.info(dimstring) 
        
        def log_vars(vars):
            
            for var in vars:
                varstring = 'Variable: \n %s \n Content: \n ' % (str(var))
                varstring += print_bounded_array(var.content)
                log.info(varstring)
                
                log_atts(var.attributes, tab='    ')
                log_dims(var.shape, tab='    ')
                
        def print_bounded_array(bounded_arrays):
            ba_string = ''
            for ba in bounded_arrays:
                ba_string += str(ba)+'\n'
                
                if len(ba.ndarray.value) <= 25:
                    ba_string += str(ba.ndarray.value)+'\n'
                else:
                    ba_string += str(ba.ndarray.value[:25])+'\n'
            return ba_string
                
        log_dims(dataset.rootGroup.dimensions)
        log_atts(dataset.rootGroup.attributes)
        log_vars(dataset.rootGroup.variables)
        
        
        
        #for var in dataset.rootGroup.variables:
        #   log.info('Variables: \n' + str(var))
            
            


