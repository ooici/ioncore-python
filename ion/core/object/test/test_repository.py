#!/usr/bin/env python
"""
@Brief Test implementation of the repository class

@file ion/play
@author David Stuebe
@test Service the protobuffers wrapper class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
#from twisted.internet import defer

from ion.test.iontest import IonTestCase

from net.ooici.play import addressbook_pb2

from ion.core.object import gpb_wrapper
from ion.core.object import repository
from ion.core.object import workbench


class RepositoryTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb
        
    def test_create_commit_ref(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)
        cref = repo._create_commit_ref()
        print cref
            
    def test_checkout_commit_id(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)  
        commit_ref = repo.commit()
        repo.checkout(commit_id=commit_ref)
        
            
    def test_simple_commit(self):
        
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)

                        
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
        
        ab.person.add()
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='John'
        p.id = 78
        p.email = 'J@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '111 222 3333'
        
        ab.person[1] = p
        
        cref = repo.commit(comment='testing commit')
        print cref
        
        print 'dotgit', repo._dotgit
        
        p = None
        ab = None
        
        ab = repo.checkout(branch_name='master')
        print 'ab after checkout',ab
        
        print ab.person[0]
        print ab.person[1]
        
        
        for person in ab.person:
            print 'person',person
        print 'owner',ab.owner
        
        