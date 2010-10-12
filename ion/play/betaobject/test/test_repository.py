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

from ion.play.betaobject import gpb_wrapper
from ion.play.betaobject import repository
from ion.play.betaobject import workbench


class RepositoryTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        
        repo, ab = wb.init(addressbook_pb2.AddressLink)
        
        self.repo = repo
        self.ab = ab
        self.wb = wb
        
        
            
    def test_simple_commit(self):

        p = self.repo.create_wrapped_object(addressbook_pb2.Person)
                        
        p = self.repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '123 456 7890'
        
        
        self.ab.owner = p
        self.assertEqual(self.ab.owner.name ,'David')
            
        self.ab.person.add()
        self.ab.person[0] = p
        
        self.ab.person.add()
        p = self.repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='John'
        p.id = 78
        p.email = 'J@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '111 222 3333'
        self.ab.person[1] = p
        
        cref = self.repo.commit(comment='testing commit')
        print cref
        
        print 'dotgit', self.repo._dotgit
        
        