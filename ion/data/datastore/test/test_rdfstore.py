#!/usr/bin/env python

"""
@file ion/data/datastore/test/test_rdfstore.py
@author David Stuebe
@author Dorian Raymer
@brief test rdf store
"""

import logging

from twisted.internet import defer
from twisted.trial import unittest

from ion.data import store
from ion.data.datastore import cas
from ion.data.datastore import objstore
from ion.data.datastore import rdfstore


sha1 = cas.sha1

class AssociationObjectTest(unittest.TestCase):

    def setUp(self):

        self.association = rdfstore.Association('ASubject','APredicate','AObject')
        

    def test_type(self):
        self.failUnlessEqual(self.association.type, 'association')
        
    def test_match(self):
        self.assert_(self.association.match(cas.Blob('ASubject')))
        self.assertNot(self.association.match(cas.Blob('BSubject')))
        self.assertNot(self.association.match(cas.Blob('BSubject'),position=rdfstore.OBJECT))
        self.assert_(self.association.match(cas.Blob('ASubject'),position=rdfstore.SUBJECT))
        self.assert_(self.association.match(cas.Blob('APredicate'),position=rdfstore.PREDICATE))
        self.assertNot(self.association.match(cas.Blob('ASubject'),position=rdfstore.PREDICATE))        
        self.assertRaises(KeyError,self.association.match,cas.Blob('ASubject'),position='blahblahblah')
        