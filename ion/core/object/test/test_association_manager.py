#!/usr/bin/env python
"""
@brief Test implementation of the workbench class

@file ion/core/object/test/test_workbench
@author David Stuebe
@test The object management WorkBench test class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer

import weakref
import gc


from net.ooici.play import addressbook_pb2

from ion.core.object import gpb_wrapper
from ion.core.object import repository
from ion.core.object import workbench
from ion.core.object import object_utils

# For testing the message based ops of the workbench
from ion.core.process.process import ProcessFactory, Process
from ion.test.iontest import IonTestCase



ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)



class AssociationInstanceTest(unittest.TestCase):


    def setUp(self):

        self.wb = workbench.WorkBench('No Process Test')

        # Copy the address book object from the setup method to three new objects and use them in an association
        subject = self.wb.create_repository(ADDRESSLINK_TYPE)
        subject.root_object.title = 'subject'
        subject.commit('a subject')

        self.subject = subject

        predicate = self.wb.create_repository(ADDRESSLINK_TYPE)
        predicate.root_object.title = 'predicate'
        predicate.commit('a predicate')

        self.predicate = predicate

        obj = self.wb.create_repository(ADDRESSLINK_TYPE)
        obj.root_object.title = 'object'
        obj.commit('a object')

        self.obj = obj
        
        self.association = self.wb.create_association(subject, predicate, obj)
        

    def test_getters(self):

        obj_repo = self.wb.get_repository(self.association.ObjectReference.key)
        self.assertEqual(obj_repo, self.obj )

        subject_repo = self.wb.get_repository(self.association.SubjectReference.key)
        self.assertEqual(subject_repo, self.subject )

        predicate_repo = self.wb.get_repository(self.association.PredicateReference.key)
        self.assertEqual(predicate_repo, self.predicate )



    def test_in_managers(self):

        self.assertIn(self.association, self.obj.associations_as_object.get_associations())
        self.assertNotIn(self.association, self.obj.associations_as_subject.get_associations())
        self.assertNotIn(self.association, self.obj.associations_as_predicate.get_associations())


        self.assertIn(self.association, self.predicate.associations_as_predicate.get_associations())
        self.assertNotIn(self.association, self.predicate.associations_as_object.get_associations())
        self.assertNotIn(self.association, self.predicate.associations_as_subject.get_associations())


        self.assertIn(self.association, self.subject.associations_as_subject.get_associations())
        self.assertNotIn(self.association, self.subject.associations_as_predicate.get_associations())
        self.assertNotIn(self.association, self.subject.associations_as_object.get_associations())

    def test_set_object_reference(self):

        new_obj = self.wb.create_repository(ADDRESSLINK_TYPE)
        new_obj.root_object.title = 'new object'
        new_obj.commit('a new object')

        self.assertNotIn(self.association, new_obj.associations_as_object.get_associations())

        self.association.SetObjectReference(new_obj)

        self.assertNotIn(self.association, self.obj.associations_as_object.get_associations())

        self.assertIn(self.association, new_obj.associations_as_object.get_associations())


    def test_set_null(self):


        self.association.set_null()

        self.assertNotIn(self.association, self.predicate.associations_as_predicate.get_associations())

        self.assertNotIn(self.association, self.obj.associations_as_object.get_associations())

        self.assertNotIn(self.association, self.subject.associations_as_subject.get_associations())




