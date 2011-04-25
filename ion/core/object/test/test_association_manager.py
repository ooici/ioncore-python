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


from ion.core.object import workbench
from ion.core.object import object_utils

ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
PREDICATE_TYPE = object_utils.create_type_identifier(object_id=14, version=1)


class AssociationInstanceTest(unittest.TestCase):


    def setUp(self):

        self.wb = workbench.WorkBench('No Process Test')

        # Copy the address book object from the setup method to three new objects and use them in an association
        subject = self.wb.create_repository(ADDRESSLINK_TYPE)
        subject.root_object.title = 'subject'
        subject.commit('a subject')

        self.subject = subject

        predicate = self.wb.create_repository(PREDICATE_TYPE)
        predicate.root_object.word = 'predicate'
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

        association_repo = self.wb.get_repository(self.association.AssociationIdentity)
        self.assertNotEqual(association_repo, None)

    def test_iter(self):

        for item in self.obj.associations_as_object:

            self.assertEqual(item, self.association)


    def test_iteritems(self):

        for predicate, association in self.obj.associations_as_object.iteritems():

            self.assertEqual(predicate, self.predicate.repository_key)
            self.assertEqual(association.pop(), self.association)


    def test_len(self):

        self.assertEqual(len(self.obj.associations_as_object),1)
        self.assertEqual(len(self.obj.associations_as_subject), 0)



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


        self.association.SetNull()

        self.assertNotIn(self.association, self.predicate.associations_as_predicate.get_associations())

        self.assertNotIn(self.association, self.obj.associations_as_object.get_associations())

        self.assertNotIn(self.association, self.subject.associations_as_subject.get_associations())



    def test_str(self):

        log.info(str(self.obj.associations_as_object))
