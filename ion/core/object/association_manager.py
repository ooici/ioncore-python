#!/usr/bin/env python


"""
@file ion/core/object/association_manager.py
@brief  Manager class for associations
@author David Stuebe

"""

from ion.core.exception import ApplicationError

import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

from ion.core.object import object_utils

ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)


class AssociaitonManagerError(ApplicationError):
    """
    Error class for the association manager
    """

class AssociationManager(object):
    """
    @Brief A class for managing associations

    @TODO Specialize the manager for managing as_subject, as_object etc....
    """

    def __init__(self, predicate_map=None):

        self.predicate_sorted_associations={}

        if predicate_map is None:
            predicate_map = {}

        if not isinstance(predicate_map,dict):
            raise AssociaitonManagerError('Invalid argument to create association manger class: predicate_map must be a dictionary')

        self.predicate_map = predicate_map


    def update_predicate_map(self, predicate_map):

        if not isinstance(predicate_map, dict):
            raise AssociaitonManagerError('Invalid argument to update association manger predicate_map: predicate_map must be a dictionary')

        self.predicate_map.update(predicate_map)


    def __str__(self):

        ret = 'Association Manager Object! Current predicates:\n'
        for k in self.predicate_sorted_associations.iterkeys():
            pred_name = k
            if k in self.predicate_map:
                pred_name = self.predicate_map.get(k)

            ret += str(pred_name) + '\n'

        ret += 'Association Manager complete.'
        return ret

    def update(self, other):

        if not isinstance(other, AssociationManager):
            raise AssociaitonManagerError('Invalid argument to update in association manager: %s' % str(other))

        for k,v in other.iteritems():

            associations = self.predicate_sorted_associations.get(k, None)
            if associations is None:
                associations = set()
                self.predicate_sorted_associations[k]=associations

            associations.update(v)

    def add(self, association):

        if not isinstance(association, AssociationInstance):
            raise AssociaitonManagerError('Invalid argument to add in association manager: %s' % str(association))

        predicate = association.PredicateReference.key

        associations = self.predicate_sorted_associations.get(predicate, None)

        if associations is None:
            associations = set()
            self.predicate_sorted_associations[predicate]=associations

        associations.add(association)


    def remove(self, association):

        if not isinstance(association, AssociationInstance):
            raise AssociaitonManagerError('Invalid argument to remove in association manager: %s' % str(association))

        predicate = association.PredicateReference.key

        associations = self.predicate_sorted_associations.get(predicate, None)

        if associations is None:
            raise AssociaitonManagerError('Predicate not found in association manager. Can not remove it!')

        try:
            associations.remove(association)
        except KeyError, ke:
            log.error('Can not remove an association which is not present in this association manager')
            raise AssociaitonManagerError('Association not found in association manager. Can not remove it!')



    def get_associations_by_predicate(self, predicate):
        """
        Return the set of associations for a particular predicate
        """
        if predicate in self.predicate_map:
            predicate_id = self.predicate_map.get(predicate)

        return self.predicate_sorted_associations.get(predicate_id,set())

    def get_associations(self):
        """
        Return the set of associations for a particular predicate
        """
        result = set()
        for k,v in self.predicate_sorted_associations.iteritems():
            result.update(v)

        return result


class AssociationInstanceError(ApplicationError):
    """
    Exception class for Resource Instance Object
    """

class AssociationInstance(object):
    """
    @brief Association instance is a wrapper for working with associations

    @TODO how do we make sure that there is only one resource instance per resource? If the process creates multiple
    resource instances to wrap the same resource we are in trouble. Need to find a way to keep a cache of the instances
    """

    NULL = 'null'

    def __init__(self, association_repository, workbench):
        """
        Resource Instance objects are created by the resource client
        """
        if not hasattr(association_repository, 'Repository'):
            raise AssociationInstanceError('Invalid argument type association_repository. Must be a repository!')

        # Use the property getter - just to be safe...
        self._repository = association_repository.Repository

        if self.Repository.status == self.Repository.NOTINITIALIZED:
            raise AssociationInstanceError('The association_repository is in an invalid state - the association must be checked out after a pull.')


        if self._repository._workspace_root.ObjectType != ASSOCIATION_TYPE:
            raise AssociationInstanceError('Can not create an association instance with an object that is not an association.')

        # Hold onto a workbench here to get the latest state of any associated objects
        self._workbench = workbench

        # For any objects which are associated and present in the work bench - set up the book keeping
        self._add_association()


    def _add_association(self):

        previous_object = self._repository._workspace_root.object
        previous_object_repository = self._workbench.get_repository(previous_object.key)

        if previous_object_repository is not None:
            previous_object_repository.associations_as_object.add(self)


        previous_subject = self._repository._workspace_root.subject
        previous_subject_repository = self._workbench.get_repository(previous_subject.key)

        if previous_subject_repository is not None:
            previous_subject_repository.associations_as_subject.add(self)

        previous_predicate = self._repository._workspace_root.predicate
        previous_predicate_repository = self._workbench.get_repository(previous_predicate.key)

        if previous_predicate_repository is not None:
            previous_predicate_repository.associations_as_predicate.add(self)



    @property
    def Repository(self):
        return self._repository


    @property
    def Association(self):
        repo = self._repository
        return repo._workspace_root


    @property
    def SubjectReference(self):
        repo = self._repository
        return repo._workspace_root.subject

    @property
    def ObjectReference(self):
        repo = self._repository
        return repo._workspace_root.object


    @property
    def PredicateReference(self):
        repo = self._repository
        return repo._workspace_root.predicate


    def SetObjectReference(self, new_object):

        if not hasattr(new_object, 'Repository'):
            raise AssociationInstanceError('Invalid argument to SetObjectReference. The new object must have or be a repository! (ResourceInstance, AssociationInstance or Repository)')


        new_repo = new_object.Repository

        if new_repo.status == new_repo.NOTINITIALIZED:
            raise AssociationInstanceError('Can not associate to an object repository which is not in a finite state - must checkout a commit first!')

        elif new_repo.status == new_repo.MODIFIED:
            # For now - do a commit automagically!

            new_repo.repo.commit('Automagical commit made during association instance method SetObjectReference')


        previous_object = self._repository._workspace_root.object

        previous_object_repository = self._workbench.get_repository(previous_object.key)

        # Confusing interface - set the object reference to point a the current state of the repository containing the new object
        new_repo.set_repository_reference(previous_object, current_state=True)

        self.Repository.commit('Updated association instance with new object reference')


        new_repo.associations_as_object.add(self)

        if previous_object_repository is not None:
            try:
                previous_object_repository.associations_as_object.remove(self)
            except AssociaitonManagerError, ame:
                log.debug('Association not found in previous objects association manager')


    def set_null(self):

        previous_object = self._repository._workspace_root.object
        previous_object_repository = self._workbench.get_repository(previous_object.key)

        previous_subject = self._repository._workspace_root.subject
        previous_subject_repository = self._workbench.get_repository(previous_subject.key)

        previous_predicate = self._repository._workspace_root.predicate
        previous_predicate_repository = self._workbench.get_repository(previous_predicate.key)

        
        if previous_object_repository is not None:
            if previous_object_repository.status == previous_object_repository.NOTINITIALIZED:
                raise AssociationInstanceError('Error in set_null: associated object repository is present but not in a finite state - must checkout a commit first!')

            elif previous_object_repository.status == previous_object_repository.MODIFIED:
                # For now - do a commit automagically!
                previous_object_repository.repo.commit('Automagical commit made during association instance method SetObjectReference')
        
                
            previous_object_repository.set_repository_reference(previous_object, current_state=True)

            try:
                previous_object_repository.associations_as_object.remove(self)
            except AssociaitonManagerError, ame:
                log.debug('Association not found in previous objects association manager')
                
                
        if previous_predicate_repository is not None:
            if previous_predicate_repository.status == previous_predicate_repository.NOTINITIALIZED:
                raise AssociationInstanceError('Error in set_null: associated predicate repository is present but not in a finite state - must checkout a commit first!')

            elif previous_predicate_repository.status == previous_predicate_repository.MODIFIED:
                # For now - do a commit automagically!
                previous_predicate_repository.repo.commit('Automagical commit made during association instance method SetpredicateReference')
        
                
            previous_predicate_repository.set_repository_reference(previous_predicate, current_state=True)

            try:
                previous_predicate_repository.associations_as_predicate.remove(self)
            except AssociaitonManagerError, ame:
                log.debug('Association not found in previous predicates association manager')
            
                
        if previous_subject_repository is not None:
            if previous_subject_repository.status == previous_subject_repository.NOTINITIALIZED:
                raise AssociationInstanceError('Error in set_null: associated subject repository is present but not in a finite state - must checkout a commit first!')

            elif previous_subject_repository.status == previous_subject_repository.MODIFIED:
                # For now - do a commit automagically!
                previous_subject_repository.repo.commit('Automagical commit made during association instance method SetsubjectReference')
        
                
            previous_subject_repository.set_repository_reference(previous_subject, current_state=True)

            try:
                previous_subject_repository.associations_as_subject.remove(self)
            except AssociaitonManagerError, ame:
                log.debug('Association not found in previous subjects association manager')
                
        if self.Repository.status == self.Repository.MODIFIED:
            self.Repository.commit('Updated association instance with new object reference before setting null')
        
        
        
        previous_object.key = self.NULL
        previous_object.branch = self.NULL
        previous_object.commit = self.NULL
        
        previous_subject.key = self.NULL
        previous_subject.branch = self.NULL
        previous_subject.commit = self.NULL
        
        previous_predicate.key = self.NULL
        previous_predicate.branch = self.NULL
        previous_predicate.commit = self.NULL

        self.Repository.commit('Association set to null!')