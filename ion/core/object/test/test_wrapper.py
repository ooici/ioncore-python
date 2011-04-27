#!/usr/bin/env python

"""
@file ion/play
@author David Stuebe
@test Service the protobuffers wrapper class
"""

import ion.util.ionlog
from twisted.trial.unittest import SkipTest
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
#from twisted.internet import defer

from ion.test.iontest import IonTestCase

from net.ooici.play import addressbook_pb2

from ion.core.object import gpb_wrapper
from ion.core.object.gpb_wrapper import LINK_TYPE, CDM_DATASET_TYPE, OOIObjectError
from ion.core.object import workbench
from ion.core.object import object_utils


PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)
ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
ADDRESSBOOK_TYPE = object_utils.create_type_identifier(object_id=20002, version=1)

ATTRIBUTE_TYPE = object_utils.create_type_identifier(object_id=10017, version=1)

TEST_TYPE = object_utils.create_type_identifier(object_id=20010, version=1)



class WrapperMethodsTest(unittest.TestCase):
    
    def test_field_props(self):
        """
        """
        ab = gpb_wrapper.Wrapper._create_object(ADDRESSBOOK_TYPE)

        self.failUnlessEqual(ab._Properties['title'].field_type, "TYPE_STRING")
        self.failUnless(ab._Properties['title'].field_enum is None)

    def test_field_enum(self):
        """
        """
        p = gpb_wrapper.Wrapper._create_object(PERSON_TYPE)
        ph = p.phone.add()
        self.failUnlessEqual(ph._Properties['type'].field_type, "TYPE_ENUM")
        self.failIf(ph._Properties['type'].field_enum is None)

        self.failUnless(hasattr(ph._Properties['type'].field_enum, 'WORK'))

        self.failUnless(hasattr(ph, '_Enums'))
        self.failUnless(ph._Enums.has_key('PhoneType'))
        self.failUnless(hasattr(ph._Enums['PhoneType'], 'WORK'))
        self.failUnlessEqual(ph._Enums['PhoneType'].WORK, 2)


    def test_derived_wrappers(self):

        ab = gpb_wrapper.Wrapper._create_object(ADDRESSBOOK_TYPE)
        # Derived wrappers is still empty
        self.assertEqual(ab.DerivedWrappers, {})

        # Setting scalar fields does not do anything here...
        ab.title = 'name'
        self.assertEqual(ab.DerivedWrappers, {})

        persons = ab.person
        self.assertIn(persons, ab.DerivedWrappers.values())
        self.assertIn(persons._gpbcontainer, ab.DerivedWrappers)

        owner = ab.owner
        self.assertIn(owner, ab.DerivedWrappers.values())
        self.assertIn(owner.GPBMessage, ab.DerivedWrappers)

        ref_to_persons = ab.person
        self.assertEqual(len(ab.DerivedWrappers),2)

        person = ab.person.add()
        self.assertIn(person, ab.DerivedWrappers.values())
        self.assertIn(person.GPBMessage, ab.DerivedWrappers)

    def test_set_get_del(self):

        ab = gpb_wrapper.Wrapper._create_object(ADDRESSBOOK_TYPE)

        # set a string field
        ab.title = 'String'

        # Can not set a string field to an integer
        self.assertRaises(TypeError, setattr, ab , 'title', 6)

        # Get a string field
        self.assertEqual(ab.title, 'String')

        # Del ?
        try:
            del ab.title
        except AttributeError, ae:
            return

        self.fail('Attribute Error not raised by invalid delete request')


    def test_myid(self):


        ab = gpb_wrapper.Wrapper._create_object(ADDRESSBOOK_TYPE)

        self.assertEqual(ab.MyId, '-1')

        ab.MyId = '5'

        self.assertEqual(ab.MyId, '5')


        ab.Invalidate()
        self.assertRaises(OOIObjectError, getattr, ab, 'MyId')


    def test_source(self):

        ab1 = gpb_wrapper.Wrapper._create_object(ADDRESSLINK_TYPE)
        ab1.MyId = '1'

        ab2 = gpb_wrapper.Wrapper._create_object(ADDRESSLINK_TYPE)
        ab2.MyId = '2'

        ab1.Invalidate(ab2)

        self.assertEqual(ab1.MyId, '2')

        self.assertIdentical(ab1._source, ab2)

    def test_source_set(self):

        ab1 = gpb_wrapper.Wrapper._create_object(ADDRESSLINK_TYPE)
        ab1.MyId = '1'

        ab2 = gpb_wrapper.Wrapper._create_object(ADDRESSBOOK_TYPE)
        ab2.MyId = '2'

        self.assertRaises(OOIObjectError, ab1.Invalidate, ab2)

    def test_source_derived(self):

        ab1 = gpb_wrapper.Wrapper._create_object(ADDRESSBOOK_TYPE)

        ab1.owner.name = 'David'
        ab1.owner.id = 5
        owner1 = ab1.owner

        person1 = ab1.person.add()
        ab1.person[0].name = 'john'
        ab1.person[0].id = 1


        ab2 = gpb_wrapper.Wrapper._create_object(ADDRESSBOOK_TYPE)

        ab2.owner.name = 'David'
        ab2.owner.id = 5
        owner2 = ab2.owner

        person2 = ab2.person.add()
        ab2.person[0].name = 'john'
        ab2.person[0].id = 2
        
        # Since the values are not equal - this will fail
        self.assertRaises(OOIObjectError, ab1.Invalidate, ab2)

        ab2.person[0].id = 1

        ab1.Invalidate(ab2)

        self.assertEqual(person1, person2)
        person1.name = 'Michael'
        self.assertEqual(person1, person2)



    def test_set_composite(self):

        ab = gpb_wrapper.Wrapper._create_object(ADDRESSBOOK_TYPE)

        ab.person.add()
        ab.person.add()

        ab.person[1].name = 'David'
        ab.person[0].name = 'John'
        ab.person[0].id = 5

        p = ab.person[0]

        ph = ab.person[0].phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '123 456 7890'

        self.assertEqual(ab.person[1].name, 'David')
        self.assertEqual(ab.person[0].name, 'John')
        self.assertEqual(ab.person[0].id, 5)

        self.assertNotEqual(ab.person[0],ab.person[1])


    def test_enum_access(self):

        p = gpb_wrapper.Wrapper._create_object(PERSON_TYPE)

        # Get an enum
        self.assertEqual(p.PhoneType.MOBILE,0)
        self.assertEqual(p.PhoneType.HOME,1)
        self.assertEqual(p.PhoneType.WORK,2)

        # error to set an enum
        self.assertRaises(AttributeError, setattr, p.PhoneType, 'MOBILE', 5)

    def test_imported_enum_access(self):


        att = gpb_wrapper.Wrapper._create_object(ATTRIBUTE_TYPE)

        att.data_type = att.DataType.DOUBLE

        self.assertEqual(att.data_type, att.DataType.DOUBLE)

        self.assertRaises(AttributeError, setattr, att.DataType, 'DOUBLE', 'this is not a double')


    def test_listsetfields_message_type(self):

        p = gpb_wrapper.Wrapper._create_object(PERSON_TYPE)


        flist = p.ListSetFields()
        self.assertEqual(len(flist), 0)

        p.name = 'David'
        p.id = 5

        flist = p.ListSetFields()
        self.assertIn('name',flist)
        self.assertIn('id',flist)


        p.phone.add()
        flist = p.ListSetFields()
        self.assertIn('phone',flist)

        self.assertEqual(len(flist), 3)


    def test_listsetfields_scalar_type(self):

        p = gpb_wrapper.Wrapper._create_object(TEST_TYPE)

        flist = p.ListSetFields()
        self.assertEqual(len(flist), 0)

        p.string = 'David'
        p.integer = 5
        p.float = 5.0

        flist = p.ListSetFields()
        self.assertIn('string',flist)
        self.assertIn('integer',flist)
        self.assertIn('float',flist)


        p.strings.append('a string')
        p.integers.append(5)
        p.floats.append(5.0)

        flist = p.ListSetFields()
        self.assertIn('strings',flist)
        self.assertIn('integers',flist)
        self.assertIn('floats',flist)


        self.assertEqual(len(flist), 6)


    def test_isfieldset_message_type(self):

        p = gpb_wrapper.Wrapper._create_object(PERSON_TYPE)

        self.assertEqual(p.IsFieldSet('name'), False)
        p.name = 'David'
        self.assertEqual(p.IsFieldSet('name'), True)

        self.assertEqual(p.IsFieldSet('id'), False)
        p.id = 5
        self.assertEqual(p.IsFieldSet('id'), True)

        self.assertEqual(p.IsFieldSet('phone'), False)
        p.phone.add()
        self.assertEqual(p.IsFieldSet('phone'), False)
        p.phone[0].number = '23232'
        self.assertEqual(p.IsFieldSet('phone'), True)


    def test_isfieldset_scalar_type(self):

        p = gpb_wrapper.Wrapper._create_object(TEST_TYPE)

        self.assertEqual(p.IsFieldSet('string'), False)
        p.string = 'David'
        self.assertEqual(p.IsFieldSet('string'), True)

        self.assertEqual(p.IsFieldSet('integer'), False)
        p.integer = 5
        self.assertEqual(p.IsFieldSet('integer'), True)

        self.assertEqual(p.IsFieldSet('float'), False)
        p.float = 5.0
        self.assertEqual(p.IsFieldSet('float'), True)


        self.assertEqual(p.IsFieldSet('strings'), False)
        p.strings.append( 'David')
        self.assertEqual(p.IsFieldSet('strings'), True)

        self.assertEqual(p.IsFieldSet('integers'), False)
        p.integers.append(5)
        self.assertEqual(p.IsFieldSet('integers'), True)

        self.assertEqual(p.IsFieldSet('floats'), False)
        p.floats.append(5.0)
        self.assertEqual(p.IsFieldSet('floats'), True)





class TestSpecializedCdmMethods(unittest.TestCase):
    """
    """
    def setUp(self):
        # Step 1: Perform initial setup
        wb = workbench.WorkBench('No Process Test')
        repo, ds = wb.init_repository(CDM_DATASET_TYPE)
        
        self.repo = repo
        self.wb = wb
        self.ds = ds
        
        # Step 2: Perform necessary pretests (these may have side-effects)
        self.pretest_MakeRootGroup()
        
        # Step 3: Finish setup as needed
        #    We need a root group to work with in following tests..  this is
        #    taken care of in self.pretest_MakeRootGroup()
        
    
    def pretest_MakeRootGroup(self):
        """
        This pretest is not handled by twistd like methods prefixed with test_.
        This method is simply called by setUp() to check various preconditions
        and then build the root_group of the dataset, to be used by subsequent
        methods.  --  Don't be confused..  this is just a normal method.
        """
        self.assertEqual(None, self.ds.root_group)
        
        self.assertRaises(TypeError, self.ds.MakeRootGroup, 5)
        self.assertEqual(None, self.ds.root_group)
        self.assertRaises(TypeError, self.ds.MakeRootGroup, None)
        self.assertEqual(None, self.ds.root_group)
        
        self.ds.MakeRootGroup('test_group')
        self.assertNotEqual(None, self.ds.root_group)
        self.assertEqual('test_group', self.ds.root_group.name)
        
        self.assertRaises(OOIObjectError, self.ds.MakeRootGroup, 'new_root_group')
        
        # @attention: Since this is a pretest..  leave the new root_group attached
        #             to the dataset (we will need it for all add'l tests
    
    def test_AddGroup(self):
        self.assertEqual(len(self.ds.root_group.groups), 0)
        
        # Test invalid arguments
        self.assertRaises(TypeError, self.ds.root_group.AddGroup, 5)
        self.assertEqual(len(self.ds.root_group.groups), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddGroup, None)
        self.assertEqual(len(self.ds.root_group.groups), 0)
        self.assertRaises(ValueError, self.ds.root_group.AddGroup, '')
        self.assertEqual(len(self.ds.root_group.groups), 0)
        # @todo: Must we disallow groups named by the empty string??
        
        # Test legitimate arguments
        self.ds.root_group.AddGroup('new_group1')
        self.assertEqual(len(self.ds.root_group.groups), 1)
        self.assertEqual(self.ds.root_group.groups[0].name, 'new_group1')
        
        # Ensure more than one group can be created
        self.ds.root_group.AddGroup('new_group2')
        self.assertEqual(len(self.ds.root_group.groups), 2)
        self.assertEqual(self.ds.root_group.groups[1].name, 'new_group2')
        
        # @todo: Test creation of preexisting groups (should this fail?)
#        self.ds.root_group.AddGroup('new_group2')
        
    def test_AddAttribute_to_group(self):
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        
        # Test invalid argument: name
        string_vals = ['val1', 'val2', 'val3']
        int_vals    = [123456, 987654, 0]
        string_type = self.ds.root_group.DataType.STRING
        int_type    = self.ds.root_group.DataType.INT
        self.assertRaises(TypeError, self.ds.root_group.AddAttribute, 5, string_type, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddAttribute, None, string_type, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        self.assertRaises(ValueError, self.ds.root_group.AddAttribute, '', string_type, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        
        # Test invalid argument: values
        mixed_vals = ['val1', 'val2', None, 25]
        self.assertRaises(ValueError, self.ds.root_group.AddAttribute, 'atrib1', string_type, 25)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        # @todo: Ensure entries for "values" of an empty list will fail
#        self.assertRaises(TypeError, self.ds.root_group.AddAttribute, 'atrib1', string_type, [])
#        self.assertEqual(len(self.ds.root_group.attributes), 0)
        self.assertRaises(ValueError, self.ds.root_group.AddAttribute, 'atrib1', string_type, None)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        self.assertRaises(ValueError, self.ds.root_group.AddAttribute, 'atrib1', string_type, mixed_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        
        # Test invalid argument: data_types
        self.assertRaises(TypeError, self.ds.root_group.AddAttribute, 'atrib1', None, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddAttribute, 'atrib1', 'not an actual data_type', string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        self.assertRaises(ValueError, self.ds.root_group.AddAttribute, 'atrib1', int_type, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        # @todo: Ensure entries for data_type of the correct type (int) but not listed in the DataType enum will fail
#        self.assertRaises(TypeError, self.ds.root_group.AddAttribute, 'atrib1', 10000, string_vals)
#        self.assertEqual(len(self.ds.root_group.attributes), 0)
        
        # Test legitimate arguments
        self.ds.root_group.AddAttribute('atrib1', string_type, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertEqual(self.ds.root_group.attributes[0].name, 'atrib1')
        self.assertEqual(self.ds.root_group.attributes[0].array.value, string_vals)
        self.assertEqual(self.ds.root_group.attributes[0].data_type, string_type)
        
        # Ensure more than one attribute can be created
        self.ds.root_group.AddAttribute('atrib2', int_type, int_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 2)
        self.assertEqual(self.ds.root_group.attributes[1].name, 'atrib2')
        self.assertEqual(self.ds.root_group.attributes[1].array.value, int_vals)
        self.assertEqual(self.ds.root_group.attributes[1].data_type, int_type)

        # @todo: Test creation of preexisting attributes (should this fail or act like SetAttribute?)
#        self.ds.root_group.AddAttribute('atrib2', int_type, int_vals)

    def test_AddAttribute_to_variable(self):
        # Seed the root_group with a variable to attach attributes to...
        DT = self.ds.root_group.DataType
        tau  = self.ds.root_group.AddDimension('time', 10, True)
        lat  = self.ds.root_group.AddDimension('lat', 6, False)
        lon  = self.ds.root_group.AddDimension('lon', 25, False)
        variable = self.ds.root_group.AddVariable('var1', DT.FLOAT, [tau, lat, lon])
    
        self.assertEqual(len(variable.attributes), 0)
        
        # Test invalid argument: name
        string_vals = ['val1', 'val2', 'val3']
        int_vals    = [123456, 987654, 0]
        string_type = variable.DataType.STRING
        int_type    = variable.DataType.INT
        self.assertRaises(TypeError, variable.AddAttribute, 5, string_type, string_vals)
        self.assertEqual(len(variable.attributes), 0)
        self.assertRaises(TypeError, variable.AddAttribute, None, string_type, string_vals)
        self.assertEqual(len(variable.attributes), 0)
        self.assertRaises(ValueError, variable.AddAttribute, '', string_type, string_vals)
        self.assertEqual(len(variable.attributes), 0)
        
        # Test invalid argument: values
        mixed_vals = ['val1', 'val2', None, 25]
        self.assertRaises(ValueError, variable.AddAttribute, 'atrib1', string_type, 25)
        self.assertEqual(len(variable.attributes), 0)
        # @todo: Ensure entries for "values" of an empty list will fail
#        self.assertRaises(TypeError, variable.AddAttribute, 'atrib1', string_type, [])
#        self.assertEqual(len(variable.attributes), 0)
        self.assertRaises(ValueError, variable.AddAttribute, 'atrib1', string_type, None)
        self.assertEqual(len(variable.attributes), 0)
        self.assertRaises(ValueError, variable.AddAttribute, 'atrib1', string_type, mixed_vals)
        self.assertEqual(len(variable.attributes), 0)
        
        # Test invalid argument: data_types
        self.assertRaises(TypeError, variable.AddAttribute, 'atrib1', None, string_vals)
        self.assertEqual(len(variable.attributes), 0)
        self.assertRaises(TypeError, variable.AddAttribute, 'atrib1', 'not an actual data_type', string_vals)
        self.assertEqual(len(variable.attributes), 0)
        self.assertRaises(ValueError, variable.AddAttribute, 'atrib1', int_type, string_vals)
        self.assertEqual(len(variable.attributes), 0)
        # @todo: Ensure entries for data_type of the correct type (int) but not listed in the DataType enum will fail
#        self.assertRaises(TypeError, variable.AddAttribute, 'atrib1', 10000, string_vals)
#        self.assertEqual(len(variable.attributes), 0)
        
        # Test legitimate arguments
        variable.AddAttribute('atrib1', string_type, string_vals)
        self.assertEqual(len(variable.attributes), 1)
        self.assertEqual(variable.attributes[0].name, 'atrib1')
        self.assertEqual(variable.attributes[0].array.value, string_vals)
        self.assertEqual(variable.attributes[0].data_type, string_type)
        
        # Ensure more than one attribute can be created
        variable.AddAttribute('atrib2', int_type, int_vals)
        self.assertEqual(len(variable.attributes), 2)
        self.assertEqual(variable.attributes[1].name, 'atrib2')
        self.assertEqual(variable.attributes[1].array.value, int_vals)
        self.assertEqual(variable.attributes[1].data_type, int_type)

        # @todo: Test creation of preexisting attributes (should this fail?)
#        variable.AddAttribute('atrib2', int_type, int_vals)

    def test_AddDimension(self):
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        
        # Test invalid argument: name
        self.assertRaises(TypeError, self.ds.root_group.AddDimension, 5, 1)
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddDimension, None, 1)
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        self.assertRaises(ValueError, self.ds.root_group.AddDimension, '', 1)
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        
        # Test invalid argument: length
        self.assertRaises(ValueError, self.ds.root_group.AddDimension, 'dim1')
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddDimension, 'dim1', None)
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddDimension, 'dim1', 'bad length')
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        self.assertRaises(ValueError, self.ds.root_group.AddDimension, 'dim1', -4)
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        
        # Test invalid argument: variable_length
        self.assertRaises(TypeError, self.ds.root_group.AddDimension, 'dim1', 1, None)
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddDimension, 'dim1', 1, 'not a boolean')
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        
        
        # Test legitimate arguments
        self.ds.root_group.AddDimension('dim1', 20, True)
        self.assertEqual(len(self.ds.root_group.dimensions), 1)
        self.assertEqual(self.ds.root_group.dimensions[0].name, 'dim1')
        self.assertEqual(self.ds.root_group.dimensions[0].length, 20)
        self.assertEqual(self.ds.root_group.dimensions[0].variable_length, True)

        # Ensure more than one dimension can be created
        self.ds.root_group.AddDimension('dim2', 12, False)
        self.assertEqual(len(self.ds.root_group.dimensions), 2)
        self.assertEqual(self.ds.root_group.dimensions[1].name, 'dim2')
        self.assertEqual(self.ds.root_group.dimensions[1].length, 12)
        self.assertEqual(self.ds.root_group.dimensions[1].variable_length, False)
        
        # @todo: Test creation of preexisting dimensions (should this fail or act like SetDimension?)
#        self.ds.root_group.AddDimension('dim2', 12, False)
    
    def test_AddVariable(self):
        self.assertEqual(len(self.ds.root_group.variables), 0)
        
        # Seed the root_group with dimensions to use in creating variables:
        tau = self.ds.root_group.AddDimension('time', 10, True)
        lat = self.ds.root_group.AddDimension('lat', 6, False)
        lon = self.ds.root_group.AddDimension('lon', 25, False)

        shape1 = [tau, lat, lon]
        shape2 = [lat, lon]
        float_type = self.ds.root_group.DataType.FLOAT
        int_type   = self.ds.root_group.DataType.INT
        
        # Test invalid argument: name
        self.assertRaises(TypeError, self.ds.root_group.AddVariable, 5, float_type, shape1)
        self.assertEqual(len(self.ds.root_group.variables), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddVariable, None, float_type, shape1)
        self.assertEqual(len(self.ds.root_group.variables), 0)
        self.assertRaises(ValueError, self.ds.root_group.AddVariable, '', float_type, shape1)
        self.assertEqual(len(self.ds.root_group.variables), 0)

        # Test invalid argument: data_type
        self.assertRaises(TypeError, self.ds.root_group.AddVariable, 'var1', None, shape1)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddVariable, 'var1', 'not an actual data_type', shape1)
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        # @todo: Ensure entries for data_type of the correct type (int) but not listed in the DataType enum will fail
#        self.assertRaises(TypeError, self.ds.root_group.AddVariable, 'var1', 10000)
#        self.assertEqual(len(self.ds.root_group.attributes), 0)

        # Test invalid argument: shape1
        self.assertRaises(TypeError, self.ds.root_group.AddVariable, 'var1', float_type, [None])
        self.assertEqual(len(self.ds.root_group.variables), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddVariable, 'var1', float_type, [lat, None])
        self.assertEqual(len(self.ds.root_group.variables), 0)
        self.assertRaises(AttributeError, self.ds.root_group.AddVariable, 'var1', float_type, [lat, 'not a dim obj'])
        self.assertEqual(len(self.ds.root_group.variables), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddVariable, 'var1', float_type, [lat, self.ds.root_group])
        self.assertEqual(len(self.ds.root_group.variables), 0)
        self.assertRaises(TypeError, self.ds.root_group.AddVariable, 'var1', float_type, 'not a list obj')
        self.assertEqual(len(self.ds.root_group.variables), 0)
        
        # Test legitimate arguments
        self.ds.root_group.AddVariable('var1', float_type, shape1)
        self.assertEqual(len(self.ds.root_group.variables), 1)
        self.assertEqual(self.ds.root_group.variables[0].name, 'var1')
        self.assertEqual(self.ds.root_group.variables[0].data_type, float_type)
        self.assertEqual(len(self.ds.root_group.variables[0].shape), len(shape1))
        for i in range(0, len(shape1)):
            self.assertEqual(self.ds.root_group.variables[0].shape[i], shape1[i])
        
        # Ensure more than one variable can be created
        self.ds.root_group.AddVariable('var2', int_type, shape2)
        self.assertEqual(len(self.ds.root_group.variables), 2)
        self.assertEqual(self.ds.root_group.variables[1].name, 'var2')
        self.assertEqual(self.ds.root_group.variables[1].data_type, int_type)
        self.assertEqual(len(self.ds.root_group.variables[1].shape), len(shape2))
        for i in range(0, len(shape2)):
            self.assertEqual(self.ds.root_group.variables[1].shape[i], shape2[i])
        
        # Ensure variables can be created without shape1 (scalars)
        self.ds.root_group.AddVariable('var3', int_type, None)
        self.assertEqual(len(self.ds.root_group.variables), 3)
        self.assertEqual(self.ds.root_group.variables[2].name, 'var3')
        self.assertEqual(self.ds.root_group.variables[2].data_type, int_type)
        self.assertEqual(len(self.ds.root_group.variables[2].shape), 0)
        
        # @todo: Test creation of preexisting variables
#        self.ds.root_group.AddDimension('dim2', 12, False)
        
    def test_FindGroupByName(self):
        # Test invalid attributes: name
        self.assertRaises(TypeError,      self.ds.root_group.FindGroupByName, 5)
        self.assertRaises(TypeError,      self.ds.root_group.FindGroupByName, None)
        self.assertRaises(ValueError,     self.ds.root_group.FindGroupByName, '')
        self.assertRaises(OOIObjectError, self.ds.root_group.FindGroupByName, 'non-existant')
        # @todo: Should we instead simply return None??

        # Seed the root_group with data to query        
        self.assertEqual(len(self.ds.root_group.groups), 0)
        grp1 = self.ds.root_group.AddGroup('new_group1')
        grp2 = self.ds.root_group.AddGroup('new_group2')
        self.assertEqual(len(self.ds.root_group.groups), 2)
        
        # Test legitimate query
        res_grp1 = self.ds.root_group.FindGroupByName('new_group1')
        res_grp2 = self.ds.root_group.FindGroupByName('new_group2')
        
        self.assertIdentical(grp1, res_grp1)
        self.assertIdentical(grp2, res_grp2)

    def test_FindAttributeByName_in_group(self):
        # Test invalid attributes: name
        self.assertRaises(TypeError,      self.ds.root_group.FindAttributeByName, 5)
        self.assertRaises(TypeError,      self.ds.root_group.FindAttributeByName, None)
        self.assertRaises(ValueError,     self.ds.root_group.FindAttributeByName, '')
        self.assertRaises(OOIObjectError, self.ds.root_group.FindAttributeByName, 'non-existant')
        # @todo: Should we instead simply return None??

        # Seed the root_group with data to query
        string_vals = ['val1', 'val2', 'val3']
        int_vals    = [123456, 987654, 0]
        string_type = self.ds.root_group.DataType.STRING
        int_type    = self.ds.root_group.DataType.INT
        self.assertEqual(len(self.ds.root_group.attributes), 0)
        obj1 = self.ds.root_group.AddAttribute('atrib1', string_type ,string_vals)
        obj2 = self.ds.root_group.AddAttribute('atrib2', int_type, int_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 2)
        
        # Test legitimate query
        res1 = self.ds.root_group.FindAttributeByName('atrib1')
        res2 = self.ds.root_group.FindAttributeByName('atrib2')
        
        self.assertIdentical(obj1, res1)
        self.assertIdentical(obj2, res2)

    def test_FindAttributeByName_in_variable(self):
        # Seed the root_group with a variable to attach attributes to...
        DT = self.ds.root_group.DataType
        tau  = self.ds.root_group.AddDimension('time', 10, True)
        lat  = self.ds.root_group.AddDimension('lat', 6, False)
        lon  = self.ds.root_group.AddDimension('lon', 25, False)
        variable = self.ds.root_group.AddVariable('var1', DT.FLOAT, [tau, lat, lon])
    
        self.assertEqual(len(variable.attributes), 0)
        
        # Test invalid attributes: name
        self.assertRaises(TypeError,      variable.FindAttributeByName, 5)
        self.assertRaises(TypeError,      variable.FindAttributeByName, None)
        self.assertRaises(ValueError,     variable.FindAttributeByName, '')
        self.assertRaises(OOIObjectError, variable.FindAttributeByName, 'non-existant')
        # @todo: Should we instead simply return None??

        # Seed the root_group with data to query
        string_vals = ['val1', 'val2', 'val3']
        int_vals    = [123456, 987654, 0]
        string_type = variable.DataType.STRING
        int_type    = variable.DataType.INT
        self.assertEqual(len(variable.attributes), 0)
        obj1 = variable.AddAttribute('atrib1', string_type ,string_vals)
        obj2 = variable.AddAttribute('atrib2', int_type, int_vals)
        self.assertEqual(len(variable.attributes), 2)
        
        # Test legitimate query
        res1 = variable.FindAttributeByName('atrib1')
        res2 = variable.FindAttributeByName('atrib2')
        
        self.assertIdentical(obj1, res1)
        self.assertIdentical(obj2, res2)
        
    def test_FindDimensionByName_in_group(self):
        # Test invalid attributes: name
        self.assertRaises(TypeError,      self.ds.root_group.FindDimensionByName, 5)
        self.assertRaises(TypeError,      self.ds.root_group.FindDimensionByName, None)
        self.assertRaises(ValueError,     self.ds.root_group.FindDimensionByName, '')
        self.assertRaises(OOIObjectError, self.ds.root_group.FindDimensionByName, 'non-existant')
        # @todo: Should we instead simply return None??

        # Seed the root_group with data to query
        self.assertEqual(len(self.ds.root_group.dimensions), 0)
        obj1 = self.ds.root_group.AddDimension('dim1', 25, True)
        obj2 = self.ds.root_group.AddDimension('dim2', 130, False)
        self.assertEqual(len(self.ds.root_group.dimensions), 2)
        
        # Test legitimate query
        res1 = self.ds.root_group.FindDimensionByName('dim1')
        res2 = self.ds.root_group.FindDimensionByName('dim2')
        
        self.assertIdentical(obj1, res1)
        self.assertIdentical(obj2, res2)
    
    def test_FindDimensionByName_in_variable(self):
        # Seed the root_group with a variable to attach attributes to...
        DT = self.ds.root_group.DataType
        tau  = self.ds.root_group.AddDimension('time', 10, True)
        lat  = self.ds.root_group.AddDimension('lat', 6, False)
        lon  = self.ds.root_group.AddDimension('lon', 25, False)
        variable = self.ds.root_group.AddVariable('var1', DT.FLOAT, [tau, lat, lon])
    
        self.assertEqual(len(variable.attributes), 0)
        
        # Test invalid attributes: name
        self.assertRaises(TypeError,      variable.FindDimensionByName, 5)
        self.assertRaises(TypeError,      variable.FindDimensionByName, None)
        self.assertRaises(ValueError,     variable.FindDimensionByName, '')
        self.assertRaises(OOIObjectError, variable.FindDimensionByName, 'non-existant')
        # @todo: Should we instead simply return None??

        self.assertEqual(len(variable.shape), 3)
        
        # Test legitimate query
        res1 = variable.FindDimensionByName('time')
        res2 = variable.FindDimensionByName('lat')
        res3 = variable.FindDimensionByName('lon')
        
        self.assertIdentical(res1, tau)
        self.assertIdentical(res2, lat)
        self.assertIdentical(res3, lon)
    
    
    def test_FindVariableByName(self):
        # Test invalid attributes: name
        self.assertRaises(TypeError,      self.ds.root_group.FindVariableByName, 5)
        self.assertRaises(TypeError,      self.ds.root_group.FindVariableByName, None)
        self.assertRaises(ValueError,     self.ds.root_group.FindVariableByName, '')
        self.assertRaises(OOIObjectError, self.ds.root_group.FindVariableByName, 'non-existant')
        # @todo: Should we instead simply return None??

        # Seed the root_group with data to query
        self.assertEqual(len(self.ds.root_group.variables), 0)

        tau = self.ds.root_group.AddDimension('time', 10, True)
        lat = self.ds.root_group.AddDimension('lat', 6, False)
        lon = self.ds.root_group.AddDimension('lon', 25, False)
        shape1 = [tau, lat, lon]
        shape2 = [lat, lon]
        float_type = self.ds.root_group.DataType.FLOAT
        int_type   = self.ds.root_group.DataType.INT
        
        obj1 = self.ds.root_group.AddVariable('var1', float_type, shape1)
        obj2 = self.ds.root_group.AddVariable('var2', int_type, shape2)
        self.assertEqual(len(self.ds.root_group.variables), 2)
        
        # Test legitimate query
        res1 = self.ds.root_group.FindVariableByName('var1')
        res2 = self.ds.root_group.FindVariableByName('var2')
        
        self.assertIdentical(obj1, res1)
        self.assertIdentical(obj2, res2)

    
    @SkipTest
    def test_FindVariableIndexByName(self):
        """
        """

    @SkipTest
    def test_FindAttributeIndexByName_for_group(self):
        """
        FindAttributeIndexByName is transitively tested by
        test_SetAttribute* methods.  This is sufficient testing for now
        """

    @SkipTest
    def test_FindAttributeIndexByName_for_variable(self):
        """
        FindAttributeIndexByName is transitively tested by
        test_SetAttribute* methods.  This is sufficient testing for now
        """
    
    def test_SetAttribute_for_group(self):
        # Seed the group with an attribute for testing
        string_vals = ['val1', 'val2', 'val3']
        int_vals    = [123456, 987654, 0]
        string_type = self.ds.root_group.DataType.STRING
        int_type    = self.ds.root_group.DataType.INT
        
        atr1 = self.ds.root_group.AddAttribute('atrib1', string_type, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertEqual(self.ds.root_group.attributes[0].name, 'atrib1')
        self.assertEqual(self.ds.root_group.attributes[0].array.value, string_vals)
        self.assertEqual(self.ds.root_group.attributes[0].data_type, string_type)
        
        # Test invalid argument: name
        self.assertRaises(TypeError, self.ds.root_group.SetAttribute, 5, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertIdentical(self.ds.root_group.attributes[0], atr1)
        self.assertRaises(TypeError, self.ds.root_group.SetAttribute, None, string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertIdentical(self.ds.root_group.attributes[0], atr1)
        self.assertRaises(ValueError, self.ds.root_group.SetAttribute, '', string_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertIdentical(self.ds.root_group.attributes[0], atr1)
        
        # Test invalid argument: values
        mixed_vals = ['val1', 'val2', 123, None, 3.2]
        self.assertRaises(ValueError, self.ds.root_group.SetAttribute, 'atrib1', 25)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertIdentical(self.ds.root_group.attributes[0], atr1)
        # @todo: Ensure entries for "values" of an empty list will fail
#        self.assertRaises(TypeError, self.ds.root_group.SetAttribute, 'atrib1', [])
#        self.assertEqual(len(self.ds.root_group.attributes), 0)
        self.assertRaises(ValueError, self.ds.root_group.SetAttribute, 'atrib1', None)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertIdentical(self.ds.root_group.attributes[0], atr1)
        # @warning: Must Implement...
        #           If SetAttribute() fails mid-swing the method may have removed the old
        #           attribute with the intent of replacing it in step 2.  Since this it
        #           not an atomic action, stage-2 failure may result in placing the
        #           dataset in an invalid state.  This must be prevented before the
        #           following check will succeed
        self.assertRaises(ValueError, self.ds.root_group.SetAttribute, 'atrib1', mixed_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertEqual(self.ds.root_group.attributes[0], atr1)
        
#        # Test legitimate arguments
        string_vals2 = ['new1', 'new2', 'new3']
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertEqual(self.ds.root_group.attributes[0], atr1)

        self.ds.root_group.SetAttribute(atr1.name, string_vals2)
        self.assertEqual(len(self.ds.root_group.attributes), 1)
        self.assertNotIdentical(self.ds.root_group.attributes[0], atr1)
        self.assertEqual(self.ds.root_group.attributes[0].array.value, string_vals2)
        
        # Ensure DAG integrity (setting one group's attribute to a new set of values
        #                       when another group contains a reference to that same
        #                       attribute should not effect the second group
        #------------------------------------------------------------------------------
        # Step 1: Create two groups each with identical attributes (by memory reference)
        int_vals2 = [434233, 403030, 1]
        grp1 = self.ds.root_group.AddGroup('group1')
        grp2 = self.ds.root_group.AddGroup('group2')
        self.assertEqual(len(self.ds.root_group.groups), 2)
        self.assertNotIdentical(grp1, grp2)
        
        shared_atrib = grp1.AddAttribute('dag1', int_type, int_vals)
        atr_ref = grp2.attributes.add()
        atr_ref.SetLink(shared_atrib)
        self.assertEqual(len(grp1.attributes), 1)
        self.assertEqual(len(grp2.attributes), 1)
        self.assertIdentical(grp1.attributes[0], grp2.attributes[0])
        
        # Step 2: Use SetAttribute() on one of the groups for the shared attribute
        grp1.SetAttribute(shared_atrib.name, int_vals2)
        self.assertEqual(len(grp1.attributes), 1)
        self.assertEqual(grp1.attributes[0].array.value, int_vals2)
        
        # Step 3: Ensure the second group's attribute was untouched and the two groups
        #         now point to separate attributes
        self.assertNotIdentical(grp1.attributes[0], shared_atrib)
        self.assertIdentical(grp2.attributes[0], shared_atrib)
        # thus we can infer grp1.atr[0] != grp2.atr[0]
        
    @SkipTest
    def test_SetDimension_for_group(self):
        """
        """
        # Seed the root_group with dimension objects
        tau  = self.ds.root_group.AddDimension('time', 10, True)
        lat  = self.ds.root_group.AddDimension('lat', 6, False)
        lon  = self.ds.root_group.AddDimension('lon', 25, False)
        
        # Test invalid argument: name
        self.assertRaises(TypeError, self.ds.root_group.SetDimension, 5, length=1)
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        self.assertRaises(TypeError, self.ds.root_group.SetDimension, None, length=1)
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        self.assertRaises(ValueError, self.ds.root_group.SetDimension, '', length=1)
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        
        # Test invalid argument: length
        self.assertRaises(ValueError, self.ds.root_group.SetDimension, tau.name)
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        self.assertRaises(TypeError, self.ds.root_group.SetDimension, tau.name, None)
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        self.assertRaises(TypeError, self.ds.root_group.SetDimension, tau.name, 'bad length')
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        self.assertRaises(ValueError, self.ds.root_group.SetDimension, tau.name, -4)
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        
        # Test changing a dimension where variable_length == false
        self.assertRaises(ValueError, self.ds.root_group.SetDimension, lat.name, int(lat.length + 1))
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        
        # Test Legitimate arguments:
        new_length = int(tau.length + 5)
        self.assertRaises(ValueError, self.ds.root_group.SetDimension, tau.name, new_length)
        self.assertEqual(len(self.ds.root_group.dimensions), 3)
        self.assertIdentical(self.ds.root_group.dimensions[0], tau)
        self.assertIdentical(self.ds.root_group.dimensions[1], lat)
        self.assertIdentical(self.ds.root_group.dimensions[2], lon)
        self.assertEqual(self.ds_root_group.dimensions[0].length, new_length)
        
        # Ensure DAG integrity (Changing one group's dimension when another group
        #                       contains a reference to that same dimension should
        #                       not effect the second group
        #------------------------------------------------------------------------------
        # Step 1: Create two groups each with identical dimensions (by memory reference)
        grp1 = self.ds.root_group.AddGroup('group1')
        grp2 = self.ds.root_group.AddGroup('group2')
        self.assertEqual(len(self.ds.root_group.groups), 2)
        self.assertNotIdentical(grp1, grp2)
        
        shared_dim = grp1.AddDimension('shared', 8, True)
        dim_ref = grp2.dimensions.add()
        dim_ref.SetLink(shared_dim)
        
        self.assertEqual(len(grp1.dimensions), 1)
        self.assertEqual(len(grp2.dimensions), 1)
        self.assertIdentical(grp1.dimensions[0], grp2.dimensions[0])
        
        # Step 2: Use SetDimension() on one of the groups for the shared dimension
        new_length = shared_dim.length = 20
        grp1.SetDimension(shared_dim.name, new_length)
        self.assertEqual(len(grp1.dimensions), 1)
        self.assertEqual(grp1.dimensions[0].length, new_length)
        
        # Step 3: Ensure the second group's dimension not only was updated with the
        #         new value but that both dimensions hold the same memory reference
        self.assertEqual(len(grp2.dimensions), 1)
        self.assertEqual(grp2.dimensions[0].length, new_length)
        self.assertIdentical(grp1.dimensions[0], grp2.dimensions[0])
        
    def test_GetValues_for_attribute(self):
        # Seed the root_group with an attribute or two
        string_vals = ['val1', 'val2', 'val3']
        int_vals    = [123456, 987654, 0]
        string_type = self.ds.root_group.DataType.STRING
        int_type    = self.ds.root_group.DataType.INT
        self.ds.root_group.AddAttribute('atrib1', string_type, string_vals)
        self.ds.root_group.AddAttribute('atrib2', int_type, int_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 2)
        
        # Test GetValues method:
        res1 = self.ds.root_group.FindAttributeByName('atrib1').GetValues()
        res2 = self.ds.root_group.FindAttributeByName('atrib2').GetValues()
        self.assertNotEqual(res1, res2)
        self.assertEqual(res1, string_vals)
        self.assertEqual(res2, int_vals)
        self.assertNotIdentical(res1, string_vals)
        self.assertNotIdentical(res2, int_vals)
        
    def test_GetValue_for_attribute(self):
        # Seed the root_group with an attribute or two
        string_vals = ['val1', 'val2', 'val3']
        int_vals    = [123456, 987654, 0]
        string_type = self.ds.root_group.DataType.STRING
        int_type    = self.ds.root_group.DataType.INT
        atr1 = self.ds.root_group.AddAttribute('atrib1', string_type, string_vals)
        atr2 = self.ds.root_group.AddAttribute('atrib2', int_type, int_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 2)
        
        # Test GetValue method:
        for i in range(len(string_vals)):
            val = atr1.GetValue(i)
            self.assertEqual(val, string_vals[i])
        for i in range(len(int_vals)):
            val = atr2.GetValue(i)
            self.assertEqual(val, int_vals[i])

    def test_GetLength_for_attribute_values(self):
        # Seed the root_group with an attribute or two
        string_vals = ['val1', 'val2', 'val3']
        int_vals    = [123456, 987654, 0]
        string_type = self.ds.root_group.DataType.STRING
        int_type    = self.ds.root_group.DataType.INT
        atr1 = self.ds.root_group.AddAttribute('atrib1', string_type, string_vals)
        atr2 = self.ds.root_group.AddAttribute('atrib2', int_type, int_vals)
        self.assertEqual(len(self.ds.root_group.attributes), 2)
        
        # Test GetLength method:
        self.assertEqual(atr1.GetLength(), len(string_vals))
        self.assertEqual(atr2.GetLength(), len(int_vals))
    
    def test_GetUnits_for_variable(self):
        # Seed the root_group with a variable and necessary attributes
        DT = self.ds.root_group.DataType
        tau  = self.ds.root_group.AddDimension('time', 10, True)
        lat  = self.ds.root_group.AddDimension('lat', 6, False)
        lon  = self.ds.root_group.AddDimension('lon', 25, False)
        var1 = self.ds.root_group.AddVariable('var1', DT.FLOAT, [tau, lat, lon])
        var2 = self.ds.root_group.AddVariable('var2', DT.INT, [lat, lon])
        
        var1.AddAttribute('units',       DT.STRING, ['meters'])
        var1.AddAttribute('middle_name', DT.STRING, ['bad'])
        var1.AddAttribute('in-my-car',   DT.STRING, ['stuff'])
        var2.AddAttribute('units',       DT.STRING, ['pancakes'])
        var2.AddAttribute('snacks',      DT.STRING, ['ring-dings'])
        var2.AddAttribute('lunch',       DT.STRING, ['cake'])
        
        # Test GetUnits
        result1 = var1.GetUnits()
        delicious = var2.GetUnits()
        self.assertEqual('meters', result1)
        self.assertEqual('pancakes', delicious)
    
    def test_GetStandardName_for_variable(self):
        # Seed the root_group with a variable and necessary attributes
        DT = self.ds.root_group.DataType
        tau  = self.ds.root_group.AddDimension('time', 10, True)
        lat  = self.ds.root_group.AddDimension('lat', 6, False)
        lon  = self.ds.root_group.AddDimension('lon', 25, False)
        var1 = self.ds.root_group.AddVariable('var1', DT.FLOAT, [tau, lat, lon])
        var2 = self.ds.root_group.AddVariable('var2', DT.INT, [lat, lon])
        
        var1.AddAttribute('standard_name', DT.STRING, ['variable_senior'])
        var1.AddAttribute('units',         DT.STRING, ['meters'])
        var1.AddAttribute('middle_name',   DT.STRING, ['bad'])
        var1.AddAttribute('in-my-car',     DT.STRING, ['stuff'])
        var2.AddAttribute('standard_name', DT.STRING, ['variable_junior'])
        var2.AddAttribute('units',         DT.STRING, ['pancakes'])
        var2.AddAttribute('snacks',        DT.STRING, ['ring-dings'])
        var2.AddAttribute('lunch',         DT.STRING, ['cake'])
        
        # Test GetUnits
        father = var1.GetStandardName()
        son = var2.GetStandardName()
        self.assertEqual('variable_senior', father)
        self.assertEqual('variable_junior', son)
        
        
class TestWrapperMethodsRequiringRepository(unittest.TestCase):
    
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        
        repo, ab = wb.init_repository(ADDRESSLINK_TYPE)
        
        self.repo = repo
        self.ab = ab
        self.wb = wb


    def test_inparents_1(self):

        person = self.ab.Repository.create_object(PERSON_TYPE)
        self.ab.owner = person

        self.failUnless(person.InParents(self.ab))
        self.failIf(self.ab.InParents(person))

    def test_inparents_2(self):

        person1 = self.ab.Repository.create_object(PERSON_TYPE)
        self.ab.person.add()
        self.ab.person[0] = person1

        person2 = self.ab.Repository.create_object(PERSON_TYPE)
        self.ab.person.add()
        self.ab.person[1] = person2

        self.failUnless(person1.InParents(self.ab))
        self.failIf(self.ab.InParents(person1))

        self.failIf(person2.InParents(person1))
        self.failIf(person1.InParents(person2))



    def test_listsetfields_composite(self):


        self.ab.title = 'foobar'

        flist = self.ab.ListSetFields()
        self.assertIn('title',flist)

        self.ab.owner = self.ab.Repository.create_object(PERSON_TYPE)

        flist = self.ab.ListSetFields()
        self.assertIn('title',flist)
        self.assertIn('owner',flist)

        self.ab.person.add()
        self.ab.person[0] = self.ab.Repository.create_object(PERSON_TYPE)

        flist = self.ab.ListSetFields()
        self.assertIn('title',flist)
        self.assertIn('owner',flist)
        self.assertIn('person',flist)

        self.assertEqual(len(flist),3)



    def test_isfieldset(self):



        self.assertEqual(self.ab.IsFieldSet('title'),False)
        self.ab.title = 'foobar'
        self.assertEqual(self.ab.IsFieldSet('title'),True)

        self.assertEqual(self.ab.IsFieldSet('owner'),False)
        self.ab.owner = self.ab.Repository.create_object(PERSON_TYPE)
        self.assertEqual(self.ab.IsFieldSet('owner'),True)


        self.assertEqual(self.ab.IsFieldSet('person'),False)
        self.ab.person.add()
        self.assertEqual(self.ab.IsFieldSet('person'),False)
        self.ab.person[0] = self.ab.Repository.create_object(PERSON_TYPE)
        self.assertEqual(self.ab.IsFieldSet('person'),True)



    def test_clearfield(self):
        """
        Test clear field and the object accounting for parents and children
        """
        self.ab.person.add()
        p = self.repo.create_object(PERSON_TYPE)
        p.name = 'David'
        p.id = 5
        self.ab.person[0] = p
        self.ab.owner = p
        
        # Check to make sure all is set in the data structure
        self.assertEqual(self.ab.IsFieldSet('owner'),True)
        
        # Assert that there is a child link
        self.assertIn(self.ab.GetLink('owner'),self.ab.ChildLinks)
        self.assertIn(self.ab.person.GetLink(0),self.ab.ChildLinks)
        self.assertEqual(len(self.ab.ChildLinks),2)
        
        # Assert that there is a parent link
        self.assertIn(self.ab.GetLink('owner'),p.ParentLinks)
        self.assertIn(self.ab.person.GetLink(0),p.ParentLinks)
        self.assertEqual(len(p.ParentLinks),2)
        
        # Get the link object which will be cleared
        owner_link = self.ab.GetLink('owner').GPBMessage
        owner_type = self.ab.GetLink('owner').type.GPBMessage

        # Assert that the derived wrappers dictionary contains these objects
        self.assertIn(owner_link, self.ab.DerivedWrappers)
        self.assertIn(owner_type, self.ab.DerivedWrappers)
        
        # ***Clear the field***
        self.ab.ClearField('owner')
        
        # The field is clear
        self.assertEqual(self.ab.IsFieldSet('owner'),False)
        
        # Assert that there is only one child link
        self.assertIn(self.ab.person.GetLink(0),self.ab.ChildLinks)
        self.assertEqual(len(self.ab.ChildLinks),1)
        
        # Assert that there is only one parent link
        self.assertIn(self.ab.person.GetLink(0),p.ParentLinks)
        self.assertEqual(len(p.ParentLinks),1)


        # Assert that the derived wrappers refs are gone!
        self.assertNotIn(owner_link, self.ab.DerivedWrappers)
        self.assertNotIn(owner_type, self.ab.DerivedWrappers)
        
        # Now try removing the person
        
        # Get the person link in the composite container
        p0_link = self.ab.person.GetLink(0).GPBMessage
        p0_type = self.ab.person.GetLink(0).type.GPBMessage
        
        # Assert that the derived wrappers dictionary contains these objects
        self.assertIn(p0_link, self.ab.DerivedWrappers)
        self.assertIn(p0_type, self.ab.DerivedWrappers)
        
        # ***Clear the field***
        self.ab.ClearField('person')
        
        # The field is clear
        self.assertEqual(len(self.ab.person),0)
            
        # Assert that there are zero parent links
        self.assertEqual(len(p.ParentLinks),0)
        
        # Assert that there are zero child links
        self.assertEqual(len(self.ab.ChildLinks),0)
        
        # Assert that the derived wrappers dictionary is empty!
        self.assertNotIn(p0_link, self.ab.DerivedWrappers)
        self.assertNotIn(p0_type, self.ab.DerivedWrappers)
        self.assertEqual(len(self.ab.DerivedWrappers),1)

        

class NodeLinkTest(unittest.TestCase):
            
            
        def setUp(self):
            wb = workbench.WorkBench('No Process Test')
            
            repo, ab = wb.init_repository(ADDRESSLINK_TYPE)
            
            self.repo = repo
            self.ab = ab
            self.wb = wb
            
        def test_link(self):
            
            p = self.repo.create_object(PERSON_TYPE)
                        
            p.name = 'David'
            self.ab.owner = p
            self.assertEqual(self.ab.owner.name ,'David')
            
        def test_composite_link(self):
            
            wL = self.ab.person.add()
            
            self.assertEqual(wL.ObjectType, LINK_TYPE)
            
            p = self.repo.create_object(PERSON_TYPE)
            
            p.name = 'David'
            
            self.ab.person[0] = p
            
            self.assertEqual(self.ab.person[0].name ,'David')
            
        def test_dag(self):
            
            self.ab.person.add()
            
            p = self.repo.create_object(PERSON_TYPE)
            
            p.name = 'David'
            p.id = 5
            
            self.ab.person[0] = p
            
            self.ab.owner = p
            
            p.name ='John'
            
            self.assertEqual(self.ab.person[0].name ,'John')
            self.assertEqual(self.ab.owner.name ,'John')
                        

            
class RecurseCommitTest(unittest.TestCase):
        
            
    def test_simple_commit(self):
        wb = workbench.WorkBench('No Process Test')
            
        repo, ab = wb.init_repository(ADDRESSLINK_TYPE)
        
        ab.person.add()
        
        p = repo.create_object(PERSON_TYPE)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '123 456 7890'
        
        ab.person[0] = p
        
        ab.owner = p
            
        strct={}
            
        ab.RecurseCommit(strct)
        
        self.assertIn(ab.MyId, strct)
        self.assertIn(p.MyId, strct)
        
        # Get the committed structure element
        ab_se = strct.get(ab.MyId)
        
        self.assertEqual(len(ab_se.ChildLinks),1)
        self.assertIn(p.MyId, ab_se.ChildLinks)
        
    def test_dag_conflict(self):
        
        wb = workbench.WorkBench('No Process Test')
            
        repo = wb.create_repository(ADDRESSLINK_TYPE)
        
        repo.root_object.person.add()
        repo.root_object.person[0] = repo.create_object(PERSON_TYPE)
        repo.root_object.person[0].name = 'David'
        repo.root_object.person[0].id = 5
        
        repo.root_object.person.add()
        repo.root_object.person[1] = repo.create_object(PERSON_TYPE)
        repo.root_object.person[1].name = 'David'
        repo.root_object.person[1].id = 5
        
        p0 = repo.root_object.person[0]
        p1 = repo.root_object.person[1]
        
        strct={}
        repo.root_object.RecurseCommit(strct)
        
        
        # The address link should now be unmodified
        self.assertEqual(repo.root_object.Modified, False)
        
        # there should be only two objects once hashed!
        self.assertEqual(len(strct), 2)
        
        # Show that the old references are not invalid
        self.assertEqual(p0.Invalid, False)
        self.assertEqual(p1.Invalid, False)

        print p0.Debug()
        print p1.Debug()


        self.assertEqual(p0.name,'David')
        self.assertEqual(p1.name,'David')

        if p0._invalid:
            self.failUnlessIdentical(p0._source, p1)
        else:
            self.failUnlessIdentical(p1._source, p0)


        self.failUnlessIdentical(p0.ParentLinks, p1.ParentLinks)
        self.failUnlessIdentical(p0.Repository, p1.Repository)
        self.failUnlessIdentical(p0.GPBMessage, p1.GPBMessage)
        self.failUnlessIdentical(p0.DerivedWrappers, p1.DerivedWrappers)
        self.failUnlessIdentical(p0.MyId, p1.MyId)
        self.failUnlessIdentical(p0.Root, p1.Root)
        self.failUnlessIdentical(p0.ChildLinks, p1.ChildLinks)
        self.failUnlessIdentical(p0.ReadOnly, p1.ReadOnly)
        self.failUnlessIdentical(p0.Modified, p1.Modified)


        # manually update the hashed elements...
        repo.index_hash.update(strct)
        
        self.assertIn(repo.root_object.MyId, strct)
        self.assertIn(repo.root_object.person[0].MyId, strct)
        self.assertIn(repo.root_object.person[1].MyId, strct)
            
        self.assertIdentical(repo.root_object.person[0], repo.root_object.person[1])
            
        # Get the committed structure element
        ab_se = strct.get(repo.root_object.MyId)
            
        # Show that the the SE recongnized only 1 child object
        self.assertEqual(len(ab_se.ChildLinks),1)
            
        # There should be two parent links
        self.assertEqual(len(repo.root_object.person[0].ParentLinks), 2)
        
        par1 = repo.root_object.person[0].ParentLinks.pop()
        par2 = repo.root_object.person[0].ParentLinks.pop()
        
        # They are not identical - they came from a set, but they should be equal!
        self.assertEqual(par1, par2)
        
        # Their root should be identical, the addresslink!
        self.assertIdentical(par1.Root, repo.root_object)
        self.assertIdentical(par1.Root, par2.Root)
        
        
    def test_reset_same_value(self):
        
        wb = workbench.WorkBench('No Process Test')
            
        repo = wb.create_repository(ADDRESSLINK_TYPE)
        
        repo.root_object.person.add()
        repo.root_object.person[0] = repo.create_object(PERSON_TYPE)
        repo.root_object.person[0].name = 'David'
        repo.root_object.person[0].id = 5
        
        repo.commit('Jokes on me')
        
        repo.root_object.person[0].name = 'David'
        
        self.assertEqual(repo.root_object.Modified, True)
        
        p0 = repo.root_object.person[0]
        
        repo.commit('Jokes on you!')

        self.assertEqual(repo.root_object.Modified, False)
        self.assertEqual(p0.Invalid, False)
        self.assertEqual(p0.Modified, False)
        
        
        
            
