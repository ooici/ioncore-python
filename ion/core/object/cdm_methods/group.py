#!/usr/bin/env python
"""
@file ion/core/object/cdm_methods/group.py
@brief Wrapper methods for the cdm group object
@author David Stuebe
@author Tim LaRocque
TODO:
"""


# Get the object decorator used on wrapper methods!
from ion.core.object.object_utils import _gpb_source

from ion.core.object.object_utils import OOIObjectError, CDM_GROUP_TYPE, CDM_ATTRIBUTE_TYPE, CDM_VARIABLE_TYPE, CDM_DIMENSION_TYPE, CDM_ARRAY_UINT32_TYPE, CDM_ARRAY_INT32_TYPE, CDM_ARRAY_INT64_TYPE, CDM_ARRAY_FLOAT32_TYPE, CDM_ARRAY_FLOAT64_TYPE, CDM_ARRAY_STRING_TYPE
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


#-----------------------------------#
# Wrapper_Group Specialized Methods #
#-----------------------------------#
@_gpb_source
def _add_group_to_group(self, name=''):
    """
    Specialized method for CDM (group) Objects to append a group object with the given name
    @return: The group being created (as a convenience)
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name":  Please specify a non-empty string')

    group = self.Repository.create_object(CDM_GROUP_TYPE)
    group.name = name
    group_ref = self.groups.add()
    group_ref.SetLink(group)

    return group

@_gpb_source
def _add_attribute(self, name, data_type, values):
    """
    Specialized method for CDM (group) Objects to append a group object with the given name
    @param name: The intented name of the attribute to be appended
    @param values: A List of primitives to be stored as this attributes value.
    @param data_type: A Value from the DataType enum indicating how the 'values' argument should be stored
    @return: The attribute being created (as a convenience)
    """
    # @attention: Should we allow an empty list of values for an attribute?
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
    if values is None or (isinstance(values, list) and 0 == len(values)):
        raise ValueError('Invalid argument "values" -- Please specify a non-empty list')
    if not data_type or not isinstance(data_type, (int, long)):
        raise TypeError('Type mismatch for argument "data_type" -- Expected int or long; received %s with value: "%s"' % (type(data_type), str(data_type)))

    if not isinstance(values, list):
        values = [values]

    # @todo: all items must be the same type...  this includes ommiting/casting null values
    #        since they will cause an error when stored in the GPB array representation
#            list_type = types.NoneType
#            for item in values:
#                if list_type == types.NoneType:
#                    # Determine the type of values in this list...
#                    list_type = type(item)
#                else:
#                    # ...and ensure all values are the same type or None
#                    next_type = type(item)
#                    if next_type != types.NoneType and next_type != list_type:
#                        raise OOIObjectError('Invalid attribute value list: "%s" -- All items in this list must be of the same type or the value None' % str(values))
#            log.debug('Type of list is "%s" for list: "%s"' % (str(list_type), str(values)))

    # Create the new attribute
    atrib = self.Repository.create_object(CDM_ATTRIBUTE_TYPE)
    atrib.name = name
    atrib.data_type = data_type

    # Set the datatype based on the type of values being given
    # @todo: add support for remaining array types (currently only string attributes are even used)
    # @todo: because many object types are not support fully here -- where should we put value checking...
    #        ex: since bool is stored as int, prevent any value other than 0 and 1
    def _attach_byte_array(parent, atrib_inst):
        atrib_inst.data_type = atrib_inst.DataType.BYTE
        atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_UINT32_TYPE)
    def _attach_short_array(parent, atrib_inst):
        atrib_inst.data_type = atrib_inst.DataType.SHORT
        atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_INT32_TYPE)
    def _attach_int32_array(parent, atrib_inst):
        atrib_inst.data_type = atrib_inst.DataType.INT
        atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_INT32_TYPE)
    def _attach_int64_array(parent, atrib_inst):
        atrib_inst.data_type = atrib_inst.DataType.LONG
        atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_INT64_TYPE)
    def _attach_float32_array(parent, atrib_inst):
        atrib_inst.data_type = atrib_inst.DataType.FLOAT
        atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_FLOAT32_TYPE)
    def _attach_float64_array(parent, atrib_inst):
        atrib_inst.data_type = atrib_inst.DataType.DOUBLE
        atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_FLOAT64_TYPE)
    def _attach_char_array(parent, atrib_inst): # CHAR doesnt exist in GPB
        atrib_inst.data_type = atrib_inst.DataType.CHAR
        atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_STRING_TYPE)
    def _attach_string_array(parent, atrib_inst):
        # @todo: modify this to support unicode types (GPB stringArray already supports it)
        atrib_inst.data_type = atrib_inst.DataType.STRING
        atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_STRING_TYPE)

    attach_array_definitions = {self.DataType.BYTE    : _attach_byte_array,
                                self.DataType.SHORT   : _attach_short_array,
                                self.DataType.INT     : _attach_int32_array,
                                self.DataType.LONG    : _attach_int64_array,
                                self.DataType.FLOAT   : _attach_float32_array,
                                self.DataType.DOUBLE  : _attach_float64_array,
                                self.DataType.CHAR    : _attach_char_array,
                                self.DataType.STRING  : _attach_string_array}

    attach_array_definitions[data_type](self, atrib)

    # Extend the attribute value array with the given values list
    try:
        atrib.array.value.extend(values)
    except TypeError, ex:
        raise ValueError('Parameter data_type (%s) is incompatible with the given values -- %s' % (str(data_type), str(ex)))

    # Attach the attribute resource instance to its parent resource via CASRef linking
    atrib_ref = self.attributes.add()
    atrib_ref.SetLink(atrib)

    return atrib

@_gpb_source
def _add_dimension(self, name, length=-1, variable_length=True):
    """
    Specialized method for CDM Objects to append a dimension object with the given name and length
    @return: The dimension being created (as a convenience)

    @warning: If some component of a dataset contains a dimension: say 'tau', and a dimension is
              later created anew for a differnt component which does not already contain it, we
              would effectively have two instances of the same dimension 'tau', which point to
              different in-memory objects.  This is illegal in this datastructure for dimension objects
              and should be prevented in further iterations.
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
    if not isinstance(length, int):
        raise TypeError('Type mismatch for argument "length" -- Expected %s; received %s with value: "%s"' % (repr(int), type(length), str(length)))
    if length <= 0:
        raise ValueError('Invalid argument "dimension": "%s" -- Please specify a positive integer' % str(length))
    if not isinstance(variable_length, bool):
        raise TypeError('Type mismatch for argument "variable_length" -- Expected %s; received %s with value: "%s"' % (type(bool), type(variable_length), str(variable_length)))


    dim = self.Repository.create_object(CDM_DIMENSION_TYPE)
    dim.name = name
    dim.length = length
    dim.variable_length = variable_length
    dim_ref = self.dimensions.add()
    dim_ref.SetLink(dim)

    return dim


@_gpb_source
def _add_variable(self, name, data_type, shape=None):
    """
    Specialized method for CDM Objects to append a variable object with the given name, data_type, and shape
    @return: The variable being created (as a convenience)
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
    # @todo: Find a better way to ensure DataType is a valid value in cdmdatatype enum
    if not data_type or not isinstance(data_type, int):
        raise TypeError('Type mismatch for argument "data_type" -- Expected %s; received %s with value: "%s"' % (repr(int), type(data_type), str(data_type)))


    var = self.Repository.create_object(CDM_VARIABLE_TYPE)
    var.name = name
    var.data_type = data_type

    # @note: shape is allowed to be null for scalar variables
    if shape is not None:
        if not isinstance(shape, list):
            raise TypeError('Type mismatch for argument "shape" -- Expected %s; received %s with value: "%s"' % (repr(list), type(shape), str(shape)))
        for dim in shape:
            if dim is None:
                raise TypeError('Invalid shape given -- encountered null entries: "%s".  ' % str(shape))
            elif not hasattr(dim, 'ObjectType'):
                raise AttributeError('Type mismatch for value in argument "shape" -- Could not access method ObjectType for object "%s" of type "%s"' % (str(dim), type(dim)))
            elif dim.ObjectType != CDM_DIMENSION_TYPE:
                raise TypeError('Type mismatch for value in argument "shape" -- Expected %s; received %s with value: "%s"' % (repr(CDM_DIMENSION_TYPE), repr(dim.ObjectType), str(dim)))
            else:
                # Add this dimension to the variable's shape!
                dim_ref = var.shape.add()
                dim_ref.SetLink(dim)

    var_ref = self.variables.add()
    var_ref.SetLink(var)

    return var


@_gpb_source
def _find_group_by_name(self, name=''):
    """
    Specialized method for CDM Objects to find the group object by its name
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

    result = None
    for group in self.groups:
        if group.name == name:
            result = group
            break
    if None == result:
        raise OOIObjectError('Requested group name not found: "%s"' % str(name))

    return result


@_gpb_source
def _find_attribute_by_name(self, name):
    """
    Specialized method for CDM Objects to find the attribute object by its name
    """
    # @attention: Should this find operate on the variable's standard_name attribute when avail?
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

    result = None
    for att in self.attributes:
        if att.name == name:
            result = att
            break
    if None == result:
        raise OOIObjectError('Requested attribute name not found: "%s"' % str(name))

    return result


@_gpb_source
def _find_dimension_by_name(self, name):
    """
    Specialized method for CDM Objects to find a dimension object by its name
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

    result = None

    if self.ObjectType == CDM_VARIABLE_TYPE:
        for dim in self.shape:
            if dim.name == name:
                result = dim
                break
    else:
        for dim in self.dimensions:
            if dim.name == name:
                result = dim
                break

    if None == result:
        raise OOIObjectError('Requested dimension name not found: "%s"' % str(name))

    return result


@_gpb_source
def _find_variable_by_name(self, name):
    """
    Specialized method for CDM Objects to find the variable object by its name
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

    result = None
    for var in self.variables:
        if var.name == name:
            result = var
            break
    if None == result:
        raise OOIObjectError('Requested variable name not found: "%s"' % str(name))

    return result


@_gpb_source
def _find_variable_index_by_name(self, name):
    """
    Specialized method for CDM Objects to find the variable object's index by the variable's name
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

    result = -1
    for i in xrange(0, len(self.variables)):
        var = self.variables[i]
        if var is not None and var.name == name:
            result = i
            break

    if -1 == result:
        raise OOIObjectError('Requested variable not found: "%s"' % str(name))

    return result


@_gpb_source
def _find_attribute_index_by_name(self, name):
    """
    Specialized method for CDM Objects to find the attribute object's index by the attribute's name
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

    result = -1
    for i in xrange(0, len(self.attributes)):
        atrib = self.attributes[i]
        if atrib is not None and atrib.name == name:
            result = i
            break

    if -1 == result:
        raise OOIObjectError('Requested attribute not found: "%s"' % str(name))

    return result


@_gpb_source
def _cdm_resource_has_attribute(self, name):
    """
    Specialized method for CDM Objects to check existance of an attribute object by its name
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

    result = False
    for att in self.attributes:
        if att.name == name:
            result = True
            break

    return result


@_gpb_source
def _remove_attribute(self, name):
    """
    Removes an attribute with the given name from this CDM Object (GROUP)
    @return: The attribute which was removed as a convenience
    """
    idx = _find_attribute_index_by_name(self, name)
    atr = self.attributes.__getitem__(idx)
    self.attributes.__delitem__(idx)

    return atr


@_gpb_source
def _set_attribute(self, name, values, data_type=None):
    """
    Specialized method for CDM Objects to set values for existing attributes
    @raise ValueError: When setting an attribute to a different data_type, if
                       data_type is not explicitly specified a ValueError will be raised
    """
    # @attention: Should we allow an empty list of values for an attribute?
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
    if values is None or (isinstance(values, list) and 0 == len(values)):
        raise ValueError('Invalid argument "values", please specify a non-empty list')

    if not isinstance(values, list):
        values = [values]

    atr = _remove_attribute(self, name)
    data_type = data_type or atr.data_type
    try:
        _add_attribute(self, name, int(data_type), values)
        log.warn('Old references to the attribute "%s" are now detached and will not point to the new attribute value' % name)
    except Exception, ex:
        log.warn('WARNING! Exception may have left this resource in an invalid state')
        atr_link = self.attributes.add()
        atr_link.SetLink(atr)
        raise ex


@_gpb_source
def _set_dimension(self, name, length):
    """
    Specialized method for CDM Objects to set fields for existing dimensions
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if not name:
        raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
    if not isinstance(length, int):
        raise TypeError('Type mismatch for argument "length" -- Expected %s; received %s with value: "%s"' % (repr(int), type(length), str(length)))
    # @attention: Can a dimension have a length of zero??
    if length <= 0:
        raise ValueError('Invalid argument "dimension": "%s" -- Please specify a positive integer' % str(length))

    dim = _find_dimension_by_name(self, name)

    ### Variable Length is a NETCDF 4 feature which describes a ragged array.
    #if dim.variable_length and length != dim.length:
    #    raise OOIObjectError('Cannot change the length of a dimension when dimension.variable_length is set to False.  Old length: %s.  New length: %s.' % (str(dim.length), str(length)))

    dim.length = length


