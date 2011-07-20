#!/usr/bin/env python
"""
@file ion/core/object/cdm_methods/attribute.py
@brief Wrapper methods for the cdm attribute object
@author David Stuebe
@author Tim LaRocque
TODO:
"""

# Get the object decorator used on wrapper methods!
from ion.core.object.object_utils import _gpb_source


from ion.core.object.object_utils import OOIObjectError
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)



#---------------------------------------#
# Wrapper_Attribute Specialized Methods #
#---------------------------------------#
@_gpb_source
def _get_attribute_value_by_index(self, index = 0):
    """
    Specialized method for CDM Objects to find an attribute value by its index
    """
    if not isinstance(index, int):
        raise OOIObjectError('Invalid array index requested: "%s"' % str(index))
    # @todo: determine if you can have an empty array -- if so, check for empty here
    if index < 0 or index >= len(self.array.value):
        raise OOIObjectError('Given array index out of bounds: %i -- valid range: 0 to %i' % (int(index), len(self.array.value) - 1))

    if self.data_type == self.DataType.STRING:
        return uni(self.array.value[index])
    else:
        return self.array.value[index]

@_gpb_source
def _get_attribute_values(self):
    """
    Specialized method for CDM Objects to retreive all attribute values as a string list
    """
    # Create a copy of the values array
    if self.data_type == self.DataType.STRING:
        result = [uni(item) for item in self.array.value]
    else:
        result = [item for item in self.array.value]
    return result

@_gpb_source
def _get_attribute_values_length(self):
    """
    Specialized method for CDM Objects to find the length of an attribute object's values
    """
    return len(self.array.value)

@_gpb_source
def _get_attribute_data_type(self):
    """
    Specialized method for CDM Objects to retrieve the attribute data_type as a long.
    This value can be used to compare equality with other attributes' data_types
    """
    return self.data_type

@_gpb_source
def _attribute_is_same_type(self, attribute):
    if not hasattr(attribute, 'GetDataType'):
        raise TypeError('The datatype of the given attribute cannot be found.:  Please specify an instance of "%s".  Recieved "%s"' % (type(self), type(attribute)))

    return self.GetDataType() == attribute.GetDataType()


def uni(text):
    
    try:

        try:
            return text.decode('utf-8')               # Most likely
        except UnicodeEncodeError:
            try:
                return text.decode('latin-1')         # Windows

            except UnicodeEncodeError:

                try:
                    return text.decode('mac-roman')       # Mac

                except UnicodeEncodeError:
                    return text.decode('iso-8859-1')       # iso 8859



    except Exception, uee:

        log.warn('Sanitizing attribute character encoding failed!!!!\n%s' % str(uee))
        return '*** Unknown character encoding in attribute string ***'