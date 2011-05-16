#!/usr/bin/env python
"""
@file ion/core/object/cdm_methods/dataset.py
@brief Wrapper methods for the cdm dataset object
@author David Stuebe
@author Tim LaRocque
TODO:
"""

# Get the object decorator used on wrapper methods!
from ion.core.object.object_utils import _gpb_source

from ion.core.object.object_utils import OOIObjectError, CDM_GROUP_TYPE
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


#-------------------------------------#
# Wrapper_Dataset Specialized Methods #
#-------------------------------------#

@_gpb_source
def _make_root_group(self, name=''):
    """
    Specialized method for CDM (dataset) Objects to append a group object with the given name
    """
    if not isinstance(name, (str, unicode)):
        raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
    if self.root_group is not None:
        raise OOIObjectError('Cannot make the root group when one already exists!')

    group = self.Repository.create_object(CDM_GROUP_TYPE)
    group.name = name
    self.root_group = group


@_gpb_source
def _get_variable_names(self):
    """
    """
    def _recurse_get_variable_names(group):
        result = ""
        group_name = '"%s"' % str(group.name)
        for var in group.variables:
            var_name = '"%s"' % str(var.name)
            result += 'Group: %-20s Variable: %s\n' % (group_name, var_name)
        for inner_group in group.groups:
            result += _recurse_get_variable_names(inner_group)
        return result

    return _recurse_get_variable_names(self.root_group)

@_gpb_source
def _get_group_attributes_for_display(self, group=None):
    """
    """
    if group is None:
        group = self.root_group

    result = ""
    for atrib in group.attributes:
        name = str(atrib.name)
        vals = str(group.FindAttributeByName(name).GetValues())
        idx = int(group.FindAttributeIndexByName(name))
        result += "(%02i) %-35s%s\n" % (idx, name, vals)

    return result

@_gpb_source
def _get_variable_attributes_for_display(self):
    """
    """
    def _recurse_get_variable_atts(group, group_namespace=None):
        result = ""
        group_name = group_namespace or ""
        group_name += group.name
        for var in group.variables:
            result += "\n\n%s.%s" % (str(group_name), str(var.name))
            for atrib in var.attributes:
                name = str(atrib.name)
                vals = str(var.FindAttributeByName(name).GetValues())
                idx = var.FindAttributeIndexByName(name)
                result += "\n(%03i) %-30s\t\t%s" % (idx, name, vals)

        for inner_group in group.groups:
            result += _recurse_get_variable_atts(inner_group, group_name + '.')

        return result

    return _recurse_get_variable_atts(self.root_group)