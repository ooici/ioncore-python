"""
@brief Module defining the set of primitive platform independent types.

Each type in this set represents an invariant in the system, in terms of
what it represents during usage. The encoding of these types can be
implemented in different, technology specific ways.

An encoding library can be implemented as a module that provides
encoders/decoders for this set of types. 

The ooi data object can be defined in it's own module which depends on
these types.

The serialization module provides a general interface and skeleton
mechanism for serializing data objects.
Specific serialization types are implemented separately and plugged in to the
main serializer.
"""

from zope.interface import implements

class PrimitiveType(object):

    def __init__(self, value=None):
        """
        @pram value initializer
        """
        self._value = value

    def __repr__(self):
        return "%s" % str(self._value)

class Byte(PrimitiveType):
    """
    really need this?
    """

class Short(PrimitiveType):
    """
    really need this?
    """

class Int(PrimitiveType):
    """
    """

class Long(PrimitiveType):
    """
    """

class Float(PrimitiveType):
    """
    """

class Double(PrimitiveType):
    """
    """

class String(PrimitiveType):
    """
    """

class Structure:
    """
    """

class Sequence:
    """
    """

class Enum:
    """
    hmm, this one might be tricky, since you need to define a specific
    proto per
    """

class Bytes:
    """
    The common data model names this "Opaque"
    """


class DataObject(dict):
    """
    A map of attributes. Each attribute has a name, a type, and a data
    value.
    @note how is the structure defined in a static way?
    """


    def set(self, key, value):
        """
        """
        if self.has_key(key):
            self[key] = value





