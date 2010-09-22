"""
@brief Module defining the set of primitive platform independent types and
structures.

Each type in this set represents an invariant in the system. 
The final list of primitive types is to be defined by the architecture. 

The encoding of these types can be implemented in different ways using
different technologies.

An encoding library can be implemented as a module that provides
encoders/decoders for this set of types. 
"""

from zope.interface import implements

class PrimitiveType(object):
    """
    Base class for the primitive types. 

    The exact way the datatypes are used is still under consideration.
    """

    def __init__(self, value=None):
        """
        @pram value initializer
        """
        self._value = value

    def __str__(self):
        return "%s" % str(self._value)

    def __repr__(self):
        return "%s" % str(self._value)

class Short(PrimitiveType):

    def __init__(self, value=int()):
        self._value = value

class Int(PrimitiveType):

    def __init__(self, value=int()):
        self._value = value

class Long(PrimitiveType):

    def __init__(self, value=long()):
        self._value = value

class Float(PrimitiveType):

    def __init__(self, value=float()):
        self._value = value

class Double(PrimitiveType):

    def __init__(self, value=float()):
        self._value = value

class Boolean(PrimitiveType):

    def __init__(self, value=bool()):
        self._value = value

class String(PrimitiveType):
    """
    Unicode character string.
    """
    
    def __init__(self, value=u''):
        self._value = value #has to be unicode

class Structure(dict):
    """
    """

class Sequence:
    """
    """

class Bytes:
    """
    The common data model names this "Opaque"
    """

    def __init__(self, value=''):
        self._value = value

class DataObject(dict):
    """
    Pure Data Object.
    A map structure of attributes. Each attribute has a name, a type, and a data
    value.

    This is not necessarily a tool for defining structures. 
    
    Uses of Data Object as a structure:
        * Data Object can be used as a generic structure, defined on the
          spot. This can be encoded in a self describing way such that
          knowledge of the primitive types and the Data Object type are
          sufficient for decoding.
        * Data Object could also be used to make new types (composite
          types). The new type would be a subclass of Data Object.
          In this case, Data Object represents a known type (defined by the
          system somehow). This class provides the Python representation of
          that type and enforces type checking on the component attributes.

    Implementation Notes
    With the exception of the 'set' method, this implementation of
    IDataObject is mostly provided by the python 'dict'.

    Review
    Is this really what the original intent of Data Object was?
    This may as well just be the same implementation as the Structure data
    type.
    """

    def set(self, key, value):
        """
        """
        if self.has_key(key):
            self[key] = value





