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


class BasicType(object):
    """
    Base class for the basic types. 

    The exact way the datatypes are used is still under consideration.

    Basic type containers could provide a map for allowed language
    types
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

    def get(self):
        """
        get value
        """

    def set(self, value):
        """
        set value
        """

class Short(BasicType):

    def __init__(self, value=int()):
        self._value = value

class Int(BasicType):

    def __init__(self, value=int()):
        self._value = value

class Long(BasicType):

    def __init__(self, value=long()):
        self._value = value

class Float(BasicType):

    def __init__(self, value=float()):
        self._value = value

class Double(BasicType):

    def __init__(self, value=float()):
        self._value = value

class Boolean(BasicType):

    def __init__(self, value=bool()):
        self._value = value

class String(BasicType):
    """
    Unicode character string.
    """
    
    def __init__(self, value=u''):
        self._value = value #has to be unicode

class Bytes(BasicType):
    """
    The common data model names this "Opaque"
    """

    def __init__(self, value=''):
        self._value = value

class Structure(dict):
    """
    """

class Sequence:
    """
    """







