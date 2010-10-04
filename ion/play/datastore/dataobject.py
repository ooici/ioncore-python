"""
The link between davids idea that dataobject / types should include their
encoding implementation, (and therefore, there will be multiple different
implementations of data object and types) might reconcile with this idea
of strong decoupling if there is an asymmetry in the instantiation of data
objects. Decoded ones might look like davids version, and freshly created
objects look like mine.

The structure definition is tied to the data object instantiation
process...

One point about not coupling the serializer with the data object is that it
builds complexity -- the data store client is buried under a lot of
abstraction. 
"""

from zope.interface import implements

class DataObject(dict):
    """
    Pure Data Object.
    A map structure of attributes. Each attribute has a name, a type, and a data
    value.

    {name:(type, value),
    ...
    }

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


class DataObjectDescription(object):
    """
    Describes structure of a Data Object.

    Definitive. Defined one time, in one place.

    Uses dummy basic types as place holders. Produces instance of data
    object with non-dummy types (like proto buffers)

    This can be used to eliminate always sending name information, which is
    what protobuffers does anyway...hmm
    """
    
    def __init__(self, **kwargs):
        """
        name=type multiples
        """
        self.fields = kwargs


    def buildDataObject(self):
        """
        """


