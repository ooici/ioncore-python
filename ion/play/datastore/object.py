"""
Tentative name.
(discrepancy between "Data Object" and "Content Object")

Here we explore the representation of a first-class entity in the data
store model, in light of the unified node idea.

The object chassis (or whatever the new thing will be) wraps behavior and
context around a pure data object to make a data node.
When the structure is manipulated (via the get/set accessors) while in a
chassis, the add/commit etc operations result in the serialization of the
pure data. The attributes of the structure are encoded by type through the
IEncoder interface (a particular implementation)
What can the attributes be. What are the leaf nodes (what determines if a
composite type is encoded as one big byte blob, or broken into constituent
byte blobs with a branch node encoding that composition/structure as a
table or map of pointers to the leaf nodes)?

"""



class DataObjectNode:
    """
    The fist-class entity of the data store.
    A type of node in the ooi common object model.
    A mutable structure with an identity (part of an identity namespace).
    One element of the structure is a "link" or "pointer", the value of
    which is a sha1 id of an immutable CAS node.

    Provide IDataObject interface
    """
    id = None
    pointer = None
    

class ContentNode:
    """

    Provide IDataObject interface
    Contains ICAStore providing object for storing content objects

    Uses serializer/encoder 
    """


class Chassis:
    """
    Behavior decorator for DataObjectNode
    """

    def __init__(self, dataobjectnode):
        """
        the dataobjectnode implements a certain interface, this Class
        provides that interface and forwards the method calls to
        dataobjectnode

        this encodes the 
        """

