
from ion.play.datastore.encoders import ion_basic_pb2 as basic

Float64 = basic.Float64
Float32 = basic.Float32
Int32 = basic.Int32
Int64 = basic.Int64
UInt32 = basic.UInt32
UInt64 = basic.UInt64
Byte = basic.Byte
Bytes = basic.Bytes
String = basic.String
Boolean = basic.Boolean


class CompositeType(object):

    def __init__(self, name, type):
        self.name = name
        self.type = type

class DCompositeType(object):
    """encode this in two levels:
    use a general composite container to store
    individually encoded fields
    """

    def __init__(self, **kwargs):
        """set proto buffer types as attrs
        """
        for k, v in kwargs.items():
            #setattr(self, k, v.value)
            setattr(self, k, v)

class DCompositeTypeFactory(object):

    def __init__(self, **kwargs):
        self.fields = kwargs

    def build(self):
        fields = {}
        for k, v in self.fields.items():
            fields[k] = v()
        return DCompositeType(**fields)

class DCompositeEncoder(object):
    """
    Encoding of a composite type 
    Detect that the instance is
     * an instance of CompositeType 
     * an instance of a subclass of CompositeType?

    Result of leaf encoding 
     * encoded into general container for named atts
     * encoded into specific proto buf for that Composite

    Grab the attrs by getting the keys of the CompositeType dict.
    When encoding a ion_basic_pb2 type, call the SerializeToString.
    A Composite type could be implemented as a ion_basic_pb2

    """

    def encode_composite(self, o):
        """
        """


class NodeDecorator(object):
    """
    When encoding into the Node model, Composites decompose into links and
    more nodes.
    """

    def __init__(self, obj):
        """
        """
        self.obj = obj
    
    def snapshot(self):
        """
        encode current state of composite and its constituents
        """
        objectNode = ObjectNode

class MainStore(object):

    def open(self, id):
        node = self.node_store.get(id,
                    ObjectNode.new(id))
        return node


def test():
    """
    make constituent element

    make composite container

    add constituents to container

    composite is something that can be a node (have an identity)

    open/create Node by id

    write/update Node data with composite object

    encode composite into Node
    """
    Person = DCompositeTypeFactory(name=String, id=Int32, email=String)
    
    PhoneNumber = DCompositeTypeFactory(number=String)

    AddressBook = DCompositeTypeFactory(people=Person)

    person = Person.build()

    person.name = u'door'
    person.id = 4444
    person.email = u'door@man.job'

    






