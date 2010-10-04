

from ion.play.datastore import idatastore
from ion.play.datastore import datatype
from ion.play.datastore.encoders import base
from ion.play.datastore.encoders import ooi_types_pb2 as pb



class Encoder(object):
    """
    For basic types. Not data object.

    implements(IEncoder)
    """

    def __init__(self):
        self._encoders = {
                #datatype.Byte:pb.Byte,
                datatype.Short:pb.Short,
                datatype.Int:pb.Int,
                datatype.Long:pb.Long,
                datatype.Float:pb.Float,
                datatype.Double:pb.Double,
                datatype.String:pb.String,
                datatype.Boolean:pb.Boolean,
                datatype.Bytes:pb.Bytes,
                }
        self._decoders = {
                #'byte':pb.Byte,
                'short':pb.Short,
                'int':pb.Int,
                'long':pb.Long,
                'float':pb.Float,
                'double':pb.Double,
                'string':pb.String,
                'boolean':pb.Boolean,
                'bytes':pb.Bytes,
                }

    def encode(self, o):
        """
        first check if o is a data object instance
        could check for data object provider to make it easier
        then could also use primitive type provider
        """
        if isinstance(o, datatype.DataObject):
            return self.encode_dataobject(o)
        else:
            return self.encode_type(o)

    def decode(self, data):
        """
        assume the data has a TypedData header
        """
        o = pb.TypedValue.FromString(data)
        if o.type == u'DataObject':
            return self.decode_dataobject(o.value)
        else:
            return self.decode_type(data) # an unsymmetry

    def encode_dataobject(self, o):
        """
        """
        dataobj = pb.DataObject()
        for k, v in o.iteritems():
            att = dataobj.atts.add()
            att.key = k
            encoded = self.encode(v)
            att.value = encoded
        header = pb.TypedValue()
        header.type = 'DataObject'
        header.value = dataobj.SerializeToString()
        return header.SerializeToString()

    def encode_type(self, o):
        header = pb.TypedValue()
        encoder = self._encoders[type(o)]() #need error case
        encoder.value = o._value
        header.type = type(o).__name__.lower() #cleaner way 
        header.value = encoder.SerializeToString()
        return header.SerializeToString()

    def decode_dataobject(self, data):
        """
        """
        dataobj_buf = pb.DataObject().FromString(data)
        temp_dict = {}
        for att in dataobj_buf.atts:
            temp_dict[att.key] = self.decode(att.value)
        o = datatype.DataObject(temp_dict)
        return o

    def decode_type(self, data):
        header = pb.TypedValue().FromString(data)
        decoder = self._decoders[header.type]() # handle error
        o = decoder.FromString(header.value)
        return o

class Serializer(object):
    """
    For composite types (Data Objects).

    Serializes both Composites of basic types (which maps to one final
    message)
    and
    Composites of Composites (which maps to links and multiple messages)

    In more detail, Composites of basics could serialize via packing basics
    into one continuous byte string, or it could serialize by generating
    multiple messages.

    In the case of multiple messages, the Serializer is an encoder with a
    context/state (has a reason to be an instance), and that context is a
    data store client.

    implements(IEncoder)

    Mix in cas stuff here, for exploration.

    Classical file mechanics (open/close, read/write) 
      Vs.
    Quantum file mechanics (or quantized) (clone, checkout, commit)

    Might be a useful analogy, but really the Quantum file mechanics map
    onto Classical ones, but with a few strict constraints. 

    Note: keep in mind when considering the mutable node, permission to
    change that data isn't really considered from this perspective...don't
    assume everything is always writable by everyone. What types of files
    will actually have undetermined/arbitrary contention for writes/commits?
    Probably system related things. Think about how resource descriptions,
    registries, repositories, etc. might best be organized to minimize
    contention potential. 

    In the linux file system, flags are used to indicate how a file is open
    (read, write, read/write, append, create new if file does not exist,
    etc.)

    The file system associates ownership and permissions data with a file,
    based on the context of the process creating the file.

    entry point
    -----------
    * clone / get
      at minimum, get latest commit, and maybe all content under that
      this sort of combines open and read..unless you assume that when a
      classical file is opened, no other process can change it until you
      close it.
      There really isn't a close, but there is the synchronization step
      after a commit (push)

      open/clone/get gets you the object node
      subsequent gets/puts get you content nodes
      close/flush/push/xxx writes the object node back (this needs to be atomic, and
      optionally be able to be test and set)

    operational
    -----------
    * checkout (read mutable node and then read specific immutable state)
    * push (commit?)
    * get/put (for content objects, this is similar to classical read/write)

    specific to data object type
    ----------------------------
    * add (this implies changing structure, so isn't used if structures are
      fixed)
    * commit (puts data in a state suitable for push (write,
      synchronization))
    Commit, add, etc. operations imply serialization, as they mark a
    transition (or snapshot of the data object) between the in-memory
    structure and the persisted/encoded format. Really, the ObjectNode is
    like the work area/chassis; it is a decorator/container that adds
    behavior or operates on the data. These operations involve sending
    messages, which always requires encoding. 
    Whenever a [sendable] object is passed to the send method, the
    internals of send try encoding the object...or..there are constraints
    on what can be passed to the send method such that it can handle that
    data in a general enough way that the next lower-level send can be
    called with out special cases.

    
    If send is the place where an encoder is used to try encoding what was
    sent (therefore, if what was sent falls into the constraints of
    acceptable types), the result of encode will always be a byte string.
    So, the layer above send created by the ObjectNode abstraction (and
    CAS) will create their own sendables that can be encoded (i.e. instead
    of send encoding a pure data object, it encodes a CAS node, or a data
    object node, or both...)

    """






