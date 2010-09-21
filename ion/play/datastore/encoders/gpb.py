

from ion.play.datastore import idatastore
from ion.play.datastore import datatype
from ion.play.datastore.encoders import base
from ion.play.datastore.encoders import ooi_types_pb2 as pb



class Encoder(object):

    def __init__(self):
        self._encoders = {
                #datatype.Byte:pb.Byte,
                datatype.Short:pb.Short,
                datatype.Int:pb.Int,
                datatype.Long:pb.Long,
                datatype.Float:pb.Float,
                datatype.Double:pb.Double,
                datatype.String:pb.String,
                }
        self._decoders = {
                #'byte':pb.Byte,
                'short':pb.Short,
                'int':pb.Int,
                'long':pb.Long,
                'float':pb.Float,
                'double':pb.Double,
                'string':pb.String,
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


