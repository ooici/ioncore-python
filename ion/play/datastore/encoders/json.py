
try:
    import json
except:
    import simplejson as json

class DEncoder(object):
    """Encode a DataObject into a JSON encodable dict structure.
    """

    def __init__(self):
        self._type_encoders = {
                int:self.encode_python_type,
                float:self.encode_python_type,
                str:self.encode_python_type,
                bool:self.encode_python_type,
                list:self.encode_list,
                #dict:self.encode_dict,
                datatype.DataObject:self.encode_dataobject
                }

        self._type_decoders = {
                'int':self.decode_python_type,
                'float':self.decode_python_type,
                'str':self.decode_python_type,
                'bool':self.decode_python_type,
                'list':self.decode_list,
                #'dict':self.decode_dict,
                'LCState':self.decode_lcstate,
                'DataObject':self.decode_dataobject
                }


    def encode_python_type(self, o):
        """
        int, float, str, bool
        """
        return {'type':type(o).__name__, 'value':o}

    def encode_dataobject(self, o):
        """
        """
        d = {}
        d['type'] = 'DataObject'
        d['class'] = o.__class__.__name__
        d['fields'] = {}
        for field in o.attributes:
            d['fields'][field] = self.encode(getattr(o, field))
        return d

    def encode_list(self, o):
        """
        """
        data = {'type':'list'}
        data['value'] = {}
        for ord, obj in enumerate(o):
            data['value'][str(ord)] = self.encode(obj)
        return data

    def encode_dict(self, o):
        """
        """

    def decode_python_type(self, odict):
        """
        @note using eval to get type!
        """
        t = eval(odict['type'])
        v = t(odict['value'])
        return v

    def decode_dataobject(self, odict):
        """
        """
        cls = odict['class']
        fields = odict['fields']
        __dict = {}
        for name, vdict in fields.iteritems():
            name = str(name)
            val = self.decode(vdict)
            __dict[name] = TypedAttribute(type(val), val)
        #o = type(cls, (DataObject,), __dict)()
        #o = type(str(cls), (Resource,), __dict)()
        o = type(str(cls), (self.Resource,), __dict)()
        return o

    def decode_lcstate(self, odict):
        """
        """
        return LCStates[odict['value']]

    def decode_list(self, odict):
        """
        """
        o = []
        for i in range(len(odict['value'].keys())):
            vdict = odict['value'][str(i)]
            o.append(self.decode(vdict))
        return o

    def decode_dict(self, data):
        """
        """

    def encode(self, o):
        """
        """
        if issubclass(type(o), DataObject):
            return self.encode_dataobject(o)
        else:
            return self._type_encoders.get(type(o))(o)

    def decode(self, odict):
        """
        """
        return self._type_decoders[odict['type']](odict)


class Encoder(object):
    """
    There's not much gained by making this an instance...since we haven't
    thought about partial decoding/streaming.
    """

    def __init__(self):
        self._dencoder = DEncoder()

    def encode(self, o):
        odict = self._dencoder.encode(o)
        return json.dumps(odict)

    def decode(self, data):
        odict = json.loads(data)
        return self._dencoder.decode(odict)

