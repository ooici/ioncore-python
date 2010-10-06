#!/usr/bin/env python
"""
@file ion/data/dataobject.py
@author Dorian Raymer
@author Michael Meisinger
@author David Stuebe
@author Matt Rodriguez
@brief module for ION structured data object definitions
"""

NULL_CHR = '\x00'

try:
    import json
except:
    import simplejson as json
import uuid
import re

import logging
logging = logging.getLogger(__name__)

from twisted.python import reflect

class TypedAttribute(object):
    """
    @brief Descriptor class for Data Object attributes. Data Objects are
    containers of typed attributes.
    """

    def __init__(self, type, default=None):
        self.name = None
        self.type = type
        self.default = default if default else type()
        self.cache = self.default

    def __get__(self, inst, cls):
        value = getattr(inst, self.name, self.default)
        #return self.cache
        return value

    def __set__(self, inst, value):
        if not isinstance(value, self.type):
            raise TypeError("Error setting typed attribute %s \n Attribute must be of class %s \n Received Value of Class: %s" % (self.name, self.type, value.__class__))
        setattr(inst, self.name, value)
        #self.cache = value


    @classmethod
    def decode(cls, value, _types={}):
        """
        @brief This class method decodes a typed attribute
        @param value is the string which is to be decoded
        @param _types is a dictionary of types which can be decoded
        """

        #print '=================================================='
        types = _types.copy()
        stype, default = value.split(NULL_CHR)
        #print '---------- ', stype, default

        #the use of str is temporary unti lcaarch msging is fixed
        mytype = eval(str(stype), types)

        # If a value is given for the typed attribute decode it.
        if default:
            #print 'type, default:',type, default
            #return cls(type, eval(str(default), types))
            if issubclass(mytype, DataObject):
                data_object = mytype.decode(json.loads(default),header=False)
                return cls(mytype, data_object)

            elif issubclass(mytype, (list, set, tuple)):
                list_enc = json.loads(default)

                objs=[]
                for item in list_enc:
                    itype, ival = item.split(NULL_CHR)
                    itype = eval(str(itype), types)

                    if issubclass(itype, DataObject):
                        objs.append(itype.decode(json.loads(ival),header=False) )
                    else:
                        objs.append(itype(str(ival)))

                return cls(mytype, mytype(objs))
            elif issubclass(mytype, dict):
                # since dicts are 'just' json encoded load and return!

                return cls(mytype, json.loads(default))

            elif issubclass(mytype, bool):
                return cls(mytype, eval(str(default)))

            else:
                return cls(mytype, mytype(str(default)))
        return cls(mytype)


class DataObjectType(type):
    """
    @brief Metaclass for all Data Objects.
    """

    def __new__(cls, name, bases, dict):
        """
        @brief this makes it so DataObjects can inherit TypedAttributes
        from their super class.
        """
        d = {}
        base_dicts = []

        for base in reversed(bases):
            ayb = reflect.allYourBase(base)
            base_dicts.extend(base.__dict__.items())
            for ay in reversed(ayb):
                base_dicts.extend(ay.__dict__.items())
        for key, value in base_dicts:
            if isinstance(value, TypedAttribute):
                value.name = '_' + key
                d[value.name] = value.default

        for key, value in dict.items():
            if isinstance(value, TypedAttribute):
                value.name = '_' + key
                d[value.name] = value.default

        dict['__dict__'] = d
        return type.__new__(cls, name, bases, dict)

class DataObject(object):
    """
    @brief [Abstract] Base class for all data objects. The design intent behind
    DataObjects is to do transparent persistence and transport. Therefore,
    they know how to encode and decode their internal state, and how to describe
    their structure definition.
    """
    __metaclass__ = DataObjectType

    _types = {}

    def __init__(self):
        for name, att in self.get_typedattributes().items():
            if att.type == list:
                setattr(self,name,[])
            if att.type == dict:
                setattr(self,name,{})
            if att.type == set:
                setattr(self,name,set([]))


    def __eq__(self, other):
        """
        Compare dataobjects of the same class. All attributes must be equal.
        """
        #assert isinstance(other, DataObject)
        if not isinstance(other, DataObject):
            return False

        # comparison of data objects which have different atts must not error out
        atts1 = set(self.attributes)
        atts2 = set(other.attributes)
        atts = atts1.union(atts2)
        try:
            m = [getattr(self, a) == getattr(other, a) for a in atts]
            return all(m)
        except:
            return False


    def __ge__(self,other):
        """
        Compare data object which inherit from eachother - common attributes must be equal!
        See test case for intended applications

        """
        #assert isinstance(other, DataObject)
        if not isinstance(other, DataObject):
            return False

        # comparison of data objects which have different atts must not error out
        atts = set(other.attributes)
        #print 'SELF',self.get_attributes()
        #print 'OTHER',other.get_attributes()
        try:
            m = [getattr(self, a) == getattr(other, a) for a in atts]
            return all(m)
        except:
            return False

    def compared_to(self,other,regex=False,ignore_defaults=False,attnames=None):
        """
        @brief Compares only attributes of self by default
        @param other The other object to compare to this object
        @param regex A regex to use in the comparison
        @param ignore_defaults 
        @param attnames 
        """
        
        logging.info("Called compared_to")
        logging.info("other.__class__ %s " % other.__class__)
        logging.info("regex %s" % regex)
        logging.info("ignore_defaults %s" % ignore_defaults)
        logging.info("attnames %s" % attnames)
        if not isinstance(other, DataObject):    
            return False

        atts=None
        if not attnames:
            atts = self.attributes
        else:
            atts=attnames

        if ignore_defaults:
            atts = self.non_default_atts(atts)


        if not atts:
            return True

        
        if not regex:
            logging.info("Evaluating branch")
            try:
                m=[]
                for a in atts:
                    if isinstance(getattr(self, a),DataObject):
                        m.append(getattr(self, a).compared_to(
                                                getattr(other, a),
                                                ignore_defaults=ignore_defaults))
                    else:
                        m.append(getattr(self, a) == getattr(other, a))

                return all(m)
            except:
                return False
        else:
            
            try:
                m=[]
                for a in atts:
                    if getattr(self, a) == getattr(other, a):
                        m.append(True)
                    elif isinstance(getattr(other, a),(str, unicode)):
                        m.append(re.findall(getattr(self, a),getattr(other, a)))
                    elif isinstance(getattr(other, a),DataObject):
                        m.append(getattr(self, a).compared_to(
                                                    getattr(other, a),
                                                    regex=regex,
                                                    ignore_defaults=ignore_defaults))
                    else:
                        m.append(False)

                return all(m)
            except:
                return False




    def non_default_atts(self,attnames):
        atts=[]
        # There is something wrong with the way the dataobject is decoded
        # unless you pull the class definition from _types, the defaults
        # are incorrect when using the results of a message?
        r_class = self._types[self.__class__.__name__]
        typedatts = r_class.get_typedattributes()

        if not attnames:
            attnames=self.attributes

        for a in attnames:
            #print a, getattr(self, a), typedatts[a].default
            if getattr(self, a) !=  typedatts[a].default:
                atts.append(a)
        return atts


    #    def __le__(self,other):
    #        """
    #        """

    def __str__(self, indent=''):
        head = '='*10
        strng  = """\n%s%s Resource Type: %s %s\n""" % (indent,head, str(self.__class__.__name__), head)
        strng += """%s= 'ATT NAME':'VALUE':<TYPE> \n""" % indent
        for name in self.attributes:
            value = getattr(self,name)
            if isinstance(value, (list, tuple, set)):
                strng += indent + head*3 + '\n'
                strng += """%s= '%s': %s: List of Values Follows!\n""" % (indent, name,type(value))
                for item in value:
                    if isinstance(item, DataObject):
                        strng += """%s= '%s':%s\n""" % (indent,type(item),item.__str__(indent + '>'))
                    else:
                        strng += """%s= '%s':%s\n""" % (indent,item,type(item))
                strng += indent + head + 'End Of List!' + head + '\n'
            elif isinstance(value, DataObject):
                strng += """%s= '%s':'%s':%s\n""" % (indent, name,value.__str__(indent + '>'),type(value))
            else:

                strng += """%s= '%s':'%s':%s\n""" % (indent, name,value,type(value))
        strng += indent + head*4
        return strng

    @classmethod
    def get_typedattributes(cls):
        """
        @brief Get the typed attributes of the class
        @note What about typed attributes that are over ridden?
        """
        d={}
        ayb = reflect.allYourBase(cls)
        for yb in reversed(ayb):
            if issubclass(yb, DataObject):
                d.update(yb.__dict__)

        d.update(cls.__dict__)

        atts = {}
        for key,value in d.items():
            if isinstance(value, TypedAttribute):
                atts[key]=value
        return  atts



    @property

    def attributes(self):
        """
        @bug It would be nice if the attributes function only returned the set of keys for attributes that were defined within the object, rather than all attributes (even the ones relating to the underlying registry)
        """
        names = []
        for key in self.__dict__:
            names.append(key[1:])
        return names

    def get_attributes(self):
        atts={}
        #@note Not sure why dict does not work any more - returns default not value?
        #for key,value in self.__dict__.items():
        #    atts[key[1:]]=value
        for key in self.attributes:
            atts[key] = getattr(self,key)
        return atts


    def encode(self,header=True):
        """
        """
        encoded = []
        if header:
            encoded.append(('Object_Type', "%s" % (type(self).__name__)))

        for name in self.attributes:
            value = getattr(self, name)

            # Attempt to handle nested Resources
            if isinstance(value, DataObject):
                value_enc = value.encode(header = False)
                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, json.dumps(value_enc),)))
            elif isinstance(value,(list,tuple,set)):
                # List can contain other data object or decodable types
                list_enc = []
                for val in value:
                    if isinstance(val, DataObject):
                        val_enc = val.encode(header = False)
                        list_enc.append("%s%s%s" % (type(val).__name__, NULL_CHR, json.dumps(val_enc),))
                    else:
                        list_enc.append("%s%s%s" % (type(val).__name__, NULL_CHR, str(val)))

                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, json.dumps(list_enc),)))
            elif isinstance(value,dict):
                # dict can only contain JSONable types!
                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, json.dumps(value),)))

            else:
                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, str(value),)))


        return encoded


    @classmethod
    def decode(cls, attrs,header=True):
        """
        decode store object[s]
        """
        #d = dict([(str(name), TypedAttribute.decode(value)) for name, value in attrs])
        
        clsobj = cls
        if isinstance(attrs, tuple):
            attrs = list(attrs)

        if header:
            header,clsname = attrs.pop(0)
            #print 'header',header
            #print 'clsname',clsname
            clsobj = eval(str(clsname), cls._types)

        obj = clsobj()


        for name, value in attrs:
            #print 'name',name
            #print 'value',value
            ta = TypedAttribute.decode(value, cls._types)
            #print 'ta',ta.default

            setattr(obj,name, ta.default)
            #print name, d[str(name)].default
        #print 'clsobj', clsobj



        #
        #return type(clsobj.__name__, (clsobj,), d)
        return obj




"""
Add some important proprieties for OOICI Resource Descriptions
"""

def create_unique_identity():
    """
    @brief Method to create global unique identity for any new resource
    """
    return str(uuid.uuid4())

class ResourceReference(DataObject):
    """
    @brief The ResourceReference class is the base class for all resources.
    It contains the context of the resource from the repository where it is stored.
    """

    RegistryIdentity = TypedAttribute(str,None)
    #@todo Make the commit ref a list so that an object can be a merge
    RegistryCommit = TypedAttribute(str,None)
    RegistryBranch = TypedAttribute(str,'master')

    def __init__(self,RegistryIdentity='',RegistryCommit='',RegistryBranch=''):
        DataObject.__init__(self)
        if RegistryIdentity:
            self.RegistryIdentity = RegistryIdentity
        if RegistryCommit:
            self.RegistryCommit = RegistryCommit
        if RegistryBranch:
            self.RegistryBranch = RegistryBranch

    @classmethod
    def create_new_resource(cls, id='', branch='master'):
        """
        @brief Use this method to instantiate any new resource with a unique id!
        """
        inst = cls()


        if id:
            inst.RegistryIdentity = id
        else:
            inst.RegistryIdentity = create_unique_identity()

        inst.RegistryBranch = branch
        return inst

    def create_new_reference(self, id='', branch='master', commit=''):
        """
        @brief Create or overwrite the reference identity for this resource
        """
        if id:
            self.RegistryIdentity = id
        else:
            self.RegistryIdentity = create_unique_identity()

        self.RegistryBranch = 'master'

        self.RegistryCommit = commit
        return self

    def reference(self,head=False):
        """
        @brief Use this method to make a reference to any resource
        """
        inst = ResourceReference()
        if self.RegistryIdentity:
            inst.RegistryIdentity = self.RegistryIdentity
        if self.RegistryCommit and not head:
            inst.RegistryCommit = self.RegistryCommit
        inst.RegistryBranch = self.RegistryBranch
        return inst

DataObject._types['ResourceReference']=ResourceReference

"""
Define properties of Life Cycle State for Resource Descriptions
"""
LCStateNames = ['new',
                'active',
                'inactive',
                'decomm',
                'retired',
                'developed',
                'commissioned',
                ]

class LCState(object):
    """
    @brief Class to control the possible states based on the LCStateNames list
    """

    def __init__(self, state='new'):
        assert state in LCStateNames
        self._state = state

    def __repr__(self):
        return self._state

    def __eq__(self, other):
        if ((other == None) or (other == False)
            or (not isinstance(other, LCState))):
            return (False)
        return str(self) == str(other)

LCStates = dict([('LCState', LCState)] + [(name, LCState(name)) for name in LCStateNames])

class states(dict):
    """
    Class used to set the the possible states
    """

    def __init__(self, d):
        dict.__init__(self, d)
        for k, v in d.items():
            setattr(self, k, v)

LCStates = states(LCStates)
# Classes that do not inherit from DataObject must be explicitly added to the data
# Object Dictionary to be decoded!
DataObject._types.update(LCStates)

class Resource(ResourceReference):
    """
    @brief Base for all OOI resource description objects
    @note could build in explicit link back to ResourceRegistryClient so
    user can make changes through this object.
    """
    name = TypedAttribute(str)
    lifecycle = TypedAttribute(LCState, default=LCStates.new)

    def set_lifecyclestate(self, state):
        self.lifecycle = state

    def get_lifecyclestate(self):
        return self.lifecycle


DataObject._types['Resource']=Resource


class InformationResource(Resource):
    """
    @brief Base for all OOI information resource objects
    """

DataObject._types['InformationResource']=InformationResource

class StatefulResource(Resource):
    """
    @brief Base for all OOI Stateful resource objects
    """

DataObject._types['StatefulResource']=StatefulResource

class TestResource(Resource):
    a = TypedAttribute(list)
    b = TypedAttribute(InformationResource)
    i = TypedAttribute(int)
    f = TypedAttribute(float)

class SimpleTest(DataObject):
    field = TypedAttribute(str)
    name = TypedAttribute(str)

class IEncoder(object):
    """
    @brief DataObject Encoder/Decoder interface definition.
    @note This is a loose place-holder. IEncoder encapsulates both encoding
    and decoding.
    """

    def encode(o):
        """
        @param o instance of subclass of DataObject (or object form allowed
        TypedAttribute)
        @retval data of serialized DataObject.
        """

    def decode(self, data):
        """
        @param data 'encoded' DataObject
        @retval DataObject instance.
        """


class AlphaEncoder(object):
    """
    This calls the encode/decode methods of the original prototype
    DataObject. It provides the 'Encoder' interface and is used by the
    uniform serialization mechanism.
    """

    def encode(self, o):
        """
        @param o instance of subclass of DataObject
        """
        assert issubclass(type(o), DataObject)
        return o.encode()

    def decode(self, data):
        """
        @param data 'encoded' DataObject
        """
        return Resource.decode(data)

class DEncoder(object):
    """Encode a DataObject into a JSON encodable dict structure.
    """

    class Resource(Resource):
        def __init__(self):
            """undo what DataObject.__init__ does
            """

    def __init__(self):
        self._type_encoders = {
                int:self.encode_python_type,
                float:self.encode_python_type,
                str:self.encode_python_type,
                bool:self.encode_python_type,
                list:self.encode_list,
                #dict:self.encode_dict,
                LCState:self.encode_lcstate,
                DataObject:self.encode_dataobject
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

    def encode_lcstate(self, o):
        """
        """
        return {'type':'LCState', 'value':str(o)}

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


class JSONDEncoder(object):

    def __init__(self):
        self._dencoder = DEncoder()

    def encode(self, o):
        odict = self._dencoder.encode(o)
        return json.dumps(odict)

    def decode(self, data):
        odict = json.loads(data)
        return self._dencoder.decode(odict)

class Serializer(object):
    """
    @brief Registry of DataObject Encoders and uniform interface for
    encoding/decoding a DataObject.
    @note Aug 3, 2010 - DataObjects still implement their own [partial]
    encoding. We are incrementally moving towards a full serialization of a
    DataObject that makes sense with both how data objects are stored and
    how they are messaged and without making language specific
    assumptions/implementation features.
    """

    def __init__(self):
        self._encoders = {}
        self._encoders_by_type = {}
        self._decoders = {}
        self._default_encode = None
        self._default_content_type = None
        self._default_content_encoding = None

    def __repr__(self):
        s = ["%s\t\t%s\n" % (k, v[0]) for k, v in self._encoders.items()]
        return ''.join(s).expandtabs()

    def register(self, name, encoder, decoder, content_type, content_encoding):
        """
        """
        self._encoders[name] = (content_type, content_encoding, encoder)
        self._encoders_by_type[content_type] = (content_type, content_encoding, encoder)
        self._decoders[content_type] = decoder

    def set_default(self, name):
        (self._default_content_type, self._default_content_encoding,
            self._default_encode) = self._encoders[name]

    def encode(self, o, content_type=None, serializer=None):
        """
        Serialize data object
        """
        if serializer:
            (content_type, content_encoding, encoder) = self._encoders[serializer]
        elif content_type:
            (content_type, content_encoding, encoder) = self._encoders_by_type[content_type]
        else:
            encoder = self._default_encode
            content_type = self._default_content_type
            content_encoding = self._default_content_encoding
        data = encoder(o)
        return content_type, content_encoding, data


    def decode(self, data, content_type, content_encoding=None):
        """
        @note assume encoding is always binary, for now.
        @todo See what we learn from java for content_encoding
        """
        try:
            decoder = self._decoders[content_type]
        except KeyError:
            return data
        return decoder(data)

serializer = Serializer()

def register_alpha():
    alpha = AlphaEncoder()
    serializer.register('alpha', alpha.encode, alpha.decode,
            content_type='application/ion-dataobject',
            content_encoding='binary')

def register_dencoder():
    de = DEncoder()
    serializer.register('dencoder', de.encode, de.decode,
            content_type='application/ion-dencoder',
            content_encoding='binary')

def register_jsond():
    jd = JSONDEncoder()
    serializer.register('jsond', jd.encode, jd.decode,
        content_type='application/ion-jsond',
        content_encoding='utf-8')

register_alpha()
register_jsond()
register_dencoder()
serializer.set_default('alpha')
