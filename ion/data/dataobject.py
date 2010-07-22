#!/usr/bin/env python
"""
@file ion/data/dataobject.py
@author Dorian Raymer
@author Michael Meisinger
@author David Stuebe
@brief module for ION structured data object definitions
"""

NULL_CHR = '\x00'


class TypedAttribute(object):
    """
    @brief Descriptor class for Data Object Attributes. Objects are
    containers of typed attributes.
    """

    def __init__(self, type, default=None):
        self.name = None
        self.type = type
        self.default = default if default else type()
        self.cache = None

    def __get__(self, inst, cls):
        value = getattr(inst, self.name, self.default)
        return value

    def __set__(self, inst, value):
        if not isinstance(value, self.type):
            raise TypeError("Must be a %s" % self.type)
        setattr(inst, self.name, value)

    @classmethod
    def decode(cls, value, _types={}):
        types = _types.copy()
        stype, default = value.split(NULL_CHR)
        
        #the use of str is temporary unti lcaarch msging is fixed
        type = eval(str(stype), types)
#        return cls(type, type(str(default)))
        if default:
            print 'type, default:',type, default
            #return cls(type, eval(str(default), types))
            return cls(type, type(str(default)))
        return cls(type)


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
        for key, value in [b.__dict__.items() for b in bases][0]:
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
    @brief [Abstract] Base class for all data objects.
    """
    __metaclass__ = DataObjectType

    _types = {}

    def __eq__(self, other):
        assert isinstance(other, DataObject)
        # comparison of data objects which have different atts must not error out
        try:
            m = [getattr(self, a) == getattr(other, a) for a in self.attributes]
            return reduce(lambda a, b: a and b, m)
        except:
            return False
            

    def __str__(self):
        head = '='*10
        strng  = """\n%s Resource Type: %s %s\n""" % (head, str(self.__class__.__name__), head)
        for name in self.attributes:
            value = getattr(self,name)
            strng += """= '%s':'%s'\n""" % (name,value)
        strng += head*2
        return strng

    @property
    def attributes(self):
        names = []
        for key in self.__dict__:
            names.append(key[1:])
        return names

    def encode(self):
        """
        """
        encoded = []
        for name in self.attributes:
            value = getattr(self, name)
            
            # Attempt to handle nested Resources
            if not isinstance(value, DataObject):
                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, str(value),)))
            else:
                value = value.encode()
                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, str(value),)))
        return encoded

    @classmethod
    def decode(cls, attrs):
        """
        decode store object[s]
        """
        #d = dict([(str(name), TypedAttribute.decode(value)) for name, value in attrs])
        d={}
        for name, value in attrs:
            #print 'name',name
            #print 'value',value
            d[str(name)] = TypedAttribute.decode(value, cls._types)       
        return type(cls.__name__, (cls,), d)

class ArbitraryObject(DataObject):
    key = TypedAttribute(str, 'xxx')
    name = TypedAttribute(str, 'blank')

class Test(ArbitraryObject):
    name = TypedAttribute(str, 'door')
    age = TypedAttribute(int)




