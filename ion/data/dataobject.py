#!/usr/bin/env python
"""
@file ion/data/dataobject.py
@author Dorian Raymer
@author Michael Meisinger
@author David Stuebe
@brief module for ION structured data object definitions
"""

NULL_CHR = '\x00'

import simplejson as json
import uuid
import re

from twisted.python import reflect

class TypedAttribute(object):
    """
    @brief Descriptor class for Data Object Attributes. Data Objects are
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
            raise TypeError("Error setting typed attribute %s \n Attribute must be of class %s \n Recieved Value of Class: %s" % (self.name, self.type, value.__class__))
        setattr(inst, self.name, value)


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
    @brief [Abstract] Base class for all data objects.
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
        assert isinstance(other, DataObject)
        # comparison of data objects which have different atts must not error out
        atts1 = set(self.attributes)
        atts2 = set(other.attributes)
        atts = atts1.union(atts2)
        try:
            m = [getattr(self, a) == getattr(other, a) for a in atts]
            return reduce(lambda a, b: a and b, m)
        except:
            return False

    
    def __ge__(self,other):
        """
        Compare data object which inherit from eachother - common attributes must be equal!
        See test case for intended applications
        
        """
        assert isinstance(other, DataObject)
        # comparison of data objects which have different atts must not error out
        atts = set(other.attributes)
        #print 'SELF',self.get_attributes()
        #print 'OTHER',other.get_attributes()
        try:
            m = [getattr(self, a) == getattr(other, a) for a in atts]
            return reduce(lambda a, b: a and b, m)
        except:
            return False
        
    def compared_to(self,other,regex=False,ignore_defaults=False,attnames=None):
        """
        Compares only attributes of self by default
        """
        assert isinstance(other, DataObject)

        atts=None
        if not attnames:
            atts = self.attributes
        else:
            atts=attnames
            
        if ignore_defaults:
            atts = self.non_default_atts(atts)
                
        #print 'ATTTTS',atts
        
        #print 'REGEX',regex
        #print 'ignore_defaults',ignore_defaults
        #print 'other',other.get_typedattributes()['name'].default
        #print 'self',self.get_typedattributes()['name'].default
                        
        if not atts:
            # A degenerate case
            return True
            
        if not regex:
            try:
                m=[]
                for a in atts:
                    if isinstance(getattr(self, a),DataObject):
                        m.append(getattr(self, a).compared_to(
                                                getattr(other, a),
                                                ignore_defaults=ignore_defaults))
                    else:                        
                        m.append(getattr(self, a) == getattr(other, a))
                    
                return reduce(lambda a, b: a and b, m)
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
                    
                return reduce(lambda a, b: a and b, m)
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
        @Brief Get the typed attributes of the class
        @Note What about typed attributes that are over ridden?
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
        names = []
        for key in self.__dict__:
            names.append(key[1:])
        return names

    def get_attributes(self):
        atts={}
        #@Note Not sure why dict does not work any more - returns default not value?
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
        d={}
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
    @Brief Method to create global unique identity for any new resource 
    """
    return str(uuid.uuid4())

class ResourceReference(DataObject):
    """
    @Brief The ResourceReference class is the base class for all resources.
    It contains the context of the resource from the repository where it is stored.
    """
    
    RegistryIdentity = TypedAttribute(str,None)
    #@TODO Make the commit ref a list so that an object can be a merge
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
    def create_new_resource(cls):
        """
        @Brief Use this method to instantiate any new resource with a unique id!
        """
        inst = cls()
        inst.RegistryIdentity = create_unique_identity()
        inst.RegistryBranch = 'master'
        return inst
    
    def create_new_reference(self):
        """
        @Brief Create or overwrite the reference identity for this resource
        """
        self.RegistryIdentity = create_unique_identity()
        self.RegistryBranch = 'master'
        self.RegistryCommit = ''
        return self
    
    def reference(self,head=False):
        """
        @Brief Use this method to make a reference to any resource
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
    @Brief Class to control the possible states based on the LCStateNames list
    """

    def __init__(self, state='new'):
        assert state in LCStateNames
        self._state = state

    def __repr__(self):
        return self._state

    def __eq__(self, other):
        assert isinstance(other, LCState)
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
    lifecycle = TypedAttribute(LCState, default=LCStates.new)

    def set_lifecyclestate(self, state):
        self.lifecycle = state

    def get_lifecyclestate(self):
        return self.lifecycle

    
DataObject._types['StatefulResource']=StatefulResource



