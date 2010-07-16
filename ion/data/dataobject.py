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
                
        types = _types.copy()
        stype, default = value.split(NULL_CHR)
        
        #the use of str is temporary unti lcaarch msging is fixed
        mytype = eval(str(stype), types)

        # If a value is given for the typed attribute decode it.
        if default:
            #print 'type, default:',type, default
            #return cls(type, eval(str(default), types))
            if issubclass(mytype, DataObject):
                data_object = mytype.decode(json.loads(default),header=False)()
                return cls(mytype, data_object)
                
            elif issubclass(mytype, (list, set, tuple)):
                list_enc = json.loads(default)
                    
                objs=[]
                for item in list_enc:
                    itype, ival = item.split(NULL_CHR)
                    itype = eval(str(itype), types)
                    
                    if issubclass(itype, DataObject):
                        objs.append(itype.decode(json.loads(ival),header=False)() )
                    else:
                        objs.append(itype(str(ival)))
                    
                return cls(mytype, mytype(objs))

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

        for base in bases:
            ayb = reflect.allYourBase(base)
            base_dicts.extend(base.__dict__.items())
            for ay in ayb:
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

    def __eq__(self, other):
        assert isinstance(other, DataObject)
        # comparison of data objects which have different atts must not error out
        try:
            m = [getattr(self, a) == getattr(other, a) for a in self.attributes]
            return reduce(lambda a, b: a and b, m)
        except:
            return False
            

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

    @property
    def attributes(self):
        names = []
        for key in self.__dict__:
            names.append(key[1:])
        return names

    def get_attributes(self):
        atts={}
        for key,value in self.__dict__.items():
            atts[key[1:]]=value
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
                list_enc = []
                for val in value:
                    if isinstance(val, DataObject):
                        val_enc = val.encode(header = False)
                        list_enc.append("%s%s%s" % (type(val).__name__, NULL_CHR, json.dumps(val_enc),))
                    else:
                        list_enc.append("%s%s%s" % (type(val).__name__, NULL_CHR, str(val)))
                
                encoded.append((name, "%s%s%s" % (type(value).__name__, NULL_CHR, json.dumps(list_enc),)))

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
            
        for name, value in attrs:
            #print 'name',name
            #print 'value',value
            d[str(name)] = TypedAttribute.decode(value, cls._types)       
        return type(clsobj.__name__, (clsobj,), d)

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
        if RegistryIdentity:
            self.RegistryIdentity = RegistryIdentity
        if RegistryCommit:
            self.RegistryCommit = RegistryCommit
        if RegistryBranch:
            self.RegistryBranch = RegistryBranch


    @classmethod
    def create_new_resource(cls):
        """
        @Brief Use this method to instantiate any new resource!
        """
        inst = cls()
        inst.RegistryIdentity = create_unique_identity()
        inst.RegistryBranch = 'master'
        return inst
    
    
    
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
    lifecycle = TypedAttribute(LCState, default=LCStates.new)
    resource_description = \
    'This is the base class for all resources.'

    def set_lifecyclestate(self, state):
        self.lifecycle = state

    def get_lifecyclestate(self):
        return self.lifecycle

DataObject._types['Resource']=Resource


class InformationResource(Resource):
    """
    """
    resource_description = \
    'This is the base class for all Information resources.'
    
DataObject._types['InformationResource']=InformationResource

class StatefulResource(Resource):
    """
    """
    resource_description = \
    'This is the base class for all Stateful resources.'
    
DataObject._types['StatefulResource']=StatefulResource



