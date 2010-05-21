#!/usr/bin/env python

"""
@file ion/data/dataobject.py
@author Michael Meisinger
@author David Stuebe
@brief module for ION structured data object definitions
"""
from uuid import uuid4

class DataObject(object):
    """
    Base class for all structured data objects in the ION system. Data
    objects can be persisted, transported, versioned, composed.
    """

    def __init__(self):
        """
        @brief Constructor, safe to use no arguments
        @param None
        @retval DataoObject instance
        """
        self.identity = self.create_unique_id()
        
    def __hash__(self):
        '''
        @ Note This should not be a hashable object - it can not be used as a dict key
        http://docs.python.org/reference/datamodel.html?highlight=__cmp__#object.__hash__
        '''
        return None
        
    def __eq__(self,other):
        """
        @brief Object Comparison
        @param DataObject Instance
        @retval True or False
        """
        assert type(other) is DataObject 
        return self.encode() == other.encode()
    
    def __ne__(self,other):
        """
        @brief Object Comparison
        @param other DataObject Instance
        @retval Bool 
        """
        return not self.__eq__(other)
    
    def set_attr(self, name, value):
        """
        @brief Set and Attribute/Value
        @param name String or Unicode name of attribute
        @param value Value of the attribute. Can be dict, str, int, float, DataObject or list
        @retval None
        """
        assert isinstance(name, unicode) or isinstance(name, str)
        assert isinstance(value, unicode) or isinstance(value, str) or isinstance(value, dict) or isinstance(value, int) or isinstance(value, float) or isinstance(value, list) or isinstance(value, DataObject)
        
        name = unicode(name) # because Json returns unicode, always use unicode
        setattr(self, name, value)
    
    def get_attr(self, name):
        """
        @brief Get and Attribute/Value by name
        @param name String or Unicode name of attribute
        @retval value Value of the attribute. Can be dict, str, int, float or list
        """        
        name = unicode(name)
        return getattr(self, name)

    def encode(self):
        """
        @brief Encodes content of data object into blob (string or binary). The actual
        encoding used is set via the Strategy pattern. THe method should be invertible
        after transmission using JSON encoding
        @retval d dictionary, possible nested ready for messaging
        """
        # Currently just use the dict of attributes. No need to encode
        # @todo: what with not serializable values in deep?
        d = self.__dict__
        
        for item in d:
            assert type(d[item]) is not tuple #, 'Tuples can not be used in DataObject because the are not jsonable!'
            assert type(d[item]) is not set #, 'Sets can not be used in DataObject because the are not jsonable!'
                
            if type(d[item]) is DataObject:
                d[item]=d[item].encode()
        return d
        

    def decode(self, blob):
        """
        @ Brief Decodes content of a blob (string or binary) into a data object. The actual
        encoding used is set via the Strategy pattern.
        @param blob dict of data to decode from a message
        """
        # @todo Check that object is empty before decoding
        # What should be the result if it is not empty? Error?
        if type(blob) is dict:
            for item in blob:
                assert type(blob[item]) is not tuple
                assert type(blob[item]) is not set
                
                # @ TODO deal with lists! - for not assume they are blobs
                #if type(blob[item]) is list:
                #    for i in blob[item]:
                #        assert type(i) is not tuple
                #        assert type(i) is not set
                #        assert type(i) is not list
                #        assert type(i) is not dict
   
                if type(blob[item]) is dict:
                    self.set_attr(item,DataObject.from_encoding(blob[item]))
                    
                else:
                    self.set_attr(item,blob[item])
            
            #self.__dict__.update(blob)

    @classmethod
    def from_encoding(cls, blob):
        inst = cls()
        inst.decode(blob)
        return inst

    @classmethod
    def create_unique_id(cls):
        return str(uuid4())[:9]
