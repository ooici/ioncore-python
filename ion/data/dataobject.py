#!/usr/bin/env python

"""
@file ion/data/dataobject.py
@author Michael Meisinger
@brief module for ION structured data object definitions
"""

from uuid import uuid4

class DataObject(object):
    """
    Base class for all structured data objects in the ION system. Data
    objects can be persisted, transported, versioned, composed.
    """

    def __init__(self):
        self.identity = self.create_unique_id()
        
    def set_attr(self, name, value):
        setattr(self, name, value)
    
    def get_attr(self, name):
        return getattr(self, name)

    def encode(self):
        """
        Encodes content of data object into blob (string or binary). The actual
        encoding used is set via the Strategy pattern.
        """
        # Currently just use the dict of attributes. No need to encode
        # @todo: what with not serializable values in deep?
        return self.__dict__

    def decode(self, blob):
        """
        Decodes content of a blob (string or binary) into data objecy. The actual
        encoding used is set via the Strategy pattern.
        """
        # @todo Check that object is empty before decoding
        if type(blob) is dict:
            self.__dict__.update(blob)

    @classmethod
    def create_unique_id(cls):
        return str(uuid4())[:9]
