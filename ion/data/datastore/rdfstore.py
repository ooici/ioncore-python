#!/usr/bin/env python
"""
@file ion/data/datastore/rdfstore.py
@author David Stuebe
@author Dorian Raymer

@ Ideas!
Trees & Commits should include the the repo blob! 
"""

import logging

from twisted.internet import defer
from twisted.python import reflect

from ion.data.datastore import cas
from ion.data import dataobject 

sha1 = cas.sha1

SUBJECT='subject'
OBJECT='object'
PREDICATE='predicate'

class Association(cas.Tree):
    
    type='association'
    
    entityFactory = cas.Entity
    
    spo = { # Subject, Predicate, Object
        SUBJECT:0,
        PREDICATE:1,
        OBJECT:2
    }
    
    
    def __init__(self, subject, predicate, object):
        
        triple = (subject, predicate, object)
        entities = []
        names = {}
        
        for position in self.spo:
            item = triple[self.spo[position]]

            #if isinstance(item, self.entityFactory):
             #  pass
             
            if isinstance(item, cas.BaseObject):
                child = self.entityFactory(position, item)
            else:
                item = cas.Blob(item)
                child = self.entityFactory(position, item)
        
            entities.append(child)
            names[child.name] = child
        self.children = entities
        self._names = names
        
    def match(self, other, position=None):
        
        assert isinstance(other, cas.BaseObject)
        
        if not position:
            position = self.spo.keys()
        
        if not getattr(position, '__iter__', False):
            position = (position,)
            
        for pos in position:
            
            if self._names[pos][1] == sha1(other.value):
                return True
            
        return False
                