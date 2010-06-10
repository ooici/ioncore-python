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

class Association(cas.Tree):
    
    type='association'
    
    entityFactory = cas.Entity
    
    spo = { # Subject, Predicate, Object
        'subject':0,
        'predicate':1,
        'object':2
    }
    
    
    def __init__(self, subject, predicate, object):
        
        triple = (subject, predicate, object)
        entities = []
        names = {}
        
        for item in self.spo:
            member = triple[self.spo[item]]

            #if isinstance(member, self.entityFactory):
             #  pass
             
            if isinstance(member, cas.BaseObject):
                child = self.entityFactory(item, member)
            else:
                member = cas.Blob(member)
                child = self.entityFactory(item, member)
        
            entities.append(child)
            names[child.name] = child
        self.children = entities
        self._names = names