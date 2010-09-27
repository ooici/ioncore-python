#!/usr/bin/env python
"""
@brief The Google Protocol Buffers implementation of the IObject Interface.
Other versions can be implemented based on other tool chains. They must provide
the same behavior!
"""
from zope.interface import implements
from ion.play.betaobject import IObject

class GPBObject():
    implements(IObject)
    
    def __init__(GPBClass):
        
        _workspace = {}
        _index = {}
        _content = None
        _stash = {}
        
        _GPBClass = GPBClass
        
        
    