#!/usr/bin/env python
"""
@brief The Google Protocol Buffers implementation of the IObject Interface.
Other versions can be implemented based on other tool chains. They must provide
the same behavior!
"""

from google.protobuf import message
from zope.interface import implements
#from ion.play.betaobject import IObject

class GPBContainer():
    """
    An object for keeping track of GPB contents in an OOI object
    """
    def __init__(self, gpbinstance, parent_ref=None):
        
        self.gpbinstance = gpbinstance
        self.parent_ref = parent_ref
        self.gpbclass = gpbinstance.__class__
    
    
class GPBObject():
    #implements(IObject)
    

    
    def __init__(self, GPBClass):
        
        self.obj_cntr=0
        """
        A counter object used by this class to identify content objects untill
        they are indexed
        """
        
        self._workspace = {}
        """
        A dictionary containing objects which are not yet indexed, linked by a
        counter refrence in the current workspace
        """
        
        self._index = {}
        """
        A dictionary containing the objects which are already indexed by content
        hash
        """
        
        self._gpbmessage = GPBClass()
        
        self._AddMessageProperties(GPBClass)
        
        
        
        
        #obj_id = self.get_id()        
        #self._workspace[obj_id] = GPBContainer(GPBClass())
        #self.stash('Root',obj_id)                
        #self.stash_apply('Root')
        
    def _AddMessageProperties(self, GPBClass):
        
        assert issubclass(GPBClass, message.Message)
        
        for field in GPBClass.DESCRIPTOR.fields_by_name.values():

            self._AddFieldProperty(field)
        
        
    def _AddFieldProperty(self,field):
        field_name = field.name
        def getter(self):
            field_value = getattr(self._gpbmessage, field_name)
            return field_value
        getter.__module__ = None
        getter.__doc__ = 'Getter for %s.' % field_name
        
        def setter(self, new_value):
            setattr(self._gpbmessage, field_name, new_value)
        setter.__module__ = None
        setter.__doc__ = 'Setter for %s.' % field_name
        
        doc = 'Magic attribute generated for wrapper of "%s" proto field.' % field_name
        setattr(self, field_name, property(getter, setter, doc=doc))
        
    def get_id(self):
        self.obj_cntr += 1
        return str(self.obj_cntr)
        
    def stash(self, name,obj_id):
        # What to do if the name already exists?
        self._stash[name] = obj_id
        
    def stash_apply(self, stash_name=''):
        obj_id = self._stash.get(stash_name,None)
        
        self.container = self._workspace.get(obj_id, self._index.get(obj_id))
        
        self.content = self.container.gpbinstance
        
        
        
        
    def link(self, gpblink, ):
        """
        Create a link object to the current content.
        """
        
    def set_link(self, link, node):
        """
        Set the link and add the node to the workspace
        """
        
    def get_link(self, link):
        """
        Get the linked node
        """
        
class simple_property(object):
    
    def __init__(self,name):
        self.name = name
        self.funny = 5
    
    @property
    def name(self):
        return self.__name
    
    @name.setter
    def name(self,value):
        if not isinstance(value,str):
            raise TypeError('Must be a string!')
        self.__name = value
        
class property_wrapper(object):
    
    def __init__(self, ss):
        self.ss = ss
        
    @property
    def name(self):
        return self.ss.name
    
    #@name.setter
    #def name(self,value):
    #    self.ss.name = value
        