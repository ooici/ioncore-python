#!/usr/bin/env python
"""
@brief Interface definitions for the core object.
"""

from zope.interface import Interface

class IObject(Interface):
    """
    The IObject defines an append only container for content nodes. A content
    node is a serializable object built from a particular technology or toolchain.
    Getters and Setters for particular properties are redirected to the current
    content object. These methods are not called out here - this is about the
    functionality that OOI builds on top of the serialization technology.
    
    
    The methods for working with content are based on the Git model but it does
    impliment the commit or other high level comands. It provides a basis on
    which to build these based on a common structure for all objects. The
    storeable object will also meet the IObject interface, but this is still a
    generic Object - the basis for message content, state objects and resources.
    """
    
    _workspace = zope.interface.Attribute(
        """A hashmap (dictionary) which contains objects which are not yet indexed"""
        )
    _index = zope.interface.Attribute(
        """A hashmap (dictionary) which contains objects which are indexed by sha1 hash"""
        )
    
    _content = zope.interface.Attribute(
        """The current content object"""
        )
    
    _stash = zope.interface.Attribute(
        """A hashmap (dictionary) which contains the names of various content
        object states which can be applied from the workspace or index"""
        )
    

    def index():
        """
        Take the current content in the workspace and hash its DAG of nodes and
        links. Move the hashed content addressible objects to the index.
        """

    def stash(name):
        """
        Stash the current content object as name and return to the indexed state
        """

    def stash_apply(name):
        """
        Give the name of a stash set the current content object
        """

    def diff(a,b=None):
        """
        Return a hashmap containing the objects which are different between states
        a and b from the stash.
        """
