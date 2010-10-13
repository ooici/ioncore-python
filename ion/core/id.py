#!/usr/bin/env python

"""
@author Dorian Raymer
@author Michael Meisinger
@brief IDs in the system
"""

class Id(object):
    """
    @todo Remove static dependency on default container
    """

    # Class attribute with default container id
    # @note STATICALLY SET BY OUTSIDE CODE
    default_container_id = "/"

    """
    Create an entity instance id
    @param local is a local identifier, such as an incrementing counter
    @param container a qualifier, such as the id of a container
    """
    def __init__(self, local, container=None):
        self.local = str(local)
        if container is None:
            container = self.default_container_id
        self.container = str(container)
        self.full = self.container + '.' + self.local

    def __str__(self):
        return self.full

    def __repr__(self):
        return """Id(%s, container="%s")""" % (self.local, self.container)

    def __eq__(self, other):
        try:
            # Should equality be about the str content, or also be the instance?
            return self.full == str(other)
            #return self.full == other.full
        except AttributeError, ae:
            return False

    def __hash__(self):
        return str.__hash__(self.full)
