"""
@author Dorian Raymer
@author Michael Meisinger
@brief IDs in the system
"""

class Id(object):
    # Class attribute with default container id
    default_container_id = "/"

    """
    Entity instance id
    @param local is a local identifier, such as an incrementing counter
    @param container the id of a container
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
        return self.local == other.local

    def __hash__(self):
        return str.__hash__(self.full)
