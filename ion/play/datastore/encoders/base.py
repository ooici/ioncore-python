"""
@brief Base implementation of IEncoder
"""


from zope.interface import implements

from ion.play.datastore import idatastore
from ion.play.datastore import types

class Encoder(object):

    implements(idatastore.IEncoder)

    def __init__(self):
        self._encoders = {} 

    def encode(self, obj):
        """
        """

    def decode(self, data):
        """
        """


