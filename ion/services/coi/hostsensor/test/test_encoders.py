"""
@file ion/services/coi/hostsensor/test/test_encoders.py
@author Dorian Raymer
"""

from twisted.trial import unittest

from ion.services.coi.hostsensor import encoders

class TestEncoders(unittest.TestCase):

    def test_encode_iterables(self):
        test_object = {'list':[1,2,3], 'tuple':(1,2,3,)}

        encoded = encoders.encodeJSONToXMLRPC(test_object)
        decoded = encoders.decodeXMLRPCToJSON(encoded)

    def test_encode_big_int(self):
        encoders.decodeXMLRPCToJSON(encoders.encodeJSONToXMLRPC(2**31))


