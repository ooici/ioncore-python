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

    """
    Need a better test!
    $ bin/trial ion.services.coi.hostsensor.test.test_encoders.TestEncoders.test_encode_big_int
    ion.services.coi.hostsensor.test.test_encoders
     TestEncoders
       test_encode_big_int ...                                             [ERROR]

    ===============================================================================
    [ERROR]
    Traceback (most recent call last):
     File "/Users/dstuebe/Documents/Dev/code/ioncore-python/ion/services/coi/hostsensor/test/test_encoders.py", line 19, in test_encode_big_int
       encoders.decodeXMLRPCToJSON(encoders.encodeJSONToXMLRPC(2**31))
     File "/Users/dstuebe/Documents/Dev/code/ioncore-python/ion/services/coi/hostsensor/encoders.py", line 128, in decodeXMLRPCToJSON
       return int(object[5:], 16)
    exceptions.ValueError: invalid literal for int() with base 16: '00000L'

    ion.services.coi.hostsensor.test.test_encoders.TestEncoders.test_encode_big_int
    -------------------------------------------------------------------------------
    Ran 1 tests in 0.006s
    def test_encode_big_int(self):
        encoders.decodeXMLRPCToJSON(encoders.encodeJSONToXMLRPC(2**31))
    """

