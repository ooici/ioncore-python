#!/usr/bin/env python
"""
@brief Test implementation of the codec class

@file ion/core/object/test/test_codec
@author David Stuebe
@test The object codec test class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer


from net.ooici.play import addressbook_pb2

from ion.core.object import codec
from ion.core.object import workbench
from ion.core.object import object_utils

PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)
ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
ADDRESSBOOK_TYPE = object_utils.create_type_identifier(object_id=20002, version=1)


class CodecTest(unittest.TestCase):

    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb

        repo = self.wb.create_repository(ADDRESSLINK_TYPE)

        ab = repo.root_object

        p = repo.create_object(PERSON_TYPE)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '123 456 7890'

        ab.owner = p

        ab.person.add()
        ab.person[0] = p

        ab.person.add()
        p = repo.create_object(PERSON_TYPE)
        p.name='John'
        p.id = 78
        p.email = 'J@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '111 222 3333'

        ab.person[1] = p

        self.ab = ab
        self.repo = repo


    def test_pack_eq_unpack(self):

        serialized = codec.pack_structure(self.ab)

        res = codec.unpack_structure(serialized)

        self.assertEqual(res,self.ab)


    def test_unpack_error(self):

        self.assertRaises(codec.CodecError,codec.unpack_structure,'junk that is not a serialized container!')
