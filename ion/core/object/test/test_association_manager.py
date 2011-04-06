#!/usr/bin/env python
"""
@brief Test implementation of the workbench class

@file ion/core/object/test/test_workbench
@author David Stuebe
@test The object management WorkBench test class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer

import weakref
import gc


from net.ooici.play import addressbook_pb2

from ion.core.object import gpb_wrapper
from ion.core.object import repository
from ion.core.object import workbench
from ion.core.object import object_utils

# For testing the message based ops of the workbench
from ion.core.process.process import ProcessFactory, Process
from ion.test.iontest import IonTestCase



person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)
invalid_type = object_utils.create_type_identifier(object_id=-1, version=1)


class AssociationManagerTest(unittest.TestCase):