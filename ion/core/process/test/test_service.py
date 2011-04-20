#!/usr/bin/env python

"""
@file ion/core/process/test/test_service.py
@author David Stuebe
@brief test case for process base class
"""

import ion.core.ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer

from ion.core.process.process import Process
from ion.core.process.service_process import ServiceProcess
from ion.core.process.test import test_process
from ion.core.cc.container import Container
from ion.core.exception import ReceivedError, ApplicationError
from ion.core.messaging.receiver import Receiver, WorkerReceiver
from ion.core.id import Id
from ion.test.iontest import IonTestCase, ReceiverProcess
import ion.util.procutils as pu


class EchoService(ServiceProcess, test_process.EchoProcess):

    pass