#!/usr/bin/env python

"""
@file ion/interact/test/test_lazy_eye.py
@author Paul Hubbard
@date 4/26/11
@test Test lazy eye process and client
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.interact.lazy_eye import LazyEyeClient
from ion.test.iontest import IonTestCase
from ion.core.process.process import ProcessDesc

from ion.core import bootstrap

class LETest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_basic(self):

        pd1 = {'name':'lazy_eye','module':'ion.interact.lazy_eye','class':'LazyEye'}

        proc1 = ProcessDesc(**pd1)

        sup1 = yield bootstrap.create_supervisor()

        p1id = yield self.test_sup.spawn_child(proc1)

        lec = LazyEyeClient(proc=sup1, target=p1id)
        yield lec.start('superfile.msc')
        msc_txt = yield lec.stop()
        log.debug('msc says: "%s"' % msc_txt)
        rc = yield lec.get_results()

        log.debug("image name: %s" % rc['imagename'])
        self.failUnlessEqual(rc['imagename'], 'superfile.png')
        self.failIf(rc['num_edges'] <= 0)

        