"""
@file ion/core/test/test_version.py
@note Adapted from twisted.python.test.test_versions
"""

from twisted.trial import unittest

from ion.core._version import Version, _inf
from ion.core._version import IncomparableVersions

class VersionsTest(unittest.TestCase):

    def test_versionComparison(self):
        """
        Versions can be compared for equality and order.
        """
        va = Version("dummy", 1, 0, 0)
        vb = Version("dummy", 0, 1, 0)
        self.failUnless(va > vb)
        self.failUnless(vb < va)
        self.failUnless(va >= vb)
        self.failUnless(vb <= va)
        self.failUnless(va != vb)
        self.failUnless(vb == Version("dummy", 0, 1, 0))
        self.failUnless(vb == vb)

        # BREAK IT DOWN@!!
        self.failIf(va < vb)
        self.failIf(vb > va)
        self.failIf(va <= vb)
        self.failIf(vb >= va)
        self.failIf(va == vb)
        self.failIf(vb != Version("dummy", 0, 1, 0))
        self.failIf(vb != vb)


    def test_comparingPrereleasesWithReleases(self):
        """
        Prereleases are always less than versions without prereleases.
        """
        va = Version("whatever", 1, 0, 0, prerelease=1)
        vb = Version("whatever", 1, 0, 0)
        self.assertTrue(va < vb)
        self.assertFalse(va > vb)
        self.assertNotEquals(vb, va)


    def test_comparingPrereleases(self):
        """
        The value specified as the prerelease is used in version comparisons.
        """
        va = Version("whatever", 1, 0, 0, prerelease=1)
        vb = Version("whatever", 1, 0, 0, prerelease=2)
        self.assertTrue(va < vb)
        self.assertFalse(va > vb)
        self.assertNotEqual(va, vb)


    def test_infComparison(self):
        """
        L{_inf} is equal to L{_inf}.

        This is a regression test.
        """
        self.assertEquals(_inf, _inf)


    def testDontAllowBuggyComparisons(self):
        self.assertRaises(IncomparableVersions,
                          cmp,
                          Version("dummy", 1, 0, 0),
                          Version("dumym", 1, 0, 0))


    def test_repr(self):
        """
        Calling C{repr} on a version returns a human-readable string
        representation of the version.
        """
        self.assertEquals(repr(Version("dummy", 1, 2, 3)),
                          "Version('dummy', 1, 2, 3)")


    def test_reprWithPrerelease(self):
        """
        Calling C{repr} on a version with a prerelease returns a human-readable
        string representation of the version including the prerelease.
        """
        self.assertEquals(repr(Version("dummy", 1, 2, 3, prerelease=4)),
                          "Version('dummy', 1, 2, 3, prerelease=4)")


    def test_str(self):
        """
        Calling C{str} on a version returns a human-readable string
        representation of the version.
        """
        self.assertEquals(str(Version("dummy", 1, 2, 3)),
                          "[dummy, version 1.2.3]")


    def test_strWithPrerelease(self):
        """
        Calling C{str} on a version with a prerelease includes the prerelease.
        """
        self.assertEquals(str(Version("dummy", 1, 0, 0, prerelease=1)),
                          "[dummy, version 1.0.0pre1]")


    def testShort(self):
        self.assertEquals(Version('dummy', 1, 2, 3).short(), '1.2.3')


    def test_base(self):
        """
        The L{base} method returns a very simple representation of the version.
        """
        self.assertEquals(Version("foo", 1, 0, 0).base(), "1.0.0")


    def test_baseWithPrerelease(self):
        """
        The base version includes 'preX' for versions with prereleases.
        """
        self.assertEquals(Version("foo", 1, 0, 0, prerelease=8).base(),
                          "1.0.0pre8")

    def test_formatGitCommit(self):
        """If we are in a git repository, there will be a git commit id
        Else, 'Unknown' will be appended to the version.
        For now, just ensure the funciton doesn't blow up.
        """
        v = Version("ion", 1, 0, 0)
        v._formatGitCommit()
    
