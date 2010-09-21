#!/usr/bin/env python

"""
@file ion/core/_version.py
@author Dorian Raymer
@author Michael Meisinger
@brief sets the version for ION classes
"""

import os
import sys

class Version(object):
    """look for git commit to include in the version number.

    An object that represents a three-part version number.
    @note Taken from twisted python (twisted.python.versions)
    """

    def __init__(self, package, major, minor, micro, prerelease=None):
        """
        @param package: Name of the package that this is a version of.
        @type package: C{str}
        @param major: The major version number.
        @type major: C{int}
        @param minor: The minor version number.
        @type minor: C{int}
        @param micro: The micro version number.
        @type micro: C{int}
        @param prerelease: The prerelease number.
        @type prerelease: C{int}
        """
        self.package = package
        self.major = major
        self.minor = minor
        self.micro = micro
        self.prerelease = prerelease

    def short(self):
        """
        Return a string in canonical short version format,
        <major>.<minor>.<micro>[+rSVNVer].
        """
        s = self.base()
        gitcommit = self._getGitCommit()
        if gitcommit:
            s += '+git:' + str(gitcommit)
        return s

    def base(self):
        """
        Like L{short}, but without the +git.
        """
        if self.prerelease is None:
            pre = ""
        else:
            pre = "pre%s" % (self.prerelease,)
        return '%d.%d.%d%s' % (self.major,
                               self.minor,
                               self.micro,
                               pre)

    def __repr__(self):
        gitcommit = self._formatGitCommit()
        if gitcommit:
            gitcommit = '  #' + gitcommit
        if self.prerelease is None:
            prerelease = ""
        else:
            prerelease = ", prerelease=%r" % (self.prerelease,)
        return '%s(%r, %d, %d, %d%s)%s' % (
            self.__class__.__name__,
            self.package,
            self.major,
            self.minor,
            self.micro,
            prerelease,
            gitcommit)

    def __str__(self):
        return '[%s, version %s]' % (
            self.package,
            self.short())

    def __cmp__(self, other):
        """
        Compare two versions, considering major versions, minor versions, micro
        versions, then prereleases.

        A version with a prerelease is always less than a version without a
        prerelease. If both versions have prereleases, they will be included in
        the comparison.

        @param other: Another version.
        @type other: L{Version}

        @return: NotImplemented when the other object is not a Version, or one
            of -1, 0, or 1.

        @raise IncomparableVersions: when the package names of the versions
            differ.
        """
        if not isinstance(other, self.__class__):
            return NotImplemented
        if self.package != other.package:
            raise IncomparableVersions("%r != %r"
                                       % (self.package, other.package))

        if self.prerelease is None:
            prerelease = _inf
        else:
            prerelease = self.prerelease

        if other.prerelease is None:
            otherpre = _inf
        else:
            otherpre = other.prerelease

        x = cmp((self.major,
                    self.minor,
                    self.micro,
                    prerelease),
                   (other.major,
                    other.minor,
                    other.micro,
                    otherpre))
        return x

    def _getGitCommit(self):
        mod = sys.modules.get(self.package)
        if mod:
            git = os.path.join(os.path.dirname(mod.__file__), '..', '.git')
            if not os.path.exists(git):
                # It's not an git working copy
                return None

            headFile = os.path.join(git, 'HEAD')
            if os.path.exists(headFile):
                head = file(headFile).read().strip()
                headRef = head.split(':')[1].strip()
                refFile = os.path.join(git, headRef)
                if os.path.exists(refFile):
                    commit = file(refFile).read().strip()
                    return commit
                else:
                    return 'Unknown'
            else:
                return 'Unknown'

    def _formatGitCommit(self):
        commit = self._getGitCommit()
        if commit is None:
            return ''
        return ' (Git:%s)' % commit

# VERSION !!! This is the main version !!!
version = Version('ion', 0, 3, 0)
