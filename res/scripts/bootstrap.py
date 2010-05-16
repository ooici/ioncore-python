# Python Capability Container start script.
# Starts root container and core services.

from twisted.internet import defer
from ion.core import bootstrap


@defer.inlineCallbacks
def main():
    yield bootstrap.start()

main()
