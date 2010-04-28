
from twisted.internet import defer
from ion.core import bootstrap


@defer.inlineCallbacks
def main():
    yield bootstrap.start()

main()
