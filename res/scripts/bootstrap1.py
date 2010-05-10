
from twisted.internet import defer
from ion.core import bootstrap1


@defer.inlineCallbacks
def main():
    yield bootstrap1.start()

main()
