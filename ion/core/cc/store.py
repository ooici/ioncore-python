import re

from twisted.internet import defer



class Store(object):
    """core store interface; in memory version
    """
    def __init__(self):
        self.kvs = {}

    def read(self, key):
        """
        """
        return defer.maybeDeferred(self.kvs.get, key, None)

    get = read

    def write(self, key, value):
        """
        """
        return defer.maybeDeferred(self.kvs.update, {key:value})

    put = write

    def query(self, regex):
        return defer.maybeDeferred(self._query, regex)

    def _query(self, regex):
        """
        """
        return [re.search(regex,m).group() for m in self.kvs.keys() if re.search(regex,m)]

    def delete(self, key):
        return defer.maybeDeferred(self.delete, key)

    def _delete(self, key):
        """
        """
        del self.kvs[key]
        return 


class MessageSpaceRegistry(Store):
    """
    just broker + vhost for now
    """

class ContainerRegistry:
    """
    """


