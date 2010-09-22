"""
@author Dorian Raymer
@author Michael Meisinger
@brief ION Exchange definitions based on AMQP messaging.
"""

from twisted.internet import defer

from carrot import connection
from carrot import messaging

class ExchangeSpace(object):
    """
    give it a name and a connection
    """
    def __init__(self, name, connection):
        self.name = name
        self.connection = connection

class ExchangeName(object):
    """
    High-level messaging name.
    Encapsulates messaging (amqp) details

    Might also retain name config dict
    OR might just be the config
    """
