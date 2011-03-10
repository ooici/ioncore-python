"""
Container Crash Demonstration

This script will perform an operation on the messaging client that results
in a fatal error (the client connection closes).
The container catches the error, stops the reactor, and the twistd process
then exits.
Unfortunately, there is no way to have the twistd process exit with a code
other than 0. This is a known Twisted issue.

Usage:
    twistd -n cc crash_test_scri.py
"""
from ion.core.ioninit import container_instance as cc #Barf...I'm using the thing I despise
# It's like getting caught watching Jersey Shore

m = cc.exchange_manager.message_space
ch = m.connection._connection.channel()
d = ch.channel_open() # Open a channel once..that's good

def crash_operation(result, ch):
    print 'Hold on to you butts!!!'
    ch.channel_open() # Open the same channel twice...Bye Bye!

d.addCallback(crash_operation, ch)
