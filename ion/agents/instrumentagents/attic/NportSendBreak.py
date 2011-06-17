#!/usr/bin/python
"""
@file ion/agents/instrumentagents/NportSendBreak.py
@brief This module is a simple command line utility to send the commands to an
       Nport serial server that cause it to send a break to an attached serial
       device.  It is intened to be used by engineers as a manual step to
       waking up an ADCP that has gone to sleep, for testing and development.
@author Bill Bollenbacher
@date 11/01/10
"""

import time
import sys
from socket import *

serverHost = '137.110.112.119'
serverPort = 967                   

if __name__ == '__main__':
    if len(sys.argv) > 3:
        print "Usage: %s [host [port]]" % __file__
        sys.exit(1)
    if len(sys.argv) > 1:
        serverHost = sys.argv[1]
    if len(sys.argv) > 2:
        serverPort = int(sys.argv[2])
       
s = socket(AF_INET, SOCK_STREAM)    # create a TCP socket
s.settimeout(2)                     # set timeout to 2 seconds for reads

print "Sending break commands to %s:%s" % (serverHost, serverPort)
s.connect((serverHost, serverPort)) # connect to server on the port

try:
    """  old method, more complicated than single command below
    s.send('\x21\x00')                  # start sending break
    data = s.recv(1024)                 # receive up to 1K bytes
    print data
    time.sleep (1)
    s.send('\x22\x00')                  # stop sending break
    data = s.recv(1024)                 # receive up to 1K bytes
    print data
    """
    
    s.send('#\x02\x01\xf4')             # send break for 500 milli seconds (0x1f4 = 500)
    data = s.recv(1024)                 # receive up to 1K bytes
    print data                          # should receive '#OK'
    
except:
    print "Nport server doesn't seem to be responding, make sure a client is attached to the data port."