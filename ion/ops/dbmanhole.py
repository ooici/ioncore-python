#!/usr/bin/env python
"""
@file ion/ops/dbmanhole.py
@author David Stuebe

"""

import os
import time
import sys

def main():

    if len(sys.argv) is not 2:
        print 'dbmanhole argument error: You must provide a single os process id number as an argument - got %d args!' % (len(sys.argv) -1)
        exit(1)

    try:
        int(sys.argv[1])
    except ValueError:
        print 'dbmanhole argument error: You must provide a single os process id number as an argument - got invalid type!'
        exit(1)

    fname = '.ccdebugport-%s' % sys.argv[1]
    if not os.path.exists(fname):
        print 'dbmanhole error: ION CC manhole file: "%s" can not be found!'
        exit(1)

    port = None
    try:

        f = open(fname, 'r')
        port = f.readline()
    except IOError:
        print 'dbmanhole error: Could not read port number from manhole file: "%s"' % fname
    finally:
        try:
            f.close()
        except NameError:
            pass

    try:
        int(port)
    except ValueError:
        print 'dbmanhole error: Got invalid type for port number in file: "%s"; line value - "%s"' % (fname, port)
        exit(1)

    print 'Connecting to ION CC on port %s ... Good luck brave soldier!' % port
    time.sleep(1)
    os.execvpe('telnet', ['telnet', 'localhost', port], os.environ)

if __name__ == '__main__':
    main()
