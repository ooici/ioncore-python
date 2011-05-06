#/usr/bin/env python

"""
@file ion/services/dm/inventory/ncml_generator.py
@author Paul Hubbard
@date 4/29/11
@brief For each dataset in the inventory, create a corresponding NcML file and sync with remove server.

Example file:

Contents:
<?xml version="1.0" encoding="UTF-8"?>
<netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"
location="ooici:17957467-0650-49c6-b7f5-5321a1cf018e"/>

Filename: 17957467-0650-49c6-b7f5-5321a1cf018e.ncml

So the filename and 'location' are just the GUID. Seems doable.
"""

file_template = """
<?xml version="1.0" encoding="UTF-8"?>
<netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2" location="ooici:%s"/>
"""

from os import path, environ

from twisted.internet import reactor, defer
from twisted.internet.protocol import ProcessProtocol
import twisted

import ion.util.ionlog
from ion.core import ioninit

# Globals and config file variables
log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)
RSYNC_CMD = CONF['rsync']

class AsyncProcessWithCallbackProto(ProcessProtocol):
    """
    Our wrapper class to run rsync, fires callback/errback when done.
    """
    def __init__(self, completion_deferred):
        self.cbd = completion_deferred

    def connectionMade(self):
        log.debug('Rsync is running')

    def processExited(self, reason):
        # let the caller know we're done and how it went
        if isinstance(reason, twisted.internet.error.ProcessTerminated):
            log.error('rsync failed, %s' % reason.value)
            self.cbd.errback(reason)
        else:
            log.debug('Return value from rsync, %s' % reason.value)
            self.cbd.callback('Done')

    def outReceived(self, data):
        log.debug('rsync says: "%s"' % data)


def create_ncml(id_ref, filepath=""):
    """
    @brief for a given idref, generate an NcML file in the filepath directory
    @param filepath Output directory, defaults to current working directory
    @param id_ref idref object from which we pull GUID
    @retval File contents, as a string, or None if error
    """

    full_filename = path.join(filepath, id_ref + '.ncml')
    log.debug('Generating NcML file %s' % full_filename)
    try:
        fh = open(full_filename, 'w')
        fh.write(file_template % id_ref)
        fh.close()
    except IOError:
        log.exception('Error writing NcML file')
        return None

    return file_template % id_ref


def rsync_ncml(local_filepath, server_url):
    """
    @brief Method to perform a bidirectional sync with a remote server, probably via rsync, unison
    or similar. Should be called after generating all local ncml files.
    @param local_filepath Local directory for writing ncml file(s)
    @param server_url rsync URL of the server
    @retval Deferred that will callback when rsync exits, or errback if rsync fails
    """
    d = defer.Deferred()
    rpp = AsyncProcessWithCallbackProto(d)
    args = [RSYNC_CMD, '', '-r', '--include', '"*.ncml"',
            '-v', '--stats', '--delete', local_filepath + '/', server_url]
    log.debug('Command is "%s %s"'% (RSYNC_CMD, args))

    # Adding environ.data uses the parent environment, otherwise empty
    reactor.spawnProcess(rpp, RSYNC_CMD, args, env=environ.data)

    return d

def rsa_to_dot_ssh(rsa_key):
    """
    @brief Another nasty hack. Take an RSA key, save it as an ssh-formatted file into the .ssh
        directory for use by rsync.
    """
    ssh_dir = path.join(path.expanduser('~'), '.ssh')
    rsa_filename = path.join(ssh_dir, 'rsync_ncml.rsa')

    if not path.exists(ssh_dir):
        log.error('ssh directory "%s" not found, cannot continue' % ssh_dir)
        return None

    if path.exists(rsa_filename):
        log.warn('RSA keyfile found, skipping')
        return None

    try:
        
    

