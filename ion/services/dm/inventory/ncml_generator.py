#/usr/bin/env python

"""
@file ion/services/dm/inventory/ncml_generator.py
@author Paul Hubbard
@date 4/29/11
@brief For each dataset in the inventory, create a corresponding NcML file and
sync with remove server. Some tricky code for running a process and noting its
exit with a deferred.
"""

# File template. The filename and 'location' are just the GUID.
# Note the %s for string substitution.
file_template = """<?xml version="1.0" encoding="UTF-8"?>\n<netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2" location="ooici:%s"/>
"""

from os import path, environ, chmod, unlink, listdir, remove
import fnmatch

from twisted.internet import reactor, defer, error
from ion.util.os_process import OSProcess

import ion.util.ionlog
from ion.core import ioninit

# Globals and config file variables
log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)
RSYNC_CMD = CONF['rsync']
SSH_ADD_CMD = CONF['ssh-add']

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

def check_for_ncml_files(local_filepath):
    """
    Check for ncml files on disk.
    
    Returns True if any ncml files in the given directory, False if no files
        or an error is raised.
    """
    log.debug('checking for ncml files in %s' % local_filepath)

    try:
        allfiles = listdir(local_filepath)
        ncml_files = []
        for file in allfiles:
            if fnmatch.fnmatch(file, '*.ncml'):
                ncml_files.append(file)
    except IOError:
        log.exception('Error searching %s for ncml files' % local_filepath)
        return False

    if len(ncml_files) > 0:
        return True

    return False


def clear_ncml_files(local_filepath):
    """
    Clear ncml files on disk.
    """
    log.debug('clearing ncml files in %s' % local_filepath)

    try:
        allfiles = listdir(local_filepath)
        ncml_files = []
        for file in allfiles:
            if fnmatch.fnmatch(file, '*.ncml'):
                fpath = path.join(local_filepath, file)
                log.debug('Removing file: %s' % fpath)
                remove(fpath)
    except IOError:
        log.exception('Error searching %s for ncml files' % local_filepath)
        return False

    if len(ncml_files) > 0:
        return True

    return False



def rsync_ncml(local_filepath, server_url):
    """
    @brief Method to perform a bidirectional sync with a remote server,
    probably via rsync, unison or similar. Should be called after generating all
    local ncml files.
    @param local_filepath Local directory for writing ncml file(s)
    @param server_url rsync URL of the server
    @retval Deferred that will callback when rsync exits, or errback if rsync fails
    """
    args = ['-r', '-p', '--include', '"*.ncml"',
            '-v', '--stats', '--delete', local_filepath + '/', server_url]
    rp = OSProcess(binary=RSYNC_CMD, spawnargs=args, env=environ.data)
    log.debug('Command is "%s"'% ' '.join(args))

    return rp.spawn()
    

def rsa_to_dot_ssh(private_key, public_key, delete_old=True):
    """
    @brief Another hack. Take an RSA key, save it as an ssh-formatted file into
    the .ssh directory for use by rsync.
    @param private_key RSA private key, as returned from 'ssh-keygen -t rsa'
    @param public_key Public half of same, used for ssh-add -d
    @retval Tuple of filenames - private and public key
    @note Raises IOError if necessary
    """

    ssh_dir = path.join(path.expanduser('~'), '.ssh')
    rsa_filename = path.join(ssh_dir, 'rsync_ncml.rsa')
    pubkey_filename = path.join(ssh_dir, 'rsync_ncml.pub')

    if not path.exists(ssh_dir):
        log.error('ssh directory "%s" not found, cannot continue' % ssh_dir)
        return None

    if path.exists(rsa_filename):
        if not delete_old:
            log.warn('RSA keyfile found, skipping')
            return None

    try:
        # Write out public and private keys
        fh = open(rsa_filename, 'w')
        fh.write(private_key)
        fh.close()
        chmod(rsa_filename, 0600)

        fh = open(pubkey_filename, 'w')
        fh.write(public_key)
        fh.close()
        
        log.debug('Wrote keys OK')
    except IOError:
        log.exception('Error writing ssh keys')
        raise

    return rsa_filename, pubkey_filename

def ssh_add(filename, remove=False):
    """
    Reuse async protocol class to run ssh-add as a subprocess.
    Adds or removes a key by filename.

    @retval Returns a deferred that fires when the ssh-add completes.

    @bug Deleting the key fails -
    @see https://bugs.launchpad.net/ubuntu/+source/openssh/+bug/58162
    @note You need to have the public key present in the ssh directory for
    delete to work.
    """
    if remove:
        args = ['-d', filename]
    else:
        args = [filename]

    log.debug('Command is %s' % args)

    sap = OSProcess(binary=SSH_ADD_CMD, spawnargs=args, env=environ.data)
    return sap.spawn()


@defer.inlineCallbacks
def do_complete_rsync(local_ncml_path, server_url, private_key, public_key):
    """
    Orchestration routine to tie it all together plus cleanup at the end.
    Needs the inlineCallbacks to serialise.
    """

    if not private_key or not public_key:
        log.error('Missing required RSA key for NCML RSYNC in Dataset Controller!')
        defer.returnValue(None)
        
    # Generate a private key, add to ssh agent
    skey, pkey  = rsa_to_dot_ssh(private_key, public_key)
    yield ssh_add(skey)

    # Run rsync, which should use the key in the agent
    yield rsync_ncml(local_ncml_path, server_url)

    # Remove the key from the agent and then delete the keys for good measure.
    yield ssh_add(pkey, remove=True)

    # Delete the keys from the file system
    unlink(skey)
    unlink(pkey)



