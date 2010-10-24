#!/usr/bin/env python
"""
@file ion/data/backends/cassandra.py
@author Bing Zhu
@brief Implementation of ion.data.store.IStore using pyirods to interface a
       iRODS datastore backend for a single iRODS zone
@Note Test cases for the iRODS backend are in ion.data.backends.test.test_irodsstore 
@
"""

#import re
import sys
import logging
logging = logging.getLogger(__name__)

#from twisted.internet import defer

from ion.core import ioninit
from ion.data.store import IStore

import uuid

from irods import *
from irods_error import *

CONF = ioninit.config(__name__)


class IrodsStore(IStore):
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    @Note Default behavior is to use a random super column name space!
    """
    def __init__(self, **kwargs):
        self.hostname = None
        self.port_num = None
        self.default_resc = None
        self.obj_home = None
        self.user_name = None
        self.user_passwd = None
        self.zone = None
        self.conn = None

    @classmethod
    def get_config(self, key):
        try:
            value = CONF[key]
        except:
            value = None
        return value

    @classmethod
    def create_store(cls, **kwargs):
        """
        @brief actory method to create an create an instance of the irods store 
        @param from kwargs or from config file.
        @param cass_host_list List of hostname:ports for cassandra host or cluster
        @retval Deferred, for IStore instance.
        """
        inst = cls(**kwargs)
        inst.kwargs = kwargs

        # get values from config file first
        inst.hostname = inst.get_config('irodsHost')
        inst.port_num = inst.get_config('irodsPort')
        inst.default_resc = inst.get_config('rodsDefResource')
        inst.obj_home = inst.get_config('irodsOOIHome')
        inst.user_name = inst.get_config('irodsUserName')
        inst.user_passwd = inst.get_config('irodsUserPasswd')
        inst.zone = inst.get_config('irodsZone')

        # can be overridden by args
        if kwargs:
            if kwargs.get('irodsHost', None):
                inst.hostname = kwargs.get('irodsHost', None)
            if kwargs.get('irodsPort', None):
                inst.port_num = kwargs.get('irodsPort', None)
            if kwargs.get('rodsDefResource', None):
                inst.default_resc = kwargs.get('rodsDefResource', None)
            if kwargs.get('irodsOOIHome', None):
                inst.obj_home = kwargs.get('irodsOOIHome', None)
            if kwargs.get('irodsUserName', None):
                inst.user_name = kwargs.get('irodsUserName', None)
            if kwargs.get('irodsUserPasswd', None):
                inst.user_passwd = kwargs.get('irodsUserPasswd', None)
            if kwargs.get('irodsZone', None):
                inst.zone = kwargs.get('irodsZone', None)
        
        inst.conn, errMsg = rcConnect(inst.hostname, int(inst.port_num), inst.user_name, inst.zone)

        if not inst.conn:
            logging.info('rcConnect() error: ' + errMsg)
            raise Exception('rcConnect error', errMsg)

        status = clientLoginWithPassword(inst.conn, inst.user_passwd)
        if status < 0:
            errName, subErrName = rodsErrorName(status)
            errMsg = str(status) + ':' + errName + ' ' + subErrName
            logging.info('rcConnect() error: ' + errMsg)
            raise Exception('clientLoginWithPassword() error', errMsg)

        irods_info = 'irods connection succeeded: ' + inst.hostname + '/' + inst.port_num
        logging.info(irods_info)

        return (inst)


    def clear_store(self):
        """
        @brief Delete the super column namespace. Do not touch default namespace!
        @note This is complicated by the persistence across many 
        """
        rcDisconnect(self.conn)

    def get_irods_fname_by_key(self, key):
        """
        @brief Return the irods file name based on the key value. This scheme is used for a single zone and assumes we store all data in a single collection.
        @param key The OOI unique key for an data object
        """
        fname = self.obj_home + '/' + key 
        return fname
        
    def get(self, key):
        """
        @brief Return a RODS value (the content of the file) corresponding to a given key
        @param key The sha1 hash key from OOI repository
        @retval Deferred, the content of the irods file, or None
        """

        logging.debug('reading value from iRODS for key %s' % (key))

        value = None
        try:
            # get irods obj filename with path from iRODS ICAT
            # get the content of the irods file
            self.fname = self.get_irods_fname_by_key(key)
        except Exception:
            pass

        f = iRodsOpen(self.conn, self.fname, "r")
        if not f:
            logging.info('Failed to open file for read: ' + self.fname)
            raise Exception('Failed to open file for read: ' + self.fname)

        value = f.read()
        f.close()

        return (value)

    def put(self, key, value):
        """
        @brief Write a the data into iRODS. 
        @param key 
        @param value Corresponding value
        @note Value is composed into iRODS under 'irods_ooi_home' with filename of the key value.
        @retval Deferred for success
        """

        logging.debug('writing data %s into iRODS' % (key))

        fname = self.get_irods_fname_by_key(key)
        f = iRodsOpen(self.conn, fname, 'w', self.default_resc)
        if not f:
            logging.info('Failed to open file for write: ' + fname)
            raise Exception('Failed to open file for write: ' + fname)

        f.write(value)
        f.close()

        return (None)

    def remove(self, key):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval Deferred, for success of operation
        @note Deletes are lazy, so key may still be visible for some time.
        """
 
        logging.debug('deleting data %s from iRODS' % (key))

        self.fname = self.get_irods_fname_by_key(key)
        dataObjInp = dataObjInp_t()
        dataObjInp.getCondInput().addKeyVal(FORCE_FLAG_KW, "")
        dataObjInp.setObjPath(self.fname)

        t = rcDataObjUnlink(self.conn, dataObjInp)

        if t < 0:
            errName, subErrName = rodsErrorName(t)
            errMsg = str(t) + ':' + errName + ' ' + subErrName
            logging.info('rcDataObjUnlink() error: ' + errMsg)
            raise Exception('rcDataObjUnlink() error', errMsg)

        return (None)

# for test purpose only
if __name__ == '__main__':
    irods_config = {'irodsHost':'ec2-204-236-151-209.us-west-1.compute.amazonaws.com', \
                  'irodsPort':'1247', \
                  'irodsOOIHome':'/tempZone/home/bzhu/OOI', \
                  'irodsUserName':'bzhu', \
                  'irodsUserPasswd':'bzhubzhu', \
                  'irodsZone':'tempZone', \
                  'rodsDefResource':'demoResc2'}

    if len(sys.argv) < 2:
        print 'Usage: ' + sys.argv[0] + ' key'
        sys.exit(0)

    key = sys.argv[1]

    sys.stderr.write('creating the store object ...\n')
    irods = IrodsStore.create_store(**irods_config)

    sys.stderr.write('put method ...\n')
    irods.put(key, 'ABCDEFGHIJK') 

    sys.stderr.write('get method ...\n')
    s = irods.get(key)
 
    sys.stderr.write('The content of the object -->\n')
    sys.stdout.write(s)
    sys.stdout.write('\n')

    import time
    time.sleep(1)
    
    sys.stderr.write('delete the object...\n')
    irods.remove(key)

    sys.stderr.write('test done.\n')
