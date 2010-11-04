#!/usr/bin/env python
"""
@file ion/data/backends/irodsstore.py
@author Bing Zhu
@brief Implementation of ion.data.store.IStore using PyIrods to interface a
       iRODS backend storage servers.
@Note Test cases for the iRODS backend are in ion.data.backends.test.test_irodsstore 
@Note The Python iRODS library package can be installed with: easy_install --find-links http://ooici.net/packages pyrods-irods
@
"""

#import re
#import sys
import logging
logging = logging.getLogger(__name__)

#from twisted.internet import defer

from ion.core import ioninit
from ion.data.store import IStore

#import uuid

from irods import *
from irods_error import *

CONF = ioninit.config(__name__)


class IrodsStore(IStore):
    """
    Store interface for interacting with the iRODS distributed storage system
    @see http://www.irods.org 
    @Note The login info is stored in the ion config file.
    """
    def __init__(self, **kwargs):
        """
        @brief constructor to read iRODS info from args or the 'ion' config file
        """
        self.hostname = self.get_config('irodsHost')
        self.port_num = self.get_config('irodsPort')
        self.default_resc = self.get_config('rodsDefResource')
        self.obj_home = self.get_config('irodsOoiCollection')
        self.user_name = self.get_config('irodsUserName')
        self.user_passwd = self.get_config('irodsUserPasswd')
        self.zone = self.get_config('irodsZone')
        self.conn = None

        # can be overridden by args individually
        if kwargs:
            if kwargs.get('irodsHost', None):
                self.hostname = kwargs.get('irodsHost', None)
            if kwargs.get('irodsPort', None):
                self.port_num = kwargs.get('irodsPort', None)
            if kwargs.get('rodsDefResource', None):
                self.default_resc = kwargs.get('rodsDefResource', None)
            if kwargs.get('irodsOoiCollection', None):
                self.obj_home = kwargs.get('irodsOoiCollection', None)
            if kwargs.get('irodsUserName', None):
                self.user_name = kwargs.get('irodsUserName', None)
            if kwargs.get('irodsUserPasswd', None):
                self.user_passwd = kwargs.get('irodsUserPasswd', None)
            if kwargs.get('irodsZone', None):
                self.zone = kwargs.get('irodsZone', None)

    def get_config(self, key):
        try:
            value = CONF[key]
        except:
            value = None
        return value

    def connect_to_irods(self):
        """
        @brief creates a client connection with an iRODS server
        """
        self.conn, errMsg = rcConnect(self.hostname, int(self.port_num), self.user_name, self.zone)
        if not self.conn:
            logging.info('rcConnect() error: ' + errMsg)
            raise Exception('rcConnect error', errMsg)

        status = clientLoginWithPassword(self.conn, self.user_passwd)
        if status < 0:
            self.conn = None
            errName, subErrName = rodsErrorName(status)
            errMsg = str(status) + ':' + errName + ' ' + subErrName
            logging.info('rcConnect() error: ' + errMsg)
            raise Exception('clientLoginWithPassword() error', errMsg)

        irods_info = 'irods connection succeeded: ' + self.hostname + '/' + self.port_num
        logging.info(irods_info)

    @classmethod
    def create_store(cls, **kwargs):
        """
        @brief factory method to create an create an instance of the irods store 
        @param from optional kwargs 
        @retval IStore instance or None
        """
        inst = cls(**kwargs)

        inst.kwargs = kwargs

        try:
            inst.connect_to_irods()
        except Exception:
            pass

        return (inst)

    def disconnect_from_irods(self):
        """
        @brief close the TCP connection with the iRODS server
        """
        rcDisconnect(self.conn)

    def clear_store(self):
        """
        @brief Clean the iRODS collection and disonnect from iRODS
        """
        if not self.conn:
            return

        collinp = collInp_t()
        collinp.setCollName(self.obj_home)
        collinp.addCondInputKeyVal(RECURSIVE_OPR__KW, '')
        collinp.addCondInputKeyVal(FORCE_FLAG_KW, '')
        rcRmColl(self.conn, collinp, 0)

        self.disconnect_from_irods()

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
        @retval the content of the irods file, or None
        """
        logging.debug('reading value from iRODS for key %s' % (key))

        if not self.conn:
            print '\n\n get() the conn  is null.\n'
            return None

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
        @retval None for success
        """

        logging.debug('writing data %s into iRODS' % (key))

        if not self.conn:
            return None

        #create the collection 'obj_home' if it does not exist.
        collinp = collInp_t()
        collinp.setCollName(self.obj_home)
        status = collinp.addCondInputKeyVal(RECURSIVE_OPR__KW, "")
        status = rcCollCreate(self.conn, collinp)
        if status < 0:
            errMsg = self.get_errmsg_by_status(status)
            logging.info('rcCollCreate() error: ' + errMsg)
            raise Exception('rcCollCreate() error', errMsg)

        fname = self.get_irods_fname_by_key(key)
        f = iRodsOpen(self.conn, fname, 'w', self.default_resc)
        if not f:
            logging.info('Failed to open file for write: ' + fname)
            raise Exception('Failed to open file for write: ' + fname)

        f.write(value)
        f.close()

        return (None)

    def get_errmsg_by_status(self, t):
        errName, subErrName = rodsErrorName(t)
        errMsg = str(t) + ':' + errName + ' ' + subErrName
        return errMsg

    def remove(self, key):
        """
        @brief delete an iRODS file by OOICI key
        @param key Key to delete
        @retval None for success of operation
        """
 
        logging.debug('deleting data %s from iRODS' % (key))

        self.fname = self.get_irods_fname_by_key(key)
        dataObjInp = dataObjInp_t()
        dataObjInp.getCondInput().addKeyVal(FORCE_FLAG_KW, "")
        dataObjInp.setObjPath(self.fname)

        t = rcDataObjUnlink(self.conn, dataObjInp)

        if t < 0:
            errMsg = self.get_errmsg_by_status(t)
            logging.info('rcDataObjUnlink() error: ' + errMsg)
            raise Exception('rcDataObjUnlink() error', errMsg)

        return (None)

