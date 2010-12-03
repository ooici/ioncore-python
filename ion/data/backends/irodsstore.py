#!/usr/bin/env python
"""
@file ion/data/backends/irodsstore.py
@author Bing Zhu
@brief Implementation of ion.data.store.IStore using PyIrods to interface a
       iRODS backend storage servers.
@Note Test cases for the iRODS backend are in ion.data.backends.test.test_irodsstore 
@Note The Python iRODS library package can be installed with: easy_install --find-links http://ooici.net/packages pyrods-irods

@TODO - all IStore interface methods must returned deferreds! See the IStore.py and the
python in memory dictionary implementation.
@TODO - do not generically catch excpetions.
@TODO - make sure this implementation of the IStore can run the IStore test cases
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer

from ion.core import ioninit
from ion.services.dm.preservation.store import IStore

from irods import *
from irods_error import *

CONF = ioninit.config(__name__)


class IrodsStoreError(Exception):
    """
    Exception class for IrodsStore
    """


class IrodsStore(IStore):
    """
    @brief Store interface for interacting with the iRODS distributed storage system
    @Note see http://www.irods.org 
    @Note The login info is stored in the ion config file.
    """
    def __init__(self, **kwargs):
        """
        @brief constructor to read iRODS info from args or the 'ion' config file
        """
        self.hostname = self._get_config('irodsHost')
        self.port_num = self._get_config('irodsPort')
        self.default_resc = self._get_config('irodsDefResource')
        self.obj_home = self._get_config('irodsOoiCollection')
        self.user_name = self._get_config('irodsUserName')
        self.user_passwd = self._get_config('irodsUserPasswd')
        self.zone = self._get_config('irodsZone')
        self.conn = None

        # can be overridden by args individually
        if kwargs:
            if kwargs.get('irodsHost', None):
                self.hostname = kwargs.get('irodsHost', None)
            if kwargs.get('irodsPort', None):
                self.port_num = kwargs.get('irodsPort', None)
            if kwargs.get('irodsDefResource', None):
                self.default_resc = kwargs.get('irodsDefResource', None)
            if kwargs.get('irodsOoiCollection', None):
                self.obj_home = kwargs.get('irodsOoiCollection', None)
            if kwargs.get('irodsUserName', None):
                self.user_name = kwargs.get('irodsUserName', None)
            if kwargs.get('irodsUserPasswd', None):
                self.user_passwd = kwargs.get('irodsUserPasswd', None)
            if kwargs.get('irodsZone', None):
                self.zone = kwargs.get('irodsZone', None)

    def _get_config(self, key):
        try:
            value = CONF[key]
        except KeyError:
            # TODO - do not catch generic exceptions! This should be a key error exception I believe
            value = None
        return value

    def connect_to_irods(self):
        """
        @brief creates a client connection with an iRODS server
        """
        self.conn, errMsg = rcConnect(self.hostname, int(self.port_num), self.user_name, self.zone)
        if not self.conn:
            logging.info('rcConnect() error: ' + errMsg)
            raise IrodsStoreError('rcConnect error: ' + errMsg)

        status = clientLoginWithPassword(self.conn, self.user_passwd)
        if status < 0:
            self.conn = None
            errName, subErrName = rodsErrorName(status)
            errMsg = str(status) + ':' + errName + ' ' + subErrName
            logging.info('rcConnect() error: ' + errMsg)
            raise IrodsStoreError('clientLoginWithPassword() error:' + errMsg)

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

        inst.connect_to_irods()

        return defer.succeed(inst)

    def disconnect_from_irods(self):
        """
        @brief close the TCP connection with the iRODS server
        """
        rcDisconnect(self.conn)
        self.conn = None
    
     
    def clear_store(self):
        """
        @brief Clean the iRODS collection and disonnect from iRODS
        """
        if not self.conn:
            return defer.succeed(None)

        collinp = collInp_t()
        collinp.setCollName(self.obj_home)
        collinp.addCondInputKeyVal(RECURSIVE_OPR__KW, '')
        collinp.addCondInputKeyVal(FORCE_FLAG_KW, '')
        rcRmColl(self.conn, collinp, 0)

        self.disconnect_from_irods()
        
        return defer.succeed(None)

    def _get_irods_fname_by_key(self, key):
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

        value = None
        self.fname = self._get_irods_fname_by_key(key)

        f = iRodsOpen(self.conn, self.fname, "r")
        if not f:
            logging.info('Failed to open file for read: ' + self.fname)
            #raise IrodsStoreError('Failed to open file for read: ' + self.fname)
            return defer.succeed(None)
        value = f.read()
        f.close()
        return defer.succeed(value)

    
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
            errMsg = self._get_errmsg_by_status(status)
            logging.info('rcCollCreate() error: ' + errMsg)
            raise IrodsStoreError('rcCollCreate() error: ' + errMsg)

        fname = self._get_irods_fname_by_key(key)
        f = iRodsOpen(self.conn, fname, 'w', self.default_resc)
        if not f:
            logging.info('Failed to open file for write: ' + fname)
            raise IrodsStoreError('Failed to open file for write: ' + fname)

        f.write(value)
        f.close()

        return defer.succeed(None)

    def _get_errmsg_by_status(self, t):
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

        self.fname = self._get_irods_fname_by_key(key)
        dataObjInp = dataObjInp_t()
        dataObjInp.getCondInput().addKeyVal(FORCE_FLAG_KW, "")
        dataObjInp.setObjPath(self.fname)

        t = rcDataObjUnlink(self.conn, dataObjInp)

        if t < 0:
            errMsg = self._get_errmsg_by_status(t)
            logging.info('rcDataObjUnlink() error: ' + errMsg)
            raise IrodsStoreError('rcDataObjUnlink() error: ' + errMsg)

        return defer.succeed(None)

