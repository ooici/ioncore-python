#!/usr/bin/env python

"""
@file ion/util/config.py
@author Michael Meisinger
@brief  supports work with config files
"""

import os.path

class Config(object):
    """
    Helper class managing config files
    """

    def __init__(self, cfgFile, config=None):
        """
        @brief Creates a new Config for retrieving configuration
        @param cfgFile filename or key within Config
        @param config if present, a Config instance for which the value given
            by cfgFile will be extracted
        """
        self.filename = cfgFile
        if config != None:
            # Get a value out of existing Config
            self.obj = config.getValue(cfgFile,{})
        else:
            # Load config from filename
            filecontent = open(cfgFile,).read()
            self.obj = eval(filecontent)

    def __getitem__(self, key):
        return self.obj[key]

    def getObject(self):
        return self.obj

    def _getValue(self, dic, key, default=None):
        if dic == None:
            return None
        return dic.get(key,default)

    def getValue(self, key, default=None):
        return self._getValue(self.obj, key, default)

    def getValue2(self, key1, key2, default=None):
        value = self.getValue(key1, {})
        return self._getValue(value, key2, default)

    def getValue3(self, key1, key2, key3, default=None):
        value = self.getValue2(key1, key2, {})
        return self._getValue(value, key3, default)

    def update_from_file(self, filename):
        if os.path.isfile(filename):
            # Load config override from filename
            filecontent = open(filename,).read()
            updates = eval(filecontent)
            self.update(updates)

    def update(self, updates):
        """
        Recursively updates configuration dict with values in given dict.
        """
        self._update_dict(self.obj, updates)

    def _update_dict(self, src, upd):
        """
        Recursively updates a dict with values in another dict.
        """
        assert type(src) is dict and type(upd) is dict
        for ukey,uval in upd.iteritems():
            if type(uval) is dict:
                if not ukey in src:
                    src[ukey] = {}
                self._update_dict(src[ukey], uval)
            else:
                src[ukey] = uval
