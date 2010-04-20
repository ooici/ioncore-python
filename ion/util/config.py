#!/usr/bin/env python

"""
@file ion/util/config.py
@author Michael Meisinger
@brief  supports work with config files
"""

class Config(object):
    """Helper class managing config files
    """
    
    filename = None
    obj = None
    
    def __init__(self, cfgFile):
        self.filename = cfgFile
        filecontent = open(cfgFile,).read()
        self.obj = eval(filecontent)

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
