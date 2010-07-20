#!/usr/bin/env python

import logging
logging = logging.getLogger(__name__)
from ion.core import ioninit
CONF = ioninit.config(__name__)
import inspect
from ion.data import dataobject

from ion.resources import coi_resource_descriptions, cei_resource_descriptions, dm_resource_descriptions, ipaa_resource_descriptions, sa_resource_descriptions

descriptions=[coi_resource_descriptions,cei_resource_descriptions, dm_resource_descriptions, ipaa_resource_descriptions, sa_resource_descriptions]

"""
def load_descriptions():
    '''
    Not sure why this barfs?
    '''
    logging.info('*********RUNNIG LOAD DESCRIPTIONS')
    modules=CONF.getObject()
    if not modules:
        logging.warning('Found no Resource Description modules to load!')

    for module in modules:
        logging.info('Loading Module DataObjects: %s' % module)

        mod = __import__(module)
        print 'mod',mod
        for name in dir(mod):
            print 'name',name
            obj = getattr(mod, name)
            if inspect.isclass(obj):
                if issubclass(obj, dataobject.DataObject):
                    print "Adding Object:",name
                    dataobject.DataObject._types[name]=obj

    print 'DataObject Types:',dataobject.DataObject._types
"""

def load_descriptions():
    
    for module in descriptions:
        for name in dir(module):
            obj = getattr(module, name)
            if inspect.isclass(obj):
                if issubclass(obj, dataobject.DataObject):
                    dataobject.DataObject._types[name]=obj
    
'''
from ion.resources.description_utility import load_descriptions
load_descriptions()

from ion.resources.description_utility import load_desc
load_desc()

'''