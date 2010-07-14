#!/usr/bin/env python

import logging
logging = logging.getLogger(__name__)
from ion.core import ioninit
CONF = ioninit.config(__name__)
import inspect
from ion.data import dataobject

"""
def load_descriptions():

    logging.warning('*********RUNNIG LOAD DESCRIPTIONS')
    modules=CONF.getObject()
    if not modules:
        logging.warning('Found no Resource Description modules to load!')

    for module in modules:
        print 'X+X+X+XX+X+X+X+X+X+X+X+X+',module
        mod = __import__(module)

        for name in dir(mod):
            obj = getattr(mod, name)
            if inspect.isclass(obj):
                if issubclass(obj, dataobject.DataObject):
                    dataobject.DataObject._types[name]=obj

"""
