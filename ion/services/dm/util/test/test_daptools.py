#!/usr/bin/env python

"""
@file ion/services/dm/test/test_daptools.py
@author David Stuebe
@date 7/30/2010
"""

import logging
logging = logging.getLogger(__name__)

from twisted.trial import unittest

from ion.services.dm.util import dap_tools
import os

from ion.core import ioninit
CONF = ioninit.config(__name__)

import pydap
import numpy

class DapToolsTest(unittest.TestCase):
    """
    Test url routine, purely local.
    """
    def setUp(self):
        pass


    def test_files(self):
        """
        """
        dname = CONF.getValue('test_dir', default='../ion/services/dm/util/test/test_files')
        fullyqualifieddir = os.path.join(os.getcwd(),dname)
        
        self.assertEqual(os.path.isdir(fullyqualifieddir),True,'Bad data directory for tesing Dap Tools')
        fnames = os.listdir(fullyqualifieddir)
        
        for fname in fnames:
            if fname.rfind('.nc'):
                
                fullyqualifiedfile = os.path.join(fullyqualifieddir,fname)
                self.assertEqual(os.path.isfile(fullyqualifiedfile),True,'Bad file in data directory for tesing Dap Tools')
                logging.info('FILES:'+fullyqualifiedfile)
                
                self._compare_inverse(fullyqualifiedfile)
                
                
    def _compare_inverse(self,fname):
        
        # Load a pydap dataset from a netcdf file
        orig_dataset = dap_tools.read_netcdf_from_file(fname)
    
        # Create a DAP message object out of it
        msg_obj = dap_tools.ds2dap_msg(orig_dataset)
                
        # Unpack the dataset from the message object
        unpacked_dataset = dap_tools.dap_msg2ds(msg_obj)
        
                
        # Test for equality of the attributes
        for key, value in unpacked_dataset.attributes.items():
            
            logging.debug('Global Attribute: %s, types %s, %s' % (key, type(value), type(orig_dataset.attributes[key])) ) 
            
            if isinstance(value,numpy.ndarray):
                barray = value == orig_dataset.attributes[key]
                self.assert_(barray.all(),'Global array type attribute is not equal')
            else:
                self.assertEqual(value, orig_dataset.attributes[key])
        
        
        # Test for equality of the variables
        for key,value in unpacked_dataset.items():

            logging.debug('Variable: %s, types %s, %s' % (key, type(value), type(orig_dataset[key])) )           
            
            if isinstance(value, pydap.model.BaseType):

                self.assertEqual(unpacked_dataset[key].data.var,unpacked_dataset[key].data.var)

            elif isinstance(value, pydap.model.GridType):
                self.assertEqual(unpacked_dataset[key].array.data.var,unpacked_dataset[key].array.data.var)
            else:                
                self.assertEqual(unpacked_dataset[key],orig_dataset[key])
            
            
            
            for attkey, attvalue in unpacked_dataset[key].attributes.items():
                
                    logging.debug('Variable Att: %s, types %s, %s' % (attkey, type(attvalue), type(orig_dataset[key].attributes[attkey])) )

                    if isinstance(attvalue,(numpy.ndarray, list)):
                        barray = attvalue == orig_dataset[key].attributes[attkey]
                        self.assert_(barray.all(),'Variable array attribute are not equal')
                    else:
                        self.assertEqual(attvalue, orig_dataset[key].attributes[attkey])
                
                    
        

        