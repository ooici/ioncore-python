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

from ion.data.test import test_dataobject

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
            filestem, ext = os.path.splitext(fname)
            if ext == '.nc':
                
                fullyqualifiedfile = os.path.join(fullyqualifieddir,fname)
                self.assertEqual(os.path.isfile(fullyqualifiedfile),True,'Bad file in data directory for tesing Dap Tools')
                logging.info('FILES:'+fullyqualifiedfile)
                
                self._inverse_test_from_nc(fullyqualifiedfile)
                self._inverse_test_from_dap(fullyqualifiedfile)
                

    def _inverse_test_from_nc(self,fname):
        orig_dataset = dap_tools.read_netcdf_from_file(fname)
    
        # Create a DAP message object out of it
        msg_obj = dap_tools.ds2dap_msg(orig_dataset)
                
        # Unpack the dataset from the message object
        unpacked_dataset = dap_tools.dap_msg2ds(msg_obj)
        
        self._comparison_test(unpacked_dataset, orig_dataset)
                
                
    def _inverse_test_from_dap(self,fname):
        
        # Load a pydap dataset from a netcdf file
        orig_msg = dap_tools.read_msg_from_dap_files(fname)
    
        orig_dataset = dap_tools.dap_msg2ds(orig_msg)
        
        # Create a DAP message object out of it
        msg_obj = dap_tools.ds2dap_msg(orig_dataset)
                
        # Unpack the dataset from the message object
        unpacked_dataset = dap_tools.dap_msg2ds(msg_obj)
        
        self._comparison_test(unpacked_dataset, orig_dataset)
        
        
    def _comparison_test(self,ds1,ds2):
                
        # Test for equality of the attributes
        for key, value in ds1.attributes.items():
            
            logging.debug('Global Attribute: %s, types %s, %s' % (key, type(value), type(ds2.attributes[key])) ) 
            
            if isinstance(value,numpy.ndarray):
                barray = value == ds2.attributes[key]
                self.assert_(barray.all(),'Global array type attribute is not equal')
            else:
                self.assertEqual(value, ds2.attributes[key])
        
        
        # Test for equality of the variables
        for key,value in ds1.items():

            logging.debug('Variable: %s, types %s, %s' % (key, type(value), type(ds2[key])) )           
            
            if isinstance(value, pydap.model.BaseType):
                barray =  value.data == ds2[key].data
                self.assert_(barray.all(), 'Variable %s array content is not equal!' % key)

            elif isinstance(value, pydap.model.GridType):
                
                if key == 'atmp':
                    print value.array.data
                    print ds2[key].array.data[:]
                barray =  value.array.data == ds2[key].array.data
                self.assert_(barray.all(),'Variable %s array content is not equal!' % key)
            else:
                # Structure comparison not implemented yet!
                self.assertEqual(False,ds2[key],'Not set up to handle structures yet')
            
            for attkey, attvalue in ds1[key].attributes.items():
                
                    logging.debug('Variable Att: %s, types %s, %s' % (attkey, type(attvalue), type(ds2[key].attributes[attkey])) )

                    if isinstance(attvalue,(numpy.ndarray, list)):
                        barray = attvalue == ds2[key].attributes[attkey]
                        if hasattr(barray,'all'):
                            barray = barray.all()
                        self.assert_(barray,'Variable array attribute are not equal')
                    elif isinstance(attvalue, (str,bool)):
                        self.assertEqual(attvalue, ds2[key].attributes[attkey])

                    else:
                        self.assertAlmostEqual(attvalue, ds2[key].attributes[attkey])
                
                    
    

        