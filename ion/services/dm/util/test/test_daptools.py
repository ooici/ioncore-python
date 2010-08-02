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
                print 'FILES:',fullyqualifiedfile
                
                self._compare_inverse(fullyqualifiedfile)
                
                
    def _compare_inverse(self,fname):
        
        orig_dataset = dap_tools.read_netcdf_from_file(fname)
    
        msg_content = dap_tools.ds2dap_msg(orig_dataset)
        
        #print msg_content.keys()
        
        
        unpacked_dataset = dap_tools.dap_msg2ds(msg_content)
        
        #unpacked_dataset.attributes['NC_GLOBAL']['ooi-source-url'] = 'STRING!'
                
            
        for key, value in unpacked_dataset.attributes.items():            
            self.assertEqual(value, orig_dataset.attributes[key])
        
        
        for key,value in unpacked_dataset.items():
            
            logging.info('Comparing values for variable:' +key)           
            
            for attkey, attvalue in unpacked_dataset[key].attributes.items():
                    self.assertEqual(attvalue, orig_dataset[key].attributes[attkey])
        
            if isinstance(value, pydap.model.BaseType):

                self.assertEqual(unpacked_dataset[key].data.var,unpacked_dataset[key].data.var)

            elif isinstance(value, pydap.model.GridType):
                self.assertEqual(unpacked_dataset[key].array.data.var,unpacked_dataset[key].array.data.var)
            else:                
                self.assertEqual(unpacked_dataset[key],orig_dataset[key])
   
        
        print 'TESTING WRITE:'
        
        #dap_tools.write_dap_files_from_msg('testme',msg_content)
        
        
        