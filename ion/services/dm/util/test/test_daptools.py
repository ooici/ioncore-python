#!/usr/bin/env python

"""
@file ion/services/dm/util/test/test_daptools.py
@author David Stuebe
@date 7/30/2010
"""

import ion.util.ionlog
import gc
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from ion.test.iontest import IonTestCase
from twisted.internet import defer

from ion.data.test.test_dataobject import ResponseServiceClient
from ion.services.dm.util import dap_tools
import os
import exceptions

from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.data.test import test_dataobject

import pydap
import numpy



class DapToolsBaseTest(IonTestCase):
    """
    @Brief Test the message read and write for a DAP data Object.
    @Note The conversion of all value types should be tested, and any edge cases
    that come to mind. (Single values, arrays of values? Max and min IEEE
    numbers for all value types.)  It isn't minimal but it is probably necessary
    for everyone to be convinced it is OK.
    @TODO Flesh out the test cases with more data types!
    """
    fname ='../ion/services/dm/util/test/test_files/grid_surf_el.nc'
    testname = '../ion/services/dm/util/test/test_files/test_out.nc'
    def setUp(self):
        # Load the dataset from an nc file
        self.ds1 = dap_tools.read_netcdf_from_file(self.fname)
        
    def tearDown(self):
        #@TODO robustify this to use better exception handling
        try:
            os.remove(self.testname)
        except exceptions.OSError:
            pass
        gc.collect()
    
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        
        #print 'keys',self.ds1.keys()
        
        #print 'name',self.ds1.name
        
        # @todo fix the name - why does it not allow '.'?
        self.assertEqual(self.ds1.name,'grid_surf_el%2Enc')
        
        
        # @TODO - interface to netcdf handler objects seems broken - it does not match the methods for pydap.client?
        self.assertEqual(self.ds1.lat.data.var[0],1.)
        self.assertEqual(self.ds1.lat.data.var[6],1.75)
        
        self.assertEqual(self.ds1.lon.data.var[0],117.)
        self.assertEqual(self.ds1.lon.data.var[3],117.375)
        
        self.assertEqual(self.ds1.time.data.var[0],59040.)
        self.assertEqual(self.ds1.time.data.var[3],59049.)
        
        #print self.ds1.surf_el.array[:].var[:]
        #print self.ds1.surf_el.data[0].var[0][0][0]
        #print self.ds1.surf_el.data[0,0,0]
        

        # Very un interesting data set - all values == -30000
        self.assertEqual(self.ds1.surf_el.data[0].var[0][0][0],-30000)
        #self.assertEqual(self.ds1.surf_el.data.var[3],-2)
        
        #@TODO - check on the attributes in the data...
        #self.assertEqual(self.ds1.height.attributes['long_name'],'person height')
        


    def test_global_atts_ds1_ds2(self):
        # Convert to a message 
        self.msg1 = dap_tools.ds2dap_msg(self.ds1)
        # Convert back to a dataset
        self.ds2 = dap_tools.dap_msg2ds(self.msg1)
        # test the global atts!
        self._test_global_atts(self.ds1,self.ds2)
        
        
    def test_variables_ds1_ds2(self):
        # Convert to a message 
        self.msg1 = dap_tools.ds2dap_msg(self.ds1)
        # Convert back to a dataset
        self.ds2 = dap_tools.dap_msg2ds(self.msg1)
        # Test teh variables
        self._test_variables(self.ds1,self.ds2)
        
    def test_variables_atts_ds1_ds2(self):
        # Convert to a message 
        self.msg1 = dap_tools.ds2dap_msg(self.ds1)
        # Convert back to a dataset
        self.ds2 = dap_tools.dap_msg2ds(self.msg1)
        # Test the variable atts
        self._test_variable_atts(self.ds1,self.ds2)
        
    # Compaire the attributes, variables and variable attributes of the second two
    def test_global_atts_ds2_ds3(self):
        # Convert to a message 
        self.msg1 = dap_tools.ds2dap_msg(self.ds1)
        # Convert back to a dataset
        self.ds2 = dap_tools.dap_msg2ds(self.msg1)
        # Convert back to a message
        self.msg2 = dap_tools.ds2dap_msg(self.ds2)
        # Convert back to a dataste
        self.ds3 = dap_tools.dap_msg2ds(self.msg2)
        # Test the global atts
        self._test_global_atts(self.ds2,self.ds3)
        
    def test_variables_ds2_ds3(self):
        # Convert to a message 
        self.msg1 = dap_tools.ds2dap_msg(self.ds1)
        # Convert back to a dataset
        self.ds2 = dap_tools.dap_msg2ds(self.msg1)
        # Convert back to a message
        self.msg2 = dap_tools.ds2dap_msg(self.ds2)
        # Convert back to a dataste
        self.ds3 = dap_tools.dap_msg2ds(self.msg2)
        # Test the variables
        self._test_variables(self.ds2,self.ds3)
        
    def test_variables_atts_ds2_ds3(self):
        # Convert to a message 
        self.msg1 = dap_tools.ds2dap_msg(self.ds1)
        # Convert back to a dataset
        self.ds2 = dap_tools.dap_msg2ds(self.msg1)
        # Convert back to a message
        self.msg2 = dap_tools.ds2dap_msg(self.ds2)
        # Convert back to a dataste
        self.ds3 = dap_tools.dap_msg2ds(self.msg2)
        # Test the var_atts
        self._test_variable_atts(self.ds2,self.ds3)
    
    """    
    def test_write_read_vars(self):
        
        raise unittest.SkipTest('Problem with _test_variables - fails for reasons unknown...')
        ack = dap_tools.write_netcdf_from_dataset(self.ds1, self.testname)
        self.assertEqual(ack,0)
        
        self.ds2 = dap_tools.read_netcdf_from_file(self.testname)
        self._test_variables(self.ds1,self.ds2)
    """
    
    def test_write_read_var_atts(self):
        ack = dap_tools.write_netcdf_from_dataset(self.ds1, self.testname)
        self.assertEqual(ack,0)
        
        self.ds2 = dap_tools.read_netcdf_from_file(self.testname)
        self._test_variable_atts(self.ds1,self.ds2)
        
    def test_write_read_global_atts(self):
        ack = dap_tools.write_netcdf_from_dataset(self.ds1, self.testname)
        self.assertEqual(ack,0)
        
        self.ds2 = dap_tools.read_netcdf_from_file(self.testname)
        self._test_global_atts(self.ds1,self.ds2)
        
        
        
        
    # Make sure we can send the msg object as a message!
    @defer.inlineCallbacks
    def test_send_dap_msg(self):
        yield self._start_container()
        services = [
            {'name':'responder','module':'ion.data.test.test_dataobject','class':'ResponseService'},
        ]

        sup = yield self._spawn_processes(services)

        rsc = ResponseServiceClient(sup)
        
        # Simple Send and Check value:
        self.msg1 = dap_tools.ds2dap_msg(self.ds1)
        response = yield rsc.send_data_object(self.msg1)
        self.assertEqual(self.msg1, response)
        yield self._stop_container()
    
    # Define all the helper methods
    def _test_global_atts(self, ds1, ds2):    
        # Test for equality of the attributes
        for key, value in ds1.attributes.items():
            
            log.debug('Global Attribute: %s, types %s, %s' % (key, type(value), type(ds2.attributes[key])) ) 
            
            if isinstance(value,numpy.ndarray):
                barray = value == ds2.attributes[key]
                self.assert_(barray.all(),'Global array type attribute is not equal')
            else:
                self.assertEqual(value, ds2.attributes[key])

    def _test_variables(self,ds1,ds2):
        # Test for equality of the variables
        for key,value in ds1.items():

            log.debug('Variable: %s, types %s, %s' % (key, type(value), type(ds2[key])) )           
            
            if isinstance(value, pydap.model.BaseType):
                barray =  value.data == ds2[key].data         
                self.assert_(barray.all(), 'Variable %s Base Type array content is not equal!' % key)

            elif isinstance(value, pydap.model.GridType):
                
                #if key == 'wdir':
                #    print value.array.data.var[:]
                #    print ds2[key].array.data[0]
                barray =  value.array.data == ds2[key].array.data
                #barray =  value.array.data.var[:] == ds2[key].array.data[0]
                self.assert_(barray.all(),'Variable %s Grid Type array content is not equal!' % key)
            elif isinstance(value, pydap.model.StructureType):
                
                vs=value
                ds=ds2[key]
                
                self._test_variables(vs,ds)
                self._test_variable_atts(vs,ds)

                
            elif isinstance(value, pydap.model.SequenceType):
                self.assertEqual(value,'SequenceType Eqality check not implemented yet')
                
                

    def _test_variable_atts(self,ds1,ds2):  
        # Test for equality of the variables
        for key,value in ds1.items():

            log.debug('Variable: %s, types %s, %s' % (key, type(value), type(ds2[key])) )           
            
            for attkey, attvalue in ds1[key].attributes.items():
                
                    log.debug('Variable Att: %s, types %s, %s' % (attkey, type(attvalue), type(ds2[key].attributes[attkey])) )

                    if isinstance(attvalue,(numpy.ndarray, list)):
                        barray = attvalue == ds2[key].attributes[attkey]
                        if hasattr(barray,'all'):
                            barray = barray.all()
                        self.assert_(barray,'Variable array attribute are not equal')
                    elif isinstance(attvalue, (str,bool)):
                        self.assertEqual(attvalue, ds2[key].attributes[attkey])

                    else:
                        self.assertAlmostEqual(attvalue, ds2[key].attributes[attkey])
                

class DapToolsTest_GridWaterTemp(DapToolsBaseTest):
    fname ='../ion/services/dm/util/test/test_files/grid_water_temp.nc'
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'grid_water_temp%2Enc')

class DapToolsTest_GridWaterV(DapToolsBaseTest):
    fname ='../ion/services/dm/util/test/test_files/grid_water_v.nc'
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'grid_water_v%2Enc')
        
class DapToolsTest_StationAtmp(DapToolsBaseTest):
    fname ='../ion/services/dm/util/test/test_files/station_atmp.nc'
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'station_atmp%2Enc')
        
class DapToolsTest_StationBaro(DapToolsBaseTest):
    fname ='../ion/services/dm/util/test/test_files/station_baro.nc'
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'station_baro%2Enc')
        
"""        
class DapToolsTest_StationWdir(DapToolsBaseTest):
    fname ='../ion/services/dm/util/test/test_files/station_wdir.nc'
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'station_wdir%2Enc')
        
    def test_variables_ds1_ds2(self):
        # Running comparison between the object loaded from netcdf
        # and object loaded from dap fails. Not sure why?
        raise unittest.SkipTest('Problem with Pydap implementation')
    
"""

"""
class TestSimpleDataset(DapToolsBaseTest):
    
    def setUp(self):
        # create a dataset
        self.ds1 = dap_tools.simple_dataset(\
            {'DataSet Name':'SimpleData','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,6,4,-2,-1,5,3,1,4,5)})
        
    
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'SimpleData')
        
        
        self.assertEqual(self.ds1.time.data[0],111)
        self.assertEqual(self.ds1.time.data[9],120)
        
        self.assertEqual(self.ds1.height.data[9],5)
        self.assertEqual(self.ds1.height.data[3],-2)
        
        self.assertEqual(self.ds1.name,'SimpleData')
        
        self.assertEqual(self.ds1.height.attributes['long_name'],'person height')
        
    def test_write_read_vars(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write basetypes')
    def test_write_read_var_atts(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write basetypes')
    def test_write_read_global_atts(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write basetypes')
    
"""

"""
class TestSimpleSequenceDataset(DapToolsBaseTest):
    
    def setUp(self):
        # create a dataset
        self.ds1 = dap_tools.simple_sequence_dataset(\
            {'DataSet Name':'SimpleSequenceData','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,6,4,-2,-1,5,3,1,4,5)})
        
    
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'SimpleSequenceData')
        
        
        #print self.ds1.keys()
        #print self.ds1.sequence
        
        self.assertEqual(self.ds1.sequence.time.data[0],111)
        self.assertEqual(self.ds1.sequence.time.data[9],120)
        
        self.assertEqual(self.ds1.sequence.height.data[9],5)
        self.assertEqual(self.ds1.sequence.height.data[3],-2)
                
        self.assertEqual(self.ds1.sequence.height.attributes['long_name'],'person height')        
            
    def test_variables_ds2_ds3(self):
        raise unittest.SkipTest('Problem with Pydap xdr implementation: total size of new array must be unchanged')
    def test_variables_ds1_ds2(self):
        raise unittest.SkipTest('Problem with Pydap xdr implementation: total size of new array must be unchanged')
    def test_variables_atts_ds2_ds3(self):
        raise unittest.SkipTest('Problem with Pydap xdr implementation: total size of new array must be unchanged')
    def test_variables_atts_ds1_ds2(self):
        raise unittest.SkipTest('Problem with Pydap xdr implementation: total size of new array must be unchanged')
    def test_global_atts_ds2_ds3(self):
        raise unittest.SkipTest('Problem with Pydap xdr implementation: total size of new array must be unchanged')
    def test_global_atts_ds1_ds2(self):
        raise unittest.SkipTest('Problem with Pydap xdr implementation: total size of new array must be unchanged')

    def test_write_read_vars(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write sequencetypes')
    def test_write_read_var_atts(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write sequencetypes')
    def test_write_read_global_atts(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write sequencetypes')
    
"""    

"""
class TestSimpleStructureDataset(DapToolsBaseTest):
    
    def setUp(self):
        # create a dataset
        self.ds1 = dap_tools.simple_structure_dataset(\
            {'DataSet Name':'SimpleStructureData','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,6,4,-2,-1,5,3,1,4,5)})
        
    
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'SimpleStructureData')
        
        #print self.ds1.keys()

        self.assertEqual(self.ds1.structure.time.data[0],111)
        self.assertEqual(self.ds1.structure.time.data[9],120)
        
        self.assertEqual(self.ds1.structure.height.data[9],5)
        self.assertEqual(self.ds1.structure.height.data[3],-2)
                
        self.assertEqual(self.ds1.structure.height.attributes['long_name'],'person height')        
    
        
    def test_write_read_vars(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write structuretypes')
    def test_write_read_var_atts(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write structuretypes')
    def test_write_read_global_atts(self):
        raise unittest.SkipTest('Problem with Pydap Netcdf Handler implementation: can not read write structuretypes')
    
"""
class TestSimpleGridDataset(DapToolsBaseTest):
    
    def setUp(self):
        # create a dataset
        #self.ds1 = dap_tools.simple_grid_dataset(\
        #    {'DataSet Name':'SimpleGridData','variables':\
        #        {'time':{'long_name':'Data and Time','units':'seconds'},\
        #        'height':{'long_name':'person height','units':'meters','type':'grid','dimensions':'time'}}}, \
        #    {'time':(111,112,123,114,115,116,117,118,119,120), \
        #    'height':(8,6,4,-2,-1,5,3,1,4,5)})
        self.ds1 = dap_tools.simple_grid_dataset()
        
        
    
    def test_values(self):
        '''
        Make sure we got the data we expect...
        '''
        self.assertEqual(self.ds1.name,'SimpleGridData')
        
        print self.ds1.keys()
        print self.ds1.grid.array

        #self.assertEqual(self.ds1.height.time.data[0],111)
        #self.assertEqual(self.ds1.structure.time.data[9],120)
        
        #self.assertEqual(self.ds1.structure.height.data[9],5)
        #self.assertEqual(self.ds1.structure.height.data[3],-2)
                
        #self.assertEqual(self.ds1.structure.height.attributes['long_name'],'person height')        
        


        
    