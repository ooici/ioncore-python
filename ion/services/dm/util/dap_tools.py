#!/usr/bin/env python

"""
@file ion/services/dm/util/dap_tools.py
@author Matt Rodriguez
@author David Stuebe
@brief Functions for working with dap data messages and dap datasets
"""

#This is for python2.5 compatibility    
from __future__ import with_statement 


from pydap.handlers.netcdf import Handler
from pydap.responses.dds import DDSResponse
from pydap.responses.das import DASResponse
from pydap.xdr import DapPacker

from pydap.parsers.dds import DDSParser
from pydap.parsers.das import DASParser
from pydap.xdr import DapUnpacker
from pydap.responses import netcdf
import base64
import StringIO

from pydap.model import BaseType,  DatasetType, Float32, Float64, \
    GridType, SequenceType, Int32

import numpy

import os
import warnings

from ion.resources import dm_resource_descriptions

def ds2dap_msg(pydap_dataset,headeronly=False): 
    """
    @Brief Convert a pydap dataset object to a dap encoded message content (a dictionary)
    """
    dds_output = DDSResponse.serialize(pydap_dataset) 
    das_output = DASResponse.serialize(pydap_dataset)
    #dods_output = DODSResponse.serialize(ds) 
    
    msg=dm_resource_descriptions.DAPMessageObject()
    msg.das = das_output[0]
    msg.dds = dds_output[0]
    if not headeronly:
        
        # Catch depricated warnings!
        # Unfortunately, catch_warnings is new in python 2.6 
        #with warnings.catch_warnings():
        #    warnings.simplefilter("ignore",category=DeprecationWarning)
        #    dods = dap_gen(pydap_dataset)
        dods = dap_gen(pydap_dataset)

        msg.dods = base64.b64encode(dods)
        #msg.dods = dods    
    return (msg)

def dap_gen(ds):
    """ 
    The dods_output object is a generator with the dds preceding the
    xdr encoded DODS data. If you just want the xdr encoded then you
    need to do use the DapPacker
    """    
    buf = StringIO.StringIO()
    for line in DapPacker(ds):
        buf.write(line)
        
    string = buf.getvalue()
        
    buf.close()
    return string
    
    


def dap_msg2ds(msg):
    """
    @Brief Convert dap encoded message content in a dictionary to a pydap dataset object.
    """
    dataset = DDSParser(msg.dds).parse()
    
    dataset = DASParser(msg.das, dataset).parse()
    
    if msg.dods:
        # This block is from open_dods in client.py
        dataset.data = DapUnpacker(base64.b64decode(msg.dods), dataset).getvalue()
        #dataset.data = DapUnpacker(msg.dods, dataset).getvalue()
        
    return dataset


def read_msg_from_dap_files(filename):
    filestem, ext = os.path.splitext(filename)
    dds_file = open(".".join((filestem, "dds"))) 
    dds = dds_file.read() 
    das_file = open(".".join((filestem, "das"))) 
    das = das_file.read()
    dods_file = open(".".join((filestem, "dods")))
    xdrdata = dods_file.read()
    
    msg=dm_resource_descriptions.DAPMessageObject()
    msg.das = das
    msg.dds = dds
    msg.dods = xdrdata   
    
    return msg

def write_dap_files_from_msg(filename,msg):
    filestem, ext = os.path.splitext(filename)
    dds_file = open(".".join((filestem, "dds")), "w")
    das_file = open(".".join((filestem, "das")), "w")
    dds_file.write(msg.dds)
    das_file.write(msg.das)
    dds_file.close()
    das_file.close()
    #This breaks on some system 
    dods_file = open(".".join((filestem,"dods")), "w")
    for line in msg.dods:
        dods_file.write(line)
    dods_file.close()    
    return 0

def write_netcdf_from_dataset(dataset, filename):
    netcdf.save(dataset, ".".join((filename, "nc")))
    return 0

def read_netcdf_from_file(filename): 
    h = Handler(filename)
    ds = h.parse_constraints({'pydap.ce':(None,None)})
    return ds


def demo_dataset():
    '''
    @Brief Example methods for creating a dataset
    http://pydap.org/developer.html#the-dap-data-model
    '''
    
    #Create a dataset object
    ds = DatasetType(name='Mine')
    
    #Add Some attributes
    ds.attributes['history']='David made a dataset'
    ds.attributes['conventions']='OOIs special format'
    
    # Create some data and put it in a variable
    varname = 'var1'
    data = (1,2,3,4,5,8)
    shape=(8,) 
    type = Int32 #
    dims=('time',)
    attributes={'long_name':'long variable name one'}
    ds[varname] = BaseType(name=varname, data=data, shape=shape, dimensions=dims, type=type, attributes=attributes)

    # Now make a grid data object
    g = GridType(name='g')
    data = numpy.arange(6.)
    data.shape = (2, 3)
    # The name in the dictionary must match the name in the basetype
    g['a'] = BaseType(name='a', data=data, shape=data.shape, type=Float32, dimensions=('x', 'y'))
    g['x'] = BaseType(name='x', data=numpy.arange(2.), shape=(2,), type=Float64)
    g['y'] = BaseType(name='y', data=numpy.arange(3.), shape=(3,), type=Float64)
    
    ds[g.name]=g

    return ds

def simple_sequence_dataset(metadata, data):
    '''
    Create a simple dap dataset object from dictionary content
    '''
    # Convert metadata and data to a dap dataset
    ds = DatasetType(name=metadata['DataSet Name'])
    sequence = SequenceType(name='s')
    for varname,atts in metadata['variables'].items():

        var = BaseType(name=varname, \
                data=data[varname], \
                shape=(len(data[varname]),), \
                dimensions=(varname,), \
                type=Int32, \
                attributes=atts)


        sequence[varname] = var
    ds[sequence.name] = sequence    
    return ds
    


def simple_dataset(metadata, data):
    '''
    Create a simple dap dataset object from dictionary content
    '''
    # Convert metadata and data to a dap dataset
    ds = DatasetType(name=metadata['DataSet Name'])
    
    for varname,atts in metadata['variables'].items():
        
        var = BaseType(name=varname, \
                data=data[varname], \
                shape=(len(data[varname]),), \
                dimensions=(varname,), \
                type=Int32, \
                attributes=atts)
        ds[varname] = var
    return ds
    
def simple_datamessage(metadata, data):
    ds = simple_dataset(metadata, data)
    return ds2dap_msg(ds)

