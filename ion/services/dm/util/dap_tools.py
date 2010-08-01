from optparse import OptionParser

from pydap.handlers.netcdf import Handler
from pydap.responses.dds import DDSResponse
from pydap.responses.das import DASResponse
from pydap.responses.dods import DODSResponse
from pydap.xdr import DapPacker

from pydap.parsers.dds import DDSParser
from pydap.parsers.das import DASParser
from pydap.xdr import DapUnpacker
from pydap.responses import netcdf

import StringIO

NULL_CHR = '\x00'

def ds2dap_msg(pydap_dataset,headeronly=False): 
    """
    @Brief Convert a pydap dataset object to a dap encoded message content (a dictionary)
    """
    dds_output = DDSResponse.serialize(pydap_dataset) 
    das_output = DASResponse.serialize(pydap_dataset)
    #dods_output = DODSResponse.serialize(ds) 
    
    msg ={}
    msg['das'] = das_output[0]
    msg['dds'] = dds_output[0]
    if not headeronly:
        dods = dap_gen(pydap_dataset)
        msg['dods'] = dods
        #print 'DODS:',dods
    
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
    dataset = DDSParser(msg['dds']).parse()
    
    dataset = DASParser(msg['das'], dataset).parse()
    
    if msg.has_key('dods'):
        #dataset.data = DapUnpacker(msg['dods'], dataset).getvalue()

        # This block is from open_dods in client.py
        #dds, xdrdata = msg['dods'].split('\nData:\n', 1)
        dataset.data = DapUnpacker(msg['dods'], dataset).getvalue()
        
        #data = DapUnpacker(msg['dods'], dataset)
        #print 'DapUnpacker Data:',data.__dict__.keys()
        
    return dataset
 

def read_msg_from_dap_files(filename):
    dds_file = open(".".join((filename, "dds"))) 
    dds = dds_file.read() 
    das_file = open(".".join((filename, "das"))) 
    das = das_file.read()
    dods_file = open(".".join((filename, "dods")))
    xdrdata = dods_file.read()
    msg ={}
    msg['das'] = das
    msg['dds'] = dds
    msg['dods'] = xdrdata    
    return msg

def write_dap_files_from_msg(filename,msg):

    filestem = filename.split(".")[0]
    dds_file = open(".".join((filestem, "dds")), "w")
    das_file = open(".".join((filestem, "das")), "w")
    dds_file.write(msg['dds'])
    das_file.write(msg['das'])
    dds_file.close()
    das_file.close()
    #This breaks on some system 
    dods_file = open(".".join((filestem,"dods")), "w")
    for line in msg['dods']:
        dods_file.write(line)
    dods_file.close()    
    return 0


def write_netcdf_from_dataset(filename, dataset):
    netcdf.save(dataset, ".".join((filename, "nc")))
    return 0
 
def read_netcdf_from_file(filename): 
    h = Handler(filename)
    ds = h.parse_constraints({'pydap.ce':(None,None)})
    return ds

