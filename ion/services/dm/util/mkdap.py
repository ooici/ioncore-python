from optparse import OptionParser

from pydap.handlers.netcdf import Handler
from pydap.responses.dds import DDSResponse
from pydap.responses.das import DASResponse
from pydap.responses.dods import DODSResponse
from pydap.xdr import DapPacker

def dap_gen(ds):
    """ 
    The dods_output object is a generator with the dds preceding the
    xdr encoded DODS data. If you just want the xdr encoded then you
    need to do use the DapPacker
    """    

    for line in DapPacker(ds):
        yield line 

def parse(filename): 
    h = Handler(filename)
    ds = h.parse_constraints({'pydap.ce':(None,None)})

    dds_output = DDSResponse.serialize(ds) 
    das_output = DASResponse.serialize(ds)
    #dods_output = DODSResponse.serialize(ds) 
    dods = dap_gen(ds)
    return (dds_output[0], das_output[0], dods)

def serialize(filename, dap_components):
    """
    filename is the prefix of the file for the dds,das and dods files 
    dap_components is a tuple of the dds,das, dods output
    """
    filestem = filename.split(".")[0]
    dds_file = open(".".join((filestem, "dds")), "w")
    das_file = open(".".join((filestem, "das")), "w")
    dds_file.write(dap_components[0])
    das_file.write(dap_components[1])
    dds_file.close()
    das_file.close()
    dods = dap_components[2]
    #This breaks on some system 
    dods_file = open(".".join((filestem,"dods")), "w")
    for line in dods:
        dods_file.write(line)
    dods_file.close()    

    
        

def main():
    option_parser = OptionParser()
    option_parser.add_option("-f", "--file", dest="filename",
                             help="name of netcdf file")
    (options,args) = option_parser.parse_args()
    filename = options.filename
    dap_components = parse(filename) 
    serialize(filename, dap_components)   

if __name__ == "__main__":
    main()
