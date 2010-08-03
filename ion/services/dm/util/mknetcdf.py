from optparse import OptionParser

from pydap.parsers.dds import DDSParser
from pydap.parsers.das import DASParser
from pydap.xdr import DapUnpacker
from pydap.responses import netcdf

def parse_dataset(filename):
    dds_file = open(".".join((filename, "dds"))) 
    dds = dds_file.read() 
    dataset = DDSParser(dds).parse()
    das_file = open(".".join((filename, "das"))) 
    das = das_file.read()
    dataset = DASParser(das, dataset).parse()
    dods_file = open(".".join((filename, "dods")))
    xdrdata = dods_file.read()
    #dds, xdrdata = dods.split('\nData:\n', 1) 
    dataset.data = DapUnpacker(xdrdata, dataset).getvalue()
    netcdf.save(dataset, ".".join((filename, "nc")))
 
"""
def main():
    optparse = OptionParser()
    optparse.add_option("-f", "--file", dest="filename", 
                        help="name of the das,dds,dods file")

    (options, args) = optparse.parse_args()
    filename = options.filename
    parse_dataset(filename)

if __name__ == "__main__":
    main()
"""