"""
@file ion/core/data/gen_cassandra_schema_script.py
@author Matt Rodriguez
@brief Generates cassandra-cli commands given the storage.cfg file as input.
"""

from optparse import OptionParser
import copy,sys
import os.path
from ion.util.config import Config


def create_keyspace(pa_dict):
    """
    Create the keyspace definition
    """
    if pa_dict["name"] == "": 
        name = "sysname"
    del pa_dict["name"]
    f = lambda x: "".join((x[0],"=",str(x[1])))
    attrs = " and ".join(map(f, pa_dict.items()))
    command = " ".join(("create keyspace", name , "with", attrs, ";")) 
    print command
    print " ".join(("use",name,";"))

def create_column_families(cache_dict):
    """
    Create the column family definitions
    """
    column_dict = {"column_name": "", "validation_class": "UTF8Type", "index_type":"KEYS"} 
    for cf in cache_dict.keys():
        indexed_cols = cache_dict[cf]['indexed columns']
        cols = []
        for col in indexed_cols:
            col_metadata = dict(column_dict)
            col_metadata.update({"column_name": col}) 
            cols.append(col_metadata)
        attrs = " with comparator=UTF8Type " 
        if len(cols) > 0:
           attrs = attrs + " and column_metadata=" + str(cols)

        command = " ".join(("create column family", cf, attrs, ";"))
        print command
 
if __name__ == "__main__":
    parser = OptionParser() 
    parser.add_option("-f", "--file", dest="filename", help="configuration file for the Cassandra Cluster")
    options, args = parser.parse_args() 
    error_message = "Problem with configuration file. You probably forget to pass the name in with -f"
    try: 
        ok = os.path.isfile(options.filename)
    except TypeError, ex:
        print error_message
        print ex.args
        sys.exit(-1)
    if not ok:    
        print error_message
        sys.exit(-1)    
        

    config = Config(options.filename) 
    create_keyspace(config["persistent archive"])
    create_column_families(config["cache configuration"]) 
