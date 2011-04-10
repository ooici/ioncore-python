"""
@file ion/core/data/gen_cassandra_schema_script.py
@author Matt Rodriguez
@brief Generates cassandra-cli commands given the storage.cfg file as input.
"""


from ion.core.data import storage_configuration_utility as scu
import sys, subprocess, os

def create_keyspace(pa_dict):
    """
    Create the keyspace definition
    """
    name = pa_dict["name"]
    f = lambda x: "".join((x[0],"=",str(x[1])))
    attrs = " and ".join(map(f, pa_dict.get("attrs",{}).items()))
    command = " ".join(("create keyspace", name, "with", attrs, ";"))
    return "\n".join([command, " ".join(("use",name,";"))])

def create_column_families(cache_dict):
    """
    Create the column family definitions
    """
    output = []

    column_dict = {"column_name": "", "validation_class": "BytesType", "index_type":"KEYS"} 
    for cf in cache_dict.keys():
        indexed_cols = cache_dict[cf]['indexed columns']
        cols = []
        for col in indexed_cols:
            col_metadata = dict(column_dict)
            col_metadata.update({"column_name": col}) 
            cols.append(col_metadata)
        attrs = " with comparator=BytesType " 
        if len(cols) > 0:
           attrs = attrs + " and column_metadata=" + str(cols)

        command = " ".join(("create column family", cf, attrs, ";"))
        output.append(command)

    return "\n".join(output)

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print "Usage: %s <path_to_cassandra_cli_binary> <sysname>" % sys.argv[0]
        sys.exit(1)

    if not os.path.exists(sys.argv[1]):
        print "Cassandra binary (%s) not found." % sys.argv[1]
        sys.exit(1)

    scu_dict            = scu.get_storage_conf_dict(sysname=sys.argv[2])

    cassandra_cli_bin   = sys.argv[1]
    host                = scu_dict[scu.STORAGE_PROVIDER]['host']
    port                = str(scu_dict[scu.STORAGE_PROVIDER]['port'])

    strkeyspace         = create_keyspace(scu_dict[scu.PERSISTENT_ARCHIVE])
    strcolumns          = create_column_families(scu_dict[scu.CACHE_CONFIGURATION])

    input               = "\n".join([strkeyspace, strcolumns])

    args                = [cassandra_cli_bin, "--host", host, "--port", port]
    subp                = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=sys.stdout, stderr=sys.stderr, shell=False)
    subp.communicate(input)

    print "\n\n"


