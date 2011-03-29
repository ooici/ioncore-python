"""
@file ion/core/data/gen_cassandra_schema_script.py
@author Matt Rodriguez
@brief Generates cassandra-cli commands given the storage.cfg file as input.
"""


from ion.core.data import storage_configuration_utility as scu


def create_keyspace(pa_dict):
    """
    Create the keyspace definition
    """
    name = pa_dict["name"]
    f = lambda x: "".join((x[0],"=",str(x[1])))
    attrs = " and ".join(map(f, pa_dict.items()))
    command = " ".join(("create keyspace", name , "with", attrs, ";")) 
    print command
    print " ".join(("use",name,";"))

def create_column_families(cache_dict):
    """
    Create the column family definitions
    """
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
        print command
 
if __name__ == "__main__":
    scu_dict = scu.STORAGE_CONF_DICTIONARY
    create_keyspace(scu_dict[scu.PERSISTENT_ARCHIVE])
    create_column_families(scu_dict[scu.CACHE_CONFIGURATION]) 
