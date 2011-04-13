{
    "type":"application",
    "name":"cassandra_initialization",
    "description": "Run the schema initialization for cassandra",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'cassandra_initialization',
        'ion.core.data.cassandra_bootstrap',
        'CassandraInitializationProcess'],
        {'cassandra_username':None,
        'cassandra_password':None,
        'keyspace':None, # Default to sysname!
        'error_if_existing':False}
    ),
    "registered": [
       "attributestore"
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}