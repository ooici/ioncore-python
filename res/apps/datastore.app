{
    "type":"application",
    "name":"datastore",
    "description": "Data store service",
    "version": "0.1",
    "mod": ("ion.zapps.datastore", [], {'cassandra_username':None,'cassandra_password':None, 'do-init':True}),
    "modules": [
        "ion.zapps.datastore",
    ],
    "registered": [
    ],
    "applications": [
        "ioncore","ccagent"
    ],
    "config": {'ion.services.coi.datastore':{
        'blobs': 'ion.core.data.cassandra_bootstrap.CassandraStoreBootstrap',
        'commits': 'ion.core.data.cassandra_bootstrap.CassandraIndexedStoreBootstrap',},
        }
}
