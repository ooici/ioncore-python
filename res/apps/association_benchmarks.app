{
    "type":"application",
    "name":"association",
    "description": "ION association management application",
    "version": "0.1",
    "mod": ("ion.zapps.association_benchmarks", [],{'username':None,'password':None}),
    "modules": [
        "ion.zapps.association_benchmarks",
    ],
    "registered": [
        "association"
    ],
    "applications": [
        "datastore", "ioncore","ccagent"
    ],
    "config": {'ion.services.dm.inventory.association_service':{
        'index_store_class': 'ion.core.data.store.IndexStore',},
        }
}