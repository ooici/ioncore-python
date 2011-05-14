{
    "type":"application",
    "name":"dataset_controller",
    "description": "Dataset Controller",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'dataset_controller',
        'ion.services.dm.inventory.dataset_controller',
        'DatasetController'], {}
    ),
    "registered": [
       "dataset_controller"
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}
