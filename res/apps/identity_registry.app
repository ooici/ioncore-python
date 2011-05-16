{
    "type":"application",
    "name":"identity_registry",
    "description": "Identity Registry Service",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'identity_registry',
        'ion.services.coi.identity_registry',
        'IdentityRegistryService'], {}
    ),
    "registered": [
       "identity_registry"
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}
