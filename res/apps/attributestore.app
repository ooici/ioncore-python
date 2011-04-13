{
    "type":"application",
    "name":"attributestore",
    "description": "Attribute Store service (dead simple, for testing)",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'attributestore',
        'ion.services.coi.attributestore',
        'AttributeStoreService'], {}
    ),
    "registered": [
       "attributestore"
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}
