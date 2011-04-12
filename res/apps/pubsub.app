{
    "type":"application",
    "name":"PubSub",
    "description": "PubSubController Service",
    "version": "0.1",
    "mod":  ("ion.core.pack.processapp", [
        'pubsub',
        'ion.services.dm.distribution.pubsub_service',
        'PubSubService'], {}
    ),
    "registered": [
        "pubsub"
    ],
    "applications": [
        "ioncore",
        "ems"
    ]
}
