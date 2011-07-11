{
    "type":"application",
    "name":"EventMonitor",
    "description": "Event Notifications Monitoring Service",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'eventmonitor',
        'ion.services.dm.distribution.eventmonitor',
        'EventMonitorService'], {}
    ),
    "registered": [
        "EventMonitor"
    ],
    "applications": [
        "ioncore",
        "pubsub"
    ]
}
