# ION R1 Dispatcher application
{
    "type":"application",
    "name":"Dispatcher",
    "description": "Dispatcher App",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'dispatcher',
        'ion.integration.eoi.dispatcher.dispatcher',
        'DispatcherProcess'], {}
    ),
    "registered": [
        "dispatcher"
    ],
    "applications": [
        "ioncore",
    ]
}