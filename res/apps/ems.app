{
    "type":"application",
    "name":"EMS",
    "description": "Exchange Management Service",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'ems',
        'ion.services.coi.exchange.exchange_management',
        'ExchangeManagementService'], {}
    ),
    "registered": [
        "ems"
    ],
    "applications": [
        "ioncore",
        "resource",
    ]
}
