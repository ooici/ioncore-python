# AIS Python Capability Container application
{
    "type":"application",
    "name":"notification_alert",
    "description": "ION notification alert management",
    "version": "0.1",
    "mod": ( "ion.core.pack.processapp", [
    			'notification_alert',
				'ion.integration.ais.notification_alert_service',
                'NotificationAlertService'
                ], {}
    ),
    "registered": [
        "notification_alert"
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}