# Example Python Capability Container application
{
    "type":"application",
    "name":"notify_web_monitor",
    "description": "Notification Web Monitor",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'notify_web_monitor_1',
        'ion.services.dm.distribution.notify_web_monitor',
        'NotificationWebMonitorService'], {}
    ),
    "registered": [
    ],
    "applications": [
        "ioncore","ccagent","eventmonitor"
    ]
}
