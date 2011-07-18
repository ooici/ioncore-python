# Example Python Capability Container application
{
    "type":"application",
    "name":"mscweb",
    "description": "MSC Web Monitor",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'mscweb_wtf',
        'ion.interact.mscweb',
        'MSCWebProcess'], {}
    ),
    "registered": [
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}
