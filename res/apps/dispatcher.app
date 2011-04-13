# Example Python Capability Container application
{
    "type":"application",
    "name":"dispatcher_dependencies_bundle",
    "description": "ION dispatcher dependencies application bundle",
    "version": "0.1",
    "mod": ("ion.zapps.dispatcher", []),
    "modules": [
        "ion.zapps.dispatcher",
    ],
    "registered": [
        "dispatcher"
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}