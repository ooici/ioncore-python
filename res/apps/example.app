# Example Python Capability Container application
# See https://confluence.oceanobservatories.org/display/syseng/Python+CC+Startup
{
    "type":"application",
    "name":"example",
    "description": "ION Example application",
    "version": "0.1",
    "mod": ("ion.zapps.example", [], {}),
    "modules": [
        "ion.zapps.example",
    ],
    "registered": [
        "example"
    ],
    "applications": [
        "ioncore",
    ],
    "config":{
        "key":"value"
    },
}
