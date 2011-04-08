# Example Python Capability Container application
# See https://confluence.oceanobservatories.org/display/syseng/Python+CC+Startup
{
    "type":"application",
    "name":"example_std",
    "description": "ION Example application",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'hello_service',
        'ion.play.hello_service',
        'HelloService'], {'firstSpawnArg':'Here'}),
    "applications": [
        "ioncore", "ccagent"
    ],
    "config":{
    },
}
