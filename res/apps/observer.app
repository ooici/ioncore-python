# Interaction Observer
# See https://confluence.oceanobservatories.org/display/syseng/Python+CC+Startup
{
    "type":"application",
    "name":"observer",
    "description": "ION Interaction Observer",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'int_observer',
        'ion.interact.int_observer',
        'InteractionObserver'], {'firstSpawnArg':'Here'}),
    "applications": [
        "ioncore", "ccagent"
    ],
}
