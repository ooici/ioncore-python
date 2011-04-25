# Interaction Observer, with web front end
# See https://confluence.oceanobservatories.org/display/syseng/Python+CC+Startup
    {
        "type":"application",
        "name":"lazyeye",
        "description": "ION Interaction Observer plus REST",
        "version": "0.1",
        "mod": ("ion.core.pack.processapp", [
            'int_observer',
            'ion.interact.int_observer',
            'InteractionObserver'], {'firstSpawnArg':'Here'}),
        "applications": [
            "ioncore", "ccagent"
        ],
    }
