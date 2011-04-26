# Interaction Observer, with web front end
# See https://confluence.oceanobservatories.org/display/syseng/Python+CC+Startup
{
    "type":"application",
    "name":"lazyeye",
    "description": "ION Interaction Observer plus interface for lazyeye_web",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'lazy_eye',
        'ion.interact.lazy_eye',
        'LazyEye'], {'firstSpawnArg':'Here'}),
    "applications": [
        "ioncore", "ccagent"
    ],
}
