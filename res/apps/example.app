# Example Python Capability Container application
{
    "type":"application",
    "name":"example",
    "description": "Channel allocator",
    "version": "1",
    "modules": [
        "ch_app",
        "ch_sup",
        "ch3"
    ],
    "registered": [
        "ch3"
    ],
    "applications": [
        "kernel",
        "stdlib",
        "sasl"
    ],
    "mod": ("ch_app",[])
}
