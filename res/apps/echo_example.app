{
    "type":"application",
    "name":"echo_example",
    "description": "Run the echo service example",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'echo_service',
        'ion.core.process.test.test_service',
        'EchoService'],
        {}
    ),
    "registered": [ ],
    "applications": [
        "ioncore","ccagent"
    ]
}