{
    "type":"application",
    "name":"workbench",
    "description": "Workbench Service",
    "version": "0.1",
    "mod": ("ion.core.pack.processapp", [
        'workbench',
        'ion.core.object.test.workbench_service',
        'WorkBenchService'], {}
    ),
    "registered": [
       "workbench"
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}
