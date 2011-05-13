# AIS Python Capability Container application
{
        
    "type":"application",
    "name":"app_integration",
    "description": "ION resource management application",
    "version": "0.1",
    
	"mod": ("ion.core.pack.processapp", [
        'app_integration',
        'ion.integration.ais.app_integration_service',
        'AppIntegrationService'], {}
    ),
    "registered": [
        "app_integration"
    ],
    "applications": [
        "ioncore","ccagent"
    ]
}