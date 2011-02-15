# @file res/apps/preservation_manager.app
# @author Matt Rodriguez
# @date 2/14/11
# @brief preservation manager bootstrap definition
#

{
        "type":"application",
        "name":"PreservationManager",
        "description": "Manager that allocates and displays storage resources",
        "version": "0.1",
        "mod": ("ion.zapps.preservation_manager", []),
        "modules": [
            "ion.zapps.preservation_manager",
        ],
        "registered": [
            "example"
        ],
        "applications": [
            "ioncore",
        ]
}
