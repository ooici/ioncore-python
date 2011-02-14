# @file res/apps/dm_bootstrap.app
# @author Paul Hubbard
# @date 2/7/11
# @brief DM bootstrap definition
#

{
        "type":"application",
        "name":"DataManagementBootstrap",
        "description": "Data Management bootstrap application",
        "version": "0.1",
        "mod": ("ion.zapps.dm_bootstrap", []),
        "modules": [
            "ion.zapps.dm_bootstrap",
        ],
        "registered": [
            "example"
        ],
        "applications": [
            "ioncore",
        ]
}
