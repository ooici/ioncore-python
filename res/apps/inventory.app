# @file res/apps/inventory.app
# @author Matt Rodriguez
# @date 2/22/11
# @brief Cassandra Inventory Service
#

{
        "type":"application",
        "name":"CassandraInventoryService",
        "description": "Cassandra Inventory Service application",
        "version": "0.1",
        "mod": ("ion.zapps.inventory", []),
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
