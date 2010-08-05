import simplejson as json


EPUIFIED_SERVICES = ["simple_worker", "data_transform_v2"]

#Format: ("service_type", "service_module", "service_class", "extra_spawnargs")
SERVICE_CODES = [
    ("sensor_aggregator", "ion.services.cei.sensor_aggregator", "SensorAggregatorService", {}),
    ("rabbitmq_sensor", "ion.services.cei.rabbitmq_sensor", "RabbitMQSensor", {"rabbitmq_node":"rabbit@184.73.158.98", "erlang_cookie":"GILWSLAQFKTZNABMKMQP"}),
    ("work_producer", "ion.services.cei.sleeper.epu_work_producer", "EPUWorkProducer", {})
]

def create_controller_sensor_worker_spec(epuified_services, service_codes):
    """
    Dynamically create "services specs" give a list of epuified_services.

    Service names have the form: "$service__$service_type".
    Service Work Queues have the form: "cei__$service__work_queue"
    """
    service_specs = {}
    for service in epuified_services:
        for (service_type, service_module, service_class, extra_spawnargs) in service_codes:
            service_name = service+"__"+service_type
            work_queue = "cei__"+service+"__work_queue"
            extra_spawnargs.update({"queue_name_work":work_queue})
            service_specs[service_name] = str({"name":service_name, "module":service_module, "spawnargs":extra_spawnargs})
    return service_specs



if __name__ == "__main__":
    spec = create_controller_sensor_worker_spec(EPUIFIED_SERVICES, SERVICE_CODES)
    fh = open("services.json", "w")
    fh.write(json.dumps(spec))
    fh.close()
