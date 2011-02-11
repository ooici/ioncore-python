from ion.core.process.service_process import ServiceProcess
from ion.core.process.process import ProcessFactory

class EPUManagementService(ServiceProcess):
    """EPU Management service interface

    The system is bootstrapped with a launch plan using the CEI bootstrap
    tool ("cloudinit.d").  When the system has reached an operational state
    without error, the EPU system is in effect, compensating for load and
    failures.

    This service manages the EPU system as a whole through remote messages.

    It is not implemented; when it is implemented it will be backed almost
    entirely by the functionality currently in the "epumgmt" tool via API.
    More basic things like launching new EPUs entirely into a running system
    will be a combination of cloudinit.d API and epumgmt API functionality.
    Currently the cloudinit.d and epumgmt tools are run from the commandline.
    
    This class represents the service interface in order to architecturally
    represent "EPU management from the outside" as a service itself which
    can be integrated via AMQP instead of commandline/scripts.
    """

    declare = ServiceProcess.service_declare(name='epu_management', version='0.1.0', dependencies=[])

    def slc_init(self):
        """Initialize the service.

        Reads in all credentials and IaaS coordinates available for
        bootstrapping new EPUs.  Also there would be a datastore dependency
        configuration.
        """
        pass

    def op_create_system(self, content, headers, msg):
        """Create an entirely new system based on the input launch plan.

        This is a pass-through to the cloudinit.d "boot" subcommand via API.

        The return message mere indicates success or failure.  To query an
        in-progress, failed or launched run, the client would interact with
        the datastore to get the latest information.

        Input: one serialized launch plan.
        """
        pass

    def op_destroy_system(self, content, headers, msg):
        """Entirely destroy a booted system.

        This will cause the provisioner node to intiate terminations on all
        known workers via IaaS.  Then it will move to destroying anything
        launched by the bootstrap tool.

        Input: flag to indicate whether or not log files should be retrieved
        or not before destruction (for post-mortem analysis).
        """
        pass
    
    def op_add_epu(self, content, headers, msg):
        """Create a new EPU entirely.

        Brings one or many services into being initially, using the EPU
        infrastructure.

        Everything in the input service spec will have an EPU controller
        running on the same instance that this will launch.

        If the services have dependencies that must be running ahead of time,
        it is assumed the operator understands that these dependencies will
        need to be resolved and operational already.  Launch-plans are
        "vetted" as a whole, but adding new EPUs on top of an already
        launched system is a manual decision that requires knowledge of what
        the system is capable of sustaining change-wise.

        Input: list of one to many service specs (just a "piece" of a normal
        launch plan).  The IaaS coordinates and credentials must have been
        given to the service at initialization time.
        """
        pass

    def op_remove_epu(self, content, headers, msg):
        """Remove an EPU entirely.

        The same caveats apply as in the add_epu operation: the system needs
        to be able to handle such a thing.  If a service is deprecated, it
        is implied that the messages drain out before the entire capability
        is removed.  This will need to be taken care of by a higher level
        metric/tool to know when it is safe to delete an EPU entirely.

        Note that this is different than reducing the node count of a certain
        worker set to zero (see the reconfigure operation).  That can create
        the same temporary effect of course (no workers for this service in
        the system) but that, for example, leaves room for workers coming
        online when the demand increases past a certain threshold.
        """
        pass

    def op_reconfigure_epu(self, content, headers, msg):
        """Reconfigure a running EPU with a new policy.

        Given the name of one or many EPU controller(s) in the system, this
        is a convenience operation that ensures the new policy is configured.

        An EPU controller's policy is dictated by its "decision engine".
        The default decision engine supports policy reconfiguration but not
        all of them are required to.  An attempt to try to configure an EPU
        controller with such a decision engine will result in a harmless error.

        See the NPreservingEngine class notes for reconfiguration details.
        """
        pass

    def op_find_workers(self, content, headers, msg):
        """Interact with the provisioner to discover any new worker nodes
        that were launched in the system since the last query.

        Input: optionally filter by a particular HA service.

        Returns the newest workers, before return it has updated the
        datastore with any new information.
        """
        pass

    def op_service_status(self, content, headers, msg):
        """Return the status of one or more services.  Each service status
        will list the:

        1) Known workers, past and present.  Instance information, time of
        launch (and failure/termination if applicable), hostnames.

        2) Worker status: IaaS status as well as the more "semantic" knowledge
        of health that is acquired via heartbeats (or the lack thereof).
        """
        pass

# Direct start of the service as a process with its default name
factory = ProcessFactory(EPUManagementService)
