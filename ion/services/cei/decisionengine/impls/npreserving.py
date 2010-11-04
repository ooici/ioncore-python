import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import random

from ion.services.cei.decisionengine import Engine
from ion.services.cei.epucontroller import LaunchItem
import ion.services.cei.states as InstanceStates

from ion.services.cei.epucontroller import PROVISIONER_VARS_KEY

BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]

CONF_PRESERVE_N = "preserve_n"
CONF_UNIQUE = "unique_instances"

class NpreservingEngine(Engine):
    """
    A decision engine that maintains N instances of the compensating units.
    
    This engine is reconfigurable, you can give it a new policy during operation
    and it will respect the new policy on the next 'decide' call.  This is
    detailed in Section I below.
    
    You can also supply it with unique configurations to give unique deployable
    units. This is detailed in Section II.
    
    There are some corner cases to consider with unique configurations, these
    are discussed in Section III.
    
    Those unique configurations are also reconfigurable.  The details of this
    are discussed in Section IV.
    
    
    I. Reconfiguration
    ------------------
    
    The typical Npreserving engine configuration would look like this:
    
    'engine_conf':{
       'preserve_n':'4',
       'provisioner_vars':{'exchange_scope':'XYZ',
                           'epuworker_image_id':'ami-XYZ',
                           'broker_ip_address':'w.x.y.z',
                           'pip_package_repo':'http://ooici.net/packages',
                           'git_lcaarch_repo':'http://github.com/ooici/lcaarch.git',
                           'git_lcaarch_branch':'develop',
                           'git_lcaarch_commit':'HEAD'}
    }
    
    The "provisioner_vars" configuration in this is a group of key/values
    that are sent to every single instance.  Nothing is different about any
    of the instance launches via IaaS.  A new compensating VM will get the
    same dynamic information via contextualization that any others did.
    
    If you used this kind of configuration and called "reconfigure" with
    a new "preserve_n" value only, the other "provisioner_vars" values
    will remain.
    
    For example, you can send this as the payload of a reconfigure operation:
    
    'engine_conf':{
       'preserve_n':'3'
    }
    
    And because the "provisioner_vars" key is not even present, nothing is
    touched from the previous configuration.  If the key was present, the
    ENTIRE set of values would be replaced by the new conf dictionary.
    
    So this is a rule: the ENTIRE tree of key/values in a configuration is
    replaced if the key is present in the top level of the reconfiguration
    message.
    
    The configurations are technically trees, so if it helps, another way of
    putting this rule is: the root node is "engine_conf" and its direct 
    children are replaceable via reconfiguration.  The ENTIRE sub-tree is
    replaced when a child of the root node is replaced.  Nothing else is
    replaceable.
    
    When do the new values take effect?  In this implementation, any unit
    that is already deployed stays deployed.  If new "provisioner_vars" values
    are sent in and the "preserve_n" configuration is the same, nothing will
    change until something fails and it is replaced with a new unit.
    
    If however a new "preserve_n" value is sent in which is higher than the
    previous one, the newest "provisioner_vars" will take effect immediately
    for any of the new units that are launched. 
    
    
    II. Unique Configurations
    -------------------------
    
    There are only two configurations to the engine that we've discussed so far:
    "preserve_n" and "provisioner_vars".
    
    To support unique configurations, a new configuration was added:
    "unique_instances".
    
    The "unique_instances" configuration is a list of unique keys with their
    own bags of attributes.
    
    Here is an example of a configuration to boot two unique instances.
    
    'engine_conf':{
       'preserve_n':'2',
       'provisioner_vars':{'exchange_scope':'XYZ',
                           'epuworker_image_id':'ami-XYZ',
                           'broker_ip_address':'w.x.y.z',
                           'pip_package_repo':'http://ooici.net/packages',
                           'git_lcaarch_repo':'http://github.com/ooici/lcaarch.git',
                           'git_lcaarch_branch':'develop',
                           'git_lcaarch_commit':'HEAD'},
       'unique_instances':{'b2db408e':{'somekey':'some_value'},
                           '1305a3bf':{'somekey':'some_other_value'}}
    }
    

    The "unique_instances" configuration is a list of unique identifiers,
    each with its own dict of key/value pairs.  Here, note that the
    "preserve_n" configuration is 2 and there are two unique instances.
    This engine will always keep two deployable units running.
    
    One labelled internally as "b2db408e" that is booted with the following
    key/value list sent into the launch contextualization:
    
        'exchange_scope':'XYZ',
        'epuworker_image_id':'ami-XYZ',
        'broker_ip_address':'w.x.y.z',
        'pip_package_repo':'http://ooici.net/packages',
        'git_lcaarch_repo':'http://github.com/ooici/lcaarch.git',
        'git_lcaarch_branch':'develop',
        'git_lcaarch_commit':'HEAD',
        'somekey':'some_value'
    
    And another labelled internally as "1305a3bf" that is booted with the
    following key/value list sent into the launch contextualization:
    
        'exchange_scope':'XYZ',
        'epuworker_image_id':'ami-XYZ',
        'broker_ip_address':'w.x.y.z',
        'pip_package_repo':'http://ooici.net/packages',
        'git_lcaarch_repo':'http://github.com/ooici/lcaarch.git',
        'git_lcaarch_branch':'develop',
        'git_lcaarch_commit':'HEAD',
        'somekey':'some_other_value'

    The internal labels "b2db408e" and "1305a3bf" correspond to unique,
    deployed units (with their own identification).  Let's say for example
    we have:
    
        b2db408e: 7265b438-dbea-4de8-8426-ed52c774367f
        1305a3bf: 5a8b2b75-67f7-433d-ad31-fce5510c976b
        
    The "1305a3bf" unit is being fulfilled with the particular instance
    "5a8b2b75-67f7-433d-ad31-fce5510c976b".  Let's say that instance died
    and needed to be replaced, the new instance will have a new, unique
    ID but it will still be tracked as the 1305a3bf unit:
    
        b2db408e: 7265b438-dbea-4de8-8426-ed52c774367f
        1305a3bf: 2efe02f6-df26-40a8-ba40-1c7c62002c06
    
    When that new unit ("2efe02f6-df26-40a8-ba40-1c7c62002c06") is launched,
    the same key/value list previously sent to the other "1305a3bf unit" will
    be sent.  i.e., this compensating unit will be sent the "some_other_value"
    
    
    III. Corner Cases with Unique Configurations
    --------------------------------------------
    
    In the last example, we had "preserve_n" as 2 and two unique instances. 
    What happens when these don't match up?
    
    1.) If you have more instances in "preserve_n" than there are unique 
    instances configured, the excess instances will just be booted with
    the "provisioner_vars" only.
    
    Example:
    
       'preserve_n':'4',
       'unique_instances':{'b2db408e':{'somekey':'some_value'},
                           '1305a3bf':{'somekey':'some_other_value'}}
    
    In that case you will two instances booted with just the "provisioner_vars",
    one instance booted with "somekey:some_value" and a fourth instance booted
    with "somekey:some_other_value"
    
    2.) If you have less instances in "preserve_n" than there are unique
    instances configured, this will be an error and nothing will be changed.
    
    3.) If you have unique instances that are replaced with new data, this
    will cause terminations.  See the next section.

    
    IV. Reconfiguring Unique Configurations
    ---------------------------------------
    
    Finally, a note about reconfigurations that also include unique 
    configurations.  Recall from Section I that new "provisioner_vars"
    configurations don't actually take effect until a new instance is
    booted.  Quoting:
    
        If however a new "preserve_n" value is sent in which is higher than
        the previous one, the newest "provisioner_vars" will take effect
        immediately for any of the new units that are launched. 
    
    The same principle applies to "provisioner_vars" but not the unique
    instance data.
    
    Recall that when reconfiguring you can only replace an entire subtree.
    For example, you can only replace "unique_instances" as a whole.  If
    you have a situation where you had two keys listed in "unique_instances"
    and you reconfigure things with two different KEYS, the old instances
    will be killed and replaced.  But this does not happen if you only replace
    the data that is supposed to be passed to the unique instances.  Some
    examples should make these rules clear.
    
    The "provisioner_vars" are the same for all examples and abstracted out.
    
    Say you start with the following:
    
       'preserve_n':'2',
       'unique_instances':{'b2db408e':{'somekey':'some_value'},
                           '1305a3bf':{'somekey':'some_other_value'}}
                           
    Two instances are launched and after a while you send a reconfigure message
    with the following:
    
       'unique_instances':{'b2db408e':{'somekey':'some_value'},
                           '1305a3bf':{'somekey':'yet_another_value'}}
    
    Nothing happens right away in that case.  You have not changed the instance
    IDs, just the key/value list for "1305a3bf".  If "1305a3bf" were to fail
    and be replaced, it would be started with the "yet_another_value" instead
    of "some_other_value" like last time.
    
    Then you send the following reconfigure message:
    
       'unique_instances':{'b2db408e':{'somekey':'some_value'},
                           'newkey123':{'somekey':'yet_another_value'}}
                           
    In this case, the instance fulfilling the "1305a3bf" role is terminated
    and one fulfilling the "newkey123" role is started with this new value
    "yet_another_value" in the launch contextualization.
    
    But consider the following reconfigure message sent after that:
    
       'unique_instances':{'b2db408e':{'somekey':'some_value'},
                           'newkey456':{'somekey':'yet_another_value'}}
                           
    Notice that "newkey123" has the same list of key/value configurations as
    "newkey456" did.  So in this case there is already an instance running
    that has been started with the exact thing that this will be started with
    (and hence presumably fulfilling the same "role").  But in this case this
    does not matter.  The configuration values are not examined, only the
    new unique instance key is noticed.  The instance started for "newkey123"
    is now terminated and a new one is started.
    
    That would be a superfluous thing to do since there was already an instance
    running booted with the same configuration, but that is what happens in
    this case.  Those examples should make the rules clear: for the unique
    instances, if the IDs are the same, then the unique conf values are only
    sent when a replacement for that ID is needed.  If the ID keys change, 
    then termination actions will occur for anything missing in the new list
    of unique instances and boots will occur for anything new.
    """

    # -----------------------------------------------------------------------
    # Initial Setup
    # -----------------------------------------------------------------------

    def __init__(self):
        super(NpreservingEngine, self).__init__()
        self.preserve_n = 0
        # todo: get all of this from conf/policy:
        self.available_allocations = ["small"]
        self.available_sites = ["ec2-east"]
        self.available_types = ["epu_work_consumer"]
        
        # dict of unique key with a dict (of key/val) as the value
        self.unique_instances = {}
        
        # dict of unique key with an instance id as the value
        self.unique_iaas_ids = {}
        
        # Those two dicts do not really need to be kept in sync.  The second
        # one (unique_iaas_ids) is used as a lookup: if something disappears
        # from the first because of reconfiguration, the second will just have
        # keys that are never looked up.
        
        
        # Finally, a reconfiguration might result in unique instances being
        # explicitly removed from service.  In this case, the decide call will
        # need to actually make sure these terminations happen, this is kept
        # track of here.
        self.uniques_that_need_to_die = []
        
    def _initdone(self, control, parameters):
        control.configure(parameters)
        log.info("Npreserving engine initialized, preserve_n: %d, unique instances: %s" % (self.preserve_n, self._uniq_report()))
        
    def initialize(self, control, state, conf=None):
        """
        Give the engine a chance to initialize.  The current state of the
        system is given as well as a mechanism for the engine to offer the
        controller input about how often it should be called.
        
        @note Must be invoked and return before the 'decide' method can
        legally be invoked.
        
        @param control instance of Control, used to request changes to system
        @param state instance of State, used to obtain any known information 
        @param conf None or dict of key/value pairs
        @exception Exception if engine cannot reach a sane state
        
        """
        
        # todo: need central constants for these key strings
        parameters = {"timed-pulse-irregular":5000}
        if conf and conf.has_key("force_site"):
            self.available_sites = [conf["force_site"]]
        
        if not conf:
            # This will start at zero, the engine will do nothing until
            # reconfiguration.
            self.preserve_n = 0
            self._initdone(control, parameters)
            return
            
        if conf.has_key(CONF_PRESERVE_N):
            self.preserve_n = int(conf[CONF_PRESERVE_N])
            if self.preserve_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, self.preserve_n))
        else:
            self.preserve_n = 0
            
        if conf.has_key(CONF_UNIQUE):
            self._reconfigure_uniques(conf[CONF_UNIQUE])
        
        self._initdone(control, parameters)


    # -----------------------------------------------------------------------
    # Unique Instances
    # -----------------------------------------------------------------------

    def _uniques_are_present(self):
        """Return True if there are any unique instances configured."""
        return len(self.unique_instances) > 0
        
    def _uniques_count(self):
        """Return the number of unique instances currently configured."""
        return len(self.unique_instances)
        
    def _uniques_legal(self, uniques_conf, new_preserve_n):
        """Do nothing if the configuration is sane.
        
        @param uniques_conf The dictionary that comes by way of the CONF_UNIQUE
        configuration in the engine_conf
        
        @param new_preserve_n The value the configurer wants to set
        
        @exception Exception if the preserve_n target is LESS than number of
        uniques, this is an illegal situation and any configuration or
        reconfiguration attempt should be rejected.
        """
        if len(uniques_conf) > new_preserve_n:
            raise Exception("The attempted configuration has a higher number of unique instances in it than the preserve_n target, this is illegal")
        
    def _reconfigure_uniques(self, uniques_conf):
        """Replaces old unique_instances configuration with the new one.
        
        If any unique instances need to die because they are now gone from
        the list, they are put on the "uniques_that_need_to_die" list.
        
        @param uniques_conf The dictionary that comes by way of the CONF_UNIQUE
        configuration in the engine_conf
        
        @exception Exception if the preserve_n target is LESS than number of
        uniques, this is an illegal situation and any configuration or
        reconfiguration attempt should be rejected.
        """
        self._uniques_legal(uniques_conf, self.preserve_n)
        oldkeys = self.unique_instances.keys()
        
        for key in oldkeys:
            if not uniques_conf.has_key(key):
                self.uniques_that_need_to_die.append(key)
                log.warn("Queued a termination for unique instance '%s' because it was not present in the reconfiguration payload." % key)
                
        self.unique_instances = uniques_conf
        
        log.info("Npreserving engine reconfigured, preserve_n: %d, unique instances: %s" % (self.preserve_n, self._uniq_report()))
        
        
    def _uniques_for_each_n(self):
        """Return True if the preserve_n target matches the number of unique
        instances that are configured.  Return False if there are less unique
        instances than the preserve_n target.
        
        @exception Exception if the preserve_n target is LESS than number of
        uniques, this is an illegal situation and any configuration or
        reconfiguration attempt should be rejected.
        """
        self._uniques_legal(self.unique_instances, self.preserve_n)
        return len(self.unique_instances) == self.preserve_n
        
    def _iaas_id_from_uniq_id(self, uniq_id):
        """Return the IaaS ID for this unique ID.  If there is none
        assigned, return None.
        
        @param uniq_id The unique instance ID (not an IaaS ID).
        """
        if not self.unique_iaas_ids.has_key(uniq_id):
            return None
        return str(self.unique_iaas_ids[uniq_id])


    # -----------------------------------------------------------------------
    # Reconfiguration
    # -----------------------------------------------------------------------

    def reconfigure(self, control, newconf):
        """
        Give the engine a new configuration.
        
        @note There must not be a decide call in progress when this is called,
        and there must not be a new decide call while this is in progress.
        
        @param control instance of Control, used to request changes to system
        @param newconf None or dict of key/value pairs
        @exception Exception if engine cannot reach a sane state
        @exception NotImplementedError if engine does not support this
        
        """
        
        # See the reconfiguration rules in the class comments.
        
        uniques_conf = None
        old_preserve_n = self.preserve_n
        new_preserve_n = old_preserve_n
        
        if newconf.has_key(CONF_UNIQUE):
            uniques_conf = newconf[CONF_UNIQUE]
        
        if newconf.has_key(CONF_PRESERVE_N):
            new_preserve_n = int(newconf[CONF_PRESERVE_N])
            if new_preserve_n < 0:
                raise Exception("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, new_preserve_n))
        
        # Need to have this legality check up front so that no changes take
        # place if something is wrong with the reconfiguration message.
        if uniques_conf != None:
            self._uniques_legal(uniques_conf, new_preserve_n)
        elif new_preserve_n != old_preserve_n:
            # Also need check if there is a new preserve_n target and
            # pre-existing uniques
            if self._uniques_are_present():
                self._uniques_legal(self.unique_instances, new_preserve_n)
        
        # Now enact the changes.
        
        # If the base provisioner vars change, reconfigure the controller
        # itself to send them with every launch.
        pkey = PROVISIONER_VARS_KEY
        if newconf.has_key(pkey):
            controlconf = {}
            controlconf[pkey] = newconf[pkey]
            control.configure(controlconf)
            log.info("Registered new provisioner vars")
        
        if new_preserve_n != old_preserve_n:
            self.preserve_n = new_preserve_n
            log.info("Reconfigured preserve_n: %d (was %d)" % (new_preserve_n, old_preserve_n))
            
        if uniques_conf != None:
            self._reconfigure_uniques(uniques_conf)


    # -----------------------------------------------------------------------
    # Decide (instance launches/terminations)
    # -----------------------------------------------------------------------
    
    def decide(self, control, state):
        """
        Give the engine a chance to act on the current state of the system.
        
        @note May only be invoked once at a time.  
        @note When it is invoked is up to EPU Controller policy and engine
        preferences, see the decision engine implementer's guide.
        
        @param control instance of Control, used to request changes to system
        @param state instance of State, used to obtain any known information 
        @retval None
        @exception Exception if the engine has been irrevocably corrupted
        
        """
        
        # Should only query this once during the decision making.  If you
        # terminate something etc. you want to reflect that in the immediate
        # logic (by changing the valid_count).  If you query get_all, you will
        # not get an accurate reading as it is almost 100% likely that the
        # provisioner has not queried IaaS and sent state update notifications
        # back to the controller in less time than it takes for this method
        # to complete.
        all_instance_lists = state.get_all("instance-state")
        
        # How many instances are not terminated/ing or corrupted?
        valid_count = self._valid_count(all_instance_lists)
        log.debug("valid count: %d" % valid_count)
        
        # First task is always to make sure that any unique instances that
        # need to be terminated are terminated.
        todie = self.uniques_that_need_to_die
        if not todie:
            log.debug("No unique instances need to be terminated")
        while todie:
            die_uniqid = todie.pop()
            die_id = self._iaas_id_from_uniq_id(die_uniqid)
            if not die_id:
                raise Exception("There is no IaaS ID for unique instance '%s' which needs to be terminated" % die_uniqid)
            if die_id:
                self._destroy_one(control, die_id)
                valid_count -= 1
                log.info("Terminated '%s' which was the instance for unique instance '%s'" % (die_id, die_uniqid))
        
        # Go through each unique instance required and make sure its
        # particular VM is either replaced if corrupted or launched if
        # never launched yet.
        for uniq_id in self.unique_instances.keys():
            iaas_id = self._iaas_id_from_uniq_id(uniq_id)
            if iaas_id:
                # If there is already an IaaS ID for this unique instance ID,
                # make sure it is active or starting already.
                iaas_state = self._state_of_iaas_id(iaas_id, all_instance_lists)
                if iaas_state in BAD_STATES:
                    log.warn("The VM '%s' for unique instance '%s' is in a state that needs compensation: %s" % (iaas_id, uniq_id, iaas_state))
                    thiskv = self.unique_instances[uniq_id]
                    instance_id = self._launch_one(control, uniquekv=thiskv)
                    self.unique_iaas_ids[uniq_id] = instance_id
                    log.info("Launched brand new IaaS instance '%s' for the unique instance '%s'" % (instance_id, uniq_id))
                    valid_count += 1
                else:
                    log.debug("IaaS instance '%s' for the unique instance '%s' is in OK state '%s'" % (iaas_id, uniq_id, iaas_state))
            else:
                # If there is no IaaS ID at all, that means nothing has ever
                # been launched for this unique ID
                thiskv = self.unique_instances[uniq_id]
                instance_id = self._launch_one(control, uniquekv=thiskv)
                self.unique_iaas_ids[uniq_id] = instance_id
                log.info("Launched brand new IaaS instance '%s' for the unique instance '%s'" % (instance_id, uniq_id))
                valid_count += 1

        # The rest of this method only applies if there are generic VMs as
        # well.
        uniqnum = self._uniques_count()
        target = self.preserve_n - uniqnum
        valid_count = valid_count - uniqnum
        if valid_count == target:
            log.debug("There are currently the correct number of non-unique instances: %d" % target)
        
        # Generic VMs
        if valid_count < target:
            log.info("Taking generic instance count from %d to %d (and there are %d unique-instances)" % (valid_count, target, uniqnum))
            while valid_count < target:
                self._launch_one(control)
                valid_count += 1
                
        elif valid_count > target:
            log.info("Taking generic instance count from %d to %d (and there are %d unique-instances)" % (valid_count, target, uniqnum))
            while valid_count > target:
                die_id = self._something_to_kill(all_instance_lists)
                if not die_id:
                    # Should be impossible in this situation
                    raise Exception("Cannot find any valid instances to terminate?")
                self._destroy_one(control, die_id)
                valid_count -= 1
                
    def _valid_count(self, all_instance_lists):
        valid_count = 0
        for instance_list in all_instance_lists:
            ok = True
            for state_item in instance_list:
                if state_item.value in BAD_STATES:
                    ok = False
                    break
            if ok:
                valid_count += 1
        return valid_count
            
    def _launch_one(self, control, uniquekv=None):
        """Return instance_id"""
        log.info("Requesting instance")
        
        launch_item = LaunchItem(1, self._allocation(), self._site(), None)
        launch_description = {}
        launch_description["work_consumer"] = launch_item
        
        (launch_id, launch_description) = \
            control.launch(self._deployable_type(), launch_description)
            
        if len(launch_item.instance_ids) != 1:
            raise Exception("Could not retrieve instance ID after launch")
            
        return str(launch_item.instance_ids[0])
    
    def _something_to_kill(self, all_instance_lists):
        """Pick one instance to die.  Filters instances that are not uniques"""
        candidates = self._filter_bad_instances(all_instance_lists)
        candidates = self._filter_unique_instances(candidates)
        if len(candidates) == 0:
            return None
        elif len(candidates) == 1:
            return candidates[0]
        else:
            idx = random.randint(0, len(candidates)-1)
            return candidates[idx]
            
    def _filter_unique_instances(self, candidates):
        if not self._uniques_are_present():
            return candidates
        newcandidates = []
        all_iaas_ids = self.unique_iaas_ids.values()
        for candidate in candidates:
            if not candidate in all_iaas_ids:
                newcandidates.append(candidate)
        return newcandidates
    
    def _filter_bad_instances(self, all_instance_lists):
        """Filter out instances that are in terminating state or 'worse'"""
        
        candidates = []
        for instance_list in all_instance_lists:
            ok = True
            for state_item in instance_list:
                if state_item.value in BAD_STATES:
                    ok = False
                    break
            if ok:
                candidates.append(state_item.key)
        
        log.debug("Found %d instances that could be killed:\n%s" % (len(candidates), candidates))
        
        return candidates
        
    def _state_of_iaas_id(self, iaas_id, all_instance_lists):
        for instance_list in all_instance_lists:
            # Important to get last item, most recent state
            one_state_item = instance_list[-1]
            if one_state_item.key == iaas_id:
                return one_state_item.value
    
    def _destroy_one(self, control, instanceid):
        log.info("Destroying an instance ('%s')" % instanceid)
        instance_list = [instanceid]
        control.destroy_instances(instance_list)
        

    # -----------------------------------------------------------------------
    # Resource Selection
    # -----------------------------------------------------------------------
    
    def _deployable_type(self):
        return self.available_types[0]
        
    def _allocation(self):
        return self.available_allocations[0]
        
    def _site(self):
        return self.available_sites[0]


    # -----------------------------------------------------------------------
    # Pretty Logging
    # -----------------------------------------------------------------------
    
    def _widest_name_len(self):
        """Return length of widest key name in the unique instance dict"""
        lengths = [len(key) for key in self.unique_instances.keys()]
        lengths.sort()
        return lengths[-1]

    def _pad_txt(self, txt, widest):
        if len(txt) >= widest:
            return txt
        difference = widest - len(txt)
        while difference:
            txt += " "
            difference -= 1
        return txt

    def _uniq_report(self):
        """Return multi-line string containing the current unique instances
        information including the configuration for each.  Or "None" if there
        are none."""
        if len(self.unique_instances) == 0:
            return "None"
        widest = self._widest_name_len()
        ret = "\n"
        for inst in self.unique_instances.iteritems():
            ret += self._pad_txt(str(inst[0]), widest)
            ret += " : "
            ret += str(inst[1])
            ret += "\n"
        return ret
