#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/phrase.py
@author Steve Foley
@brief Phrase and action structure definitions to be used with
    instrument agents
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class Phrase(object):
    """
    The basic phrase structure has a list of something that is to be executed.
    Only one type (command, get, set) can be contained in a phrase. These
    should be handled by subclasses of this phrase. The "add()" method should
    be subclassed. Clearing is not necessary as the phrase object can simply be
    discarded.
    @todo Handle timeouts properly
    """
    
    """
    Constants that define the type of entity in a phrase blurb. This allows
    mixing of device and observatory actions in the same phrase. The subclasses
    can keep track of what side of an interface the action applies to.
    """
    device = "Device"
    observatory = "Observatory"
    
    """
    The list/queue of stuff to get/set/execute. Format is a tuple of a type
    tag (either device or observatory...use constants) then the standard
    format of the action.
    """
    __phrase_contents = []
    
    """
    The "Im complete and have been ended" flag
    """
    ended = False
    
    """
    End time associated with the phrase
    """
    end_time = None
    
    def __init__(self, end_time = 0):
        self.end_time = end_time
    
    def length(self):
        """
        Check the length of the current phrase
        @retval the length of the phrase. If it is 0, the phrase is empty. Duh.
        """
        return len(self.__phrase_contents)
            
    def contents(self):
        """
        Get the contents of the phrase, presumably for execution
        @retval A list of the phrase contents
        """
        return self.__phrase_contents
        
    def end(self):
        """
        Set the phrase to be complete. Cannot un-end a phrase.
        """
        self.ended = True
        
    def is_complete(self):
        """
        Return the ended status
        """
        return self.ended
    
    def is_expired(self):
        """
        Checks to see if the phrase has expired beyond its timeout.
        @retval True if expired, False if still active
        @todo Write the logic for time checking
        """
        #get the current time, compare to the end time, return accordingly
        return False
    
    def add(self, action):
        """
        Add an action to the end of the phrase for future application
        @param action An action structure to add to the list
        @return True if action was added, False if not
        """
        assert (isinstance(action, (GetAction, SetAction, ExecuteAction))), "Invalid action type"
        if (len(self.__phrase_contents) == 0):
            self.__phrase_contents.append(action)
            return True
        elif (type(action) is type(self.__phrase_contents[0])):
            self.__phrase_contents.append(action)
            return True
        else:
            return False
        
class GetPhrase(Phrase):
    """
    A Phrase that holds Get actions, may do additional type checking
    """
    def add(self, action):
        """
        Confirm that the phrase only holds GetActions
        """
        assert(isinstance(action, GetAction))
        Phrase.__init__(self, action)

class SetPhrase(Phrase):
    """
    A Phrase that holds Set actions, may do additional type checking
    """
    def add(self, action):
        """
        Confirm that the phrase only holds SetActions
        """
        assert(isinstance(action, SetAction))
        Phrase.__init__(self, action)

class ExecutePhrase(Phrase):
    """
    A Phrase that holds Execute actions, may do additional type checking
    """
    def add(self, action):
        """
        Confirm that the phrase only holds ExecuteActions
        """
        assert(isinstance(action, ExecuteAction))
        Phrase.__init__(self, action)

class Action(object):
    """
    An action base class
    """
    """
    The structure that is the action (list, dict, etc.) formatted to accomplish
    the correct action. (ie ["cmd1", "arg1", "arg2"] for a command)
    """
    struct = None
    
    """
    The destination of the action, device or orbservatory
    """
    destination = None

    def __init__(self, destination, struct):
        self.struct = struct
        assert((destination == Phrase.device)
            or (destination == Phrase.observatory))
        self.destination = destination

class GetAction(Action):
    """
    An action that holds Get requests, may do additional type checking
    """
    def __init__(self, destination, struct):
        """
        Add in some quick sanity checks for this type of action
        """
        assert(isinstance(struct, (tuple, list)))
        assert(len(struct) > 0)
        Action.__init__(self, destination, struct)
    
class SetAction(Action):
    """
    An action that holds Set requests, may do additional type checking
    """
    def __init__(self, destination, struct):
        """
        Add in some quick sanity checks for this type of action
        """
        assert(isinstance(struct, (tuple, dict)))
        assert(len(struct) > 0)
        assert(isinstance(struct.keys()[0], tuple))
        assert(len(struct.keys()[0]) == 2)
        assert(struct.values()[0] != None)
        Action.__init__(self, destination, struct)

class ExecuteAction(Action):
    """
    An action that holds Execute requests, may do additional type checking
    """
    def __init__(self, destination, struct):
        """
        Add in some quick sanity checks for this type of action
        """
        assert(isinstance(struct, (tuple, list)))
        assert(len(struct) > 0)
        Action.__init__(self, destination, struct)
