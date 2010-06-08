#!/usr/bin/env python

"""
@file ion/interact/scribble.py
@author Michael Meisinger
@brief Working with scribble as a conversation type spec language
"""

import logging
logging = logging.getLogger(__name__)

from ion.interact.conversation import ConversationTypeSpec

class ScribbleSpec(ConversationTypeSpec):
    """Scribble conversation type specification
    """
    
    def parse(self):
        pass
    
    