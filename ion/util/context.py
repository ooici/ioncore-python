#!/usr/bin/env python

"""
@file ion/util/context.py
@author Adam R. Smith
@brief Context storage utilities, aka thread-local.
"""

import sys
import weakref
import threading

class temp(object):
    def __init__(self, f):
        self.f = f

class StackLocal(object):
    '''
    NOTE: DO NOT USE StackLocal right now. It is not safe to use with Twisted.

    StackLocal provides an interface matching threading.local as close as possible for situations where thread/greenlet context is not possible.
    The intended usage is to define a global (package-level) variable to serve as a container for data that is local to a particular context.
    Once you have created a instance of StackLocal, you can assign new attributes to it:
        context = StackLocal()
        context.foo = 'bar'
    Take care when using it to be aware of where you are in the call stack when you store data in a StackLocal instance.
    When you first assign an attribute with a particular name, the current stack frame becomes the root context for that attribute.
    If you think of the call chains as a tree, this attribute is now shared by all nodes (stack frames) that are child nodes of the root frame where it was defined.
    When child nodes (function calls/stack frames) modify the value of the attribute, a new root is not created: all other children of that root will see the change.
    This emulates thread-local storage reasonably well for situations where you cannot use threading.local.

    Note that StackLocal currently leaks memory slowly. It is intended to be used as a placeholder until proper
    thread-local storage can be swapped in.
    '''

    frame_attrs = None
    attr_frames = None

    def __init__(self):
        #object.__setattr__(self, 'frame_attrs', weakref.WeakKeyDictionary())
        #object.__setattr__(self, 'attr_frames', weakref.WeakValueDictionary())

        object.__setattr__(self, 'frame_attrs', {})
        object.__setattr__(self, 'attr_frames', {})


    def __setattr__(self, key, val):
        '''
        Set an attribute to exist for the life of this stack frame and all its children.
        The first time you set an attribute, the current stack frame becomes its root.
        You can safely change the value in child frames and have the change reflected in other calls
        that originate from the same root.
        '''

        frame = None
        if key in self.attr_frames: # Try to find the root frame for this attribute
            attr_frame = self.attr_frames[key]

            frame = sys._getframe(1)
            while frame:
                if repr(frame) == attr_frame:
                    break
                frame = frame.f_back

        if frame is None:
            frame = sys._getframe(1)
            frameid = repr(frame)
            self.attr_frames[key] = frameid

        frameid = repr(frame)

        if frameid in self.frame_attrs:
            attrs = self.frame_attrs[frameid]
        else:
            self.frame_attrs[frameid] = attrs = {}

        attrs[key] = val
        return val

    def __get(self, key):
        if not key in self.attr_frames:
            return None

        attr_frame = self.attr_frames[key]

        # Ensure the definition frame for this attribute is a parent of the current frame
        frame = sys._getframe(1)
        while frame:
            frameid = repr(frame)
            if frameid == attr_frame:
                attrs = self.frame_attrs[frameid]
                if not key in attrs:
                    return None

                return attrs[key]

            frame = frame.f_back

        return None

    def __getattr__(self, key):
        ''' Get an attribute that was defined by any parent stack frame of the current. '''
        val = self.__get(key)
        if val is None:
            raise AttributeError('There is no attribute named "%s" in the current stack.')
        return val

    def get(self, key, defaultVal=None):
        val = self.__get(key)
        if val is None:
            return defaultVal
        return val


class ContextLocal(threading.local):
    """
    Extend threading.local to have a dict-style 'get' method.
    Eventually this class could be a generic context-local class that works in threads, deferreds, greenlets, etc.
    """

    def get(self, key, defaultVal=None):
        try:
            return self.__getattribute__(key)
        except AttributeError:
            return defaultVal

    def clear(self):
        self.__dict__.clear()


class ContextObject(object):


    def __init__(self, convid="Default Context"):
        self.progenitor_convid = convid

    def get(self, key, defaultVal=None):
        try:
            return self.__getattribute__(key)
        except AttributeError:
            return defaultVal


    def clear(self, convid="Default Context"):
        self.__dict__.clear()
        self.progenitor_convid = convid
        
    def __str__(self):
        return str(self.__dict__.items())



class ConversationContext(object):


    def __init__(self):
        self.d = {}

    def create_context(self,name):
        context = ContextObject(name)
        self.d[name] = context
        return context


    def get_context(self,name):
        context = self.d.get(name,None)
        if context is None:
            raise KeyError('Conversation Context not found!')
        return context


    def reference_context(self, name, context):
        self.d[name] = context
        return None

    def remove(self,name):
        del self.d[name]

    def __str__(self):
        return str(self.d.items())

    def clear(self):

        self.d.clear()

    def replace_context(self):
        """
        If a context completes, we don't really know what to set it back to afterward... so just pick one. Hopefully it doesn't matter....
        """
        try:
            return self.d.values()[0]
        except IndexError, ie:
            return None


"""
if __name__ == '__main__':
    context = StackLocal()
    frame = sys._getframe()
    t = temp(frame)
    ref = weakref.ref(t)

    def level_3():
        msg = context.msg
        foo = context.get('foo', None)

        pass

    def level_2():
        return level_3()

    def level_1():
        return level_2()

    def fake_request():
        context.msg = 'foo'
        #context.foo = None

        def request_context_1():
            # Should get 'foo'
            level_1()

        def request_context_2():
            # Should get 'foo2'
            context.msg = 'foo2'
            level_1()

        def request_context_3():
            # Should get 'foo3'
            context.msg = 'foo3'
            level_1()

        def request_context_4():
            # Should get 'foo3'
            level_1()

        def request_context_5():
            # Should get 'bar5'
            context.foo = 'bar5'
            level_1()

        def request_context_6():
            # Should get 'None'
            level_1()

        request_context_1()
        request_context_2()
        request_context_3()
        request_context_4()
        request_context_5()
        request_context_6()

    fake_request() 

"""
