#!/usr/bin/env python

"""
@file ion/util/context.py
@author Adam R. Smith
@brief Context storage utilities, aka thread-local.
"""

import sys
import weakref

class StackLocal(object):
    '''
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
            frames = self.attr_frames[key]

            frame = sys._getframe()
            while frame:
                if frame in self.frame_attrs:
                    break
                frame = frame.f_back

        if frame is None:
            self.attr_frames[key] = frame = sys._getframe(1)

        if frame in self.frame_attrs:
            attrs = self.frame_attrs[frame]
        else:
            self.frame_attrs[frame] = attrs = {}

        attrs[key] = val
        return val

    def __get(self, key):
        if not key in self.attr_frames:
            return None

        attr_frame = self.attr_frames[key]

        # Ensure the definition frame for this attribute is a parent of the current frame
        frame = sys._getframe(1)
        while frame:
            if frame is attr_frame:
                attrs = self.frame_attrs[frame]
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

if __name__ == '__main__':
    context = StackLocal()

    def level_3():
        msg = context.msg
        #try:
        #    foo = context.foo
        #except AttributeError, ex:
        #    foo = None
        foo = context.get('foo', None)

        pass

    def level_2():
        return level_3()

    def level_1():
        return level_2()

    def fake_request():
        context.msg = 'foo'

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
    