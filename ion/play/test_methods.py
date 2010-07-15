#!/usr/bin/env python

def print_me(self):
    print self.ME
        
def print_you(self):
    print self.YOU

class demo(object):
    
    ME = 'demo me'
    YOU = 'demo you'
    print_me = print_me
    print_you = print_you
        

class prototype(object):
    
    ME = 'Proto me'
    YOU = 'Proto you'
    print_me = print_me
    print_you = print_you


class test(prototype):
    
    test_me = test.print_me
    test_you = test.print_you
    