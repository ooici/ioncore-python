#!/usr/bin/env python

"""
@file ion/services/dm/presentation/strip_chart_consumer.py
@author David Stuebe
@brief twitter the notification component of the data!
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

import logging
logging = logging.getLogger(__name__)


import gobject
import matplotlib
matplotlib.use('GTKAgg')
import numpy as np
from matplotlib.lines import Line2D
from pylab import figure, show


class Scope:
    def __init__(self, ax, maxt=10, dt=1.0):
        self.ax = ax
        self.canvas = ax.figure.canvas
        self.dt = dt
        self.maxt = maxt
        self.tdata = [0]
        self.ydata = [0]
        self.line = Line2D(self.tdata, self.ydata, animated=True)
        self.ax.add_line(self.line)
        self.background = None
        self.canvas.mpl_connect('draw_event', self.update_background)
        self.ax.set_ylim(-.1, 1.1)
        self.ax.set_xlim(0, self.maxt)

    def update_background(self, event):
        self.background = self.canvas.copy_from_bbox(self.ax.bbox)

    def emitter(self, p=0.01):
        'return a random value with probability p, else 0'
        v = np.random.rand(1)
        if v>p: return 0.
        else: return np.random.rand(1)

    def update(self, data, notification):
        
        if self.background is None: return True
        #y = self.emitter()
        lastt = self.tdata[-1]
        if lastt>self.tdata[0]+self.maxt: # reset the arrays
            self.tdata = [self.tdata[-1]]
            self.ydata = [self.ydata[-1]]
            self.ax.set_xlim(self.tdata[0], self.tdata[0]+self.maxt)
            self.ax.figure.canvas.draw()

        self.canvas.restore_region(self.background)

        t = self.tdata[-1] + self.dt
        self.tdata.append(t)
        self.ydata.append(data)
        self.line.set_data(self.tdata, self.ydata)
        self.ax.draw_artist(self.line)

        self.canvas.blit(self.ax.bbox)
        return True



class StripChartConsumer(base_consumer.BaseConsumer):
    """
    Visualization Consumer for simple data
    """

    def ondata(self, data, notification, timestamp):
        """
        """
        logging.info('Plotting data...')
        
        # Bogus way to create the figure the first time data arrives
        if not hasattr(self, 'scope'):
            fig = figure()
            ax = fig.add_subplot(111)
            self.scope = Scope(ax)
        
        self.scope.update(data['val'],notification)
        #gobject.idle_add(scope.update, data, notification)
        
        show()
        
        logging.info('Plot updated.')
        

# Spawn of the process using the module name
factory = ProtocolFactory(StripChartConsumer)
