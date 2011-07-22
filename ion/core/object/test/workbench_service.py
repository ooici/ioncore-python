#!/usr/bin/env python
"""
@file ion/core/object/workbench.py
@author David Stuebe

"""
from ion.core.process.process import ProcessFactory


from ion.core.process.service_process import ServiceProcess


class WorkBenchService(ServiceProcess):
    """
    A test service which has the ops of the workbench
    """
    declare = ServiceProcess.service_declare(name='workbench',
                                             version='0.1.0',
                                             dependencies=[])



    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.

        ServiceProcess.__init__(self, *args, **kwargs)

        self.op_pull = self.workbench.op_pull
        self.op_push = self.workbench.op_push
        self.op_fetch_blobs = self.workbench.op_fetch_blobs
        self.op_checkout = self.workbench.op_checkout

factory = ProcessFactory(WorkBenchService)
