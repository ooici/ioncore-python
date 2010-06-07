#!/usr/bin/env python

"""Instance states
"""

Requesting = '100-Requesting'
"""Request has been made but not acknowledged through SA"""

Requested = '200-Requested'
"""Request has been acknowledged by provisioner"""

ErrorRetrying = '300-ErrorRetrying'
"""Request encountered an error but is still being attempted"""

Pending = '400-Pending'
"""Request is pending in IaaS layer"""

Started = '500-Started'
"""Instance has been started in IaaS layer"""

Running = '600-Running'
"""Instance has been contextualized and is operational"""

Terminating = '700-Terminating'
"""Termination of the instance has been requested"""

Terminated = '800-Terminated'
"""Instance has been terminated in IaaS layer"""

Failed = '900-Failed'
"""Instance has failed and will not be retried"""
