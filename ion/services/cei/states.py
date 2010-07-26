#!/usr/bin/env python

"""Instance states
"""

REQUESTING = '100-REQUESTING'
"""Request has been made but not acknowledged through SA"""

REQUESTED = '200-REQUESTED'
"""Request has been acknowledged by provisioner"""

ERROR_RETRYING = '300-ERROR_RETRYING'
"""Request encountered an error but is still being attempted"""

PENDING = '400-PENDING'
"""Request is pending in IaaS layer"""

STARTED = '500-STARTED'
"""Instance has been started in IaaS layer"""

RUNNING = '600-RUNNING'
"""Instance has been contextualized and is operational"""

TERMINATING = '700-TERMINATING'
"""Termination of the instance has been requested"""

TERMINATED = '800-TERMINATED'
"""Instance has been terminated in IaaS layer"""

FAILED = '900-FAILED'
"""Instance has failed and will not be retried"""
