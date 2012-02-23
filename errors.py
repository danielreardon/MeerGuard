"""
This file contains custom errors for the CoastGuard timing pipeline.

Patrick Lazarus, Nov. 10, 2011
"""

import colour

class CoastGuardError(Exception):
    def __init__(self, msg):
        super(CoastGuardError, self).__init__(colour.cstring(msg, 'error'))


class SystemCallError(CoastGuardError):
    pass


class NoStandardProfileError(CoastGuardError):
    pass


class ToaError(CoastGuardError):
    pass
