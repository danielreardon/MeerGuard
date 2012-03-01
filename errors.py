"""
This file contains custom errors for the CoastGuard timing pipeline.

Patrick Lazarus, Nov. 10, 2011
"""

import colour

class CoastGuardError(Exception):
    def __str__(self):
       return colour.cstring(super(CoastGuardError, self).__str__(), 'error')


class SystemCallError(CoastGuardError):
    pass


class NoStandardProfileError(CoastGuardError):
    pass


class ToaError(CoastGuardError):
    pass


class DataReductionFailed(CoastGuardError):
    pass


class BadFile(CoastGuardError):
    pass
