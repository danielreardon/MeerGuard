"""
This file contains custom errors and warnings 
for the CoastGuard timing pipeline.

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


class CleanError(CoastGuardError):
    pass


class ConfigurationError(CoastGuardError):
    pass


class BadPulsarNameError(CoastGuardError):
    pass


class HeaderCorrectionError(CoastGuardError):
    pass


class DiagnosticError(CoastGuardError):
    pass

# Custom Warnings
class CoastGuardWarning(Warning):
    def __str__(self):
        return colour.cstring(super(CoastGuardWarning, self).__str__(), 'warning')
