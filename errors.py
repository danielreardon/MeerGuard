"""
This file contains custom errors and warnings 
for the CoastGuard timing pipeline.

Patrick Lazarus, Nov. 10, 2011
"""

import colour
import log

class CoastGuardError(Exception):
    def __init__(self, msg):
        log.log(msg, 'error')
        super(CoastGuardError, self).__init__(msg)

    def __str__(self):
        return colour.cstring(super(CoastGuardError, self).__str__(), 'error')


class SystemCallError(CoastGuardError):
    pass


class StandardProfileError(CoastGuardError):
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


class InputError(CoastGuardError):
    pass


class FitError(CoastGuardError):
    pass


class FormatError(CoastGuardError):
    pass


class DatabaseError(CoastGuardError):
    pass


# Fatal class of errors. These should not be caught.
class FatalCoastGuardError(Exception):
    def __init__(self, msg):
        log.log(msg, 'critical')
        super(FatalCoastGuardError, self).__init__(msg)

    def __str__(self):
        return colour.cstring(super(FatalCoastGuardError, self).__str__(), 'error')


class BadColumnNameError(FatalCoastGuardError):
    pass

# Custom Warnings
class CoastGuardWarning(Warning):
    def __str__(self):
        return colour.cstring(super(CoastGuardWarning, self).__str__(), 'warning')


class LoggedCoastGuardWarning(CoastGuardWarning)
    def __init__(self, msg):
        log.log(msg, 'warning')
        super(CoastGuardWarning, self).__init__(msg)

