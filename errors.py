"""
This file contains custom errors for the CoastGuard timing pipeline.

Patrick Lazarus, Nov. 10, 2011
"""

class CoastGuardError(Exception):
    pass


class SystemCallError(CoastGuardError):
    pass
