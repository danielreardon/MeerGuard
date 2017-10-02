#!/usr/bin/env python
import sys

import utils
import toas

arf = utils.ArchiveFile(sys.argv[1])
print toas.get_standard(arf, analytic=False)
