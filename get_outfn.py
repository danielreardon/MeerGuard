#!/usr/bin/env python

import sys

import utils

arf = utils.ArchiveFile(sys.argv[2])
print utils.get_outfn(sys.argv[1], arf)
