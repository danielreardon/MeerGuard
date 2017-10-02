#!/usr/bin/env python

import sys
import os.path

import utils

if "-h" in sys.argv or "--help" in sys.argv or len(sys.argv) < 3:
    sys.stderr.write("Usage: %s OUTNAME INFILE\n" % \
                        os.path.split(sys.argv[0])[-1])
    sys.exit(1)

arf = utils.ArchiveFile(sys.argv[2])
print utils.get_outfn(sys.argv[1], arf)
