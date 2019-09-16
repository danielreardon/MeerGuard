#!/usr/bin/env python

# For python3 and python2 compatibility
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

# Import CoastGuard
from coast_guard import cleaners
import argparse
import psrchive as ps
import os

# Parse some arguments to set up cleaning
parser = argparse.ArgumentParser(
            description="Run MeerGuard on input archive file")
parser.add_argument("-ar", dest="archivepath", help="Path to the archive file")
parser.add_argument(
    "-temp",
    dest="templatepath",
    help="Path to the template file")
parser.add_argument("-ct", dest="chan_thresh", help="Channel threshold")
parser.add_argument("-st", dest="subint_thresh", help="Subint threshold")
parser.add_argument(
    "-out",
    dest="output_path",
    help="Output path (for custom CG)")
args = parser.parse_args()

archive = str(args.archivepath)
template = str(args.templatepath)
chan_thresh = float(args.chan_thresh)
subint_thresh = float(args.subint_thresh)

# Load an Archive file
loaded_archive = ps.Archive_load(archive)
archive_path, archive_name = os.path.split(loaded_archive.get_filename())
archive_name_orig = archive_name.split('.')[0]
psrname = archive_name_orig.split('_')[0]

# Renaming archive file with statistical thresholds
archive_name = archive_name_orig + \
    '_ch{0}_sub{1}.ar'.format(chan_thresh, subint_thresh)
output_path = str(args.output_path)
print(archive_name)

# Surgical cleaner
print("Applying the surgical cleaner")
surgical_cleaner = cleaners.load_cleaner('surgical')
surgical_parameters = 'chan_numpieces=1,subint_numpieces=1,chanthresh={1},'\
                      'subintthresh={2},template={0}'.format(
                              str(args.templatepath), chan_thresh,
                              subint_thresh)
surgical_cleaner.parse_config_string(surgical_parameters)
surgical_cleaner.run(loaded_archive)

# Bandwagon cleaner
print("Applying the bandwagon cleaner")
bandwagon_cleaner = cleaners.load_cleaner('bandwagon')
bandwagon_parameters = 'badchantol=0.8,badsubtol=0.95'
bandwagon_cleaner.parse_config_string(bandwagon_parameters)
bandwagon_cleaner.run(loaded_archive)

# Unload the Archive file
print("Unloading the cleaned archive")
loaded_archive.unload("{0}/{1}".format(output_path, archive_name))
