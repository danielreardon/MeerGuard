#!/usr/bin/env python

"""
Given a list of PSRCHIVE archives combine them.

Patrick Lazarus, Nov. 10, 2011
"""
import glob 
import os
import sys
import tempfile
import warnings
import os.path
import glob
import collections
import datetime
import shutil

import numpy as np

import utils
import clean
import config
import errors
import debug


SUBINT_GLOB = '[0-9]'*4+'-'+'[0-9]'*2+'-'+'[0-9]'*2+'-' + \
                '[0-9]'*2+':'+'[0-9]'*2+':'+'[0-9]'*2 + \
                    '.ar'


def group_subband_dirs(subdirs, maxspan=None, maxgap=None, \
            tossfrac=0.7):
    """Based on file names group sub-ints from different
        sub-bands. Each subband is assumed to be in a separate
        directory.

        Inputs:
            subdirs: List of sub-band directories
            maxspan: Maximum span, in seconds, between first and 
                last sub-int in a combined file.
            maxgap: Maximum gap, in seconds, permitted before 
                starting a new output file.
            tossfrac: Fraction of sub-ints required for a 
                sub-band to be combined. If a sub-band has
                fewer than tossfrac*N_subint sub-ints it
                will be excluded.

        Outputs:
            usedirs: List of directories to use when combining.
                (NOTE: This may be different than the input
                    'subdirs' because some directories may have
                    too few subints to be worth combining. This
                    depends on the input value of 'tossfrac'.)
            groups: List of groups of files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed appears in each of 'usedirs'.)
    """
    if maxspan is None:
        maxspan = config.cfg.combine_maxspan
    if maxgap is None:
        maxgap = config.cfg.combine_maxgap

    # Ensure paths are absolute
    subdirs = [os.path.abspath(path) for path in subdirs]
    utils.print_debug("Grouping subints from %d sub-band directories" % \
                        len(subdirs), 'combine')

    nindirs = len(subdirs)
    nsubbands = len(subdirs)
    nperdir = collections.Counter()
    noccurs = collections.Counter()
    nintotal = 0
    for subdir in subdirs:
        fns = glob.glob(os.path.join(subdir, SUBINT_GLOB))
        nn = len(fns)
        utils.print_debug("Found %d sub-int files in %s" % \
                            (nn, subdir), 'combine')
        nintotal += nn
        nperdir[subdir] = nn
        noccurs.update([os.path.basename(fn) for fn in fns])
    nsubints = len(noccurs)

    # Remove sub-bands that have too few subints
    thresh = tossfrac*nsubints
    for ii in xrange(len(subdirs)-1, -1, -1):
        subdir = subdirs[ii]
        if nperdir[subdir] < thresh:
            utils.print_info("Ignoring sub-ints from %s. " \
                    "It has too few sub-ints (%d < %d; tossfrac: %f)" % \
                    (subdir, nperdir[subdir], thresh, tossfrac), 2)
            subdirs.pop(ii)
            del nperdir[subdir]

            fns = glob.glob(os.path.join(subdir, SUBINT_GLOB))
            noccurs.subtract([os.path.basename(fn) for fn in fns])
            nsubbands -= 1

    # Now combine subints
    lastsubint = datetime.datetime.min
    filestart = datetime.datetime.min
    groups = []
    for subint in sorted(noccurs):
        if noccurs[subint] < nsubbands:
            continue
        start = datetime.datetime.strptime(subint, "%Y-%m-%d-%H:%M:%S.ar")
        if (start - filestart).seconds > maxspan or \
                    (start - lastsubint).seconds > maxgap:
            filestart = start
            utils.print_debug("Starting a new file at %s" % \
                    filestart, 'combine')
            # Start a new file
            groups.append([])
        groups[-1].append(subint)
        lastsubint = start
    nused = sum([len(grp) for grp in groups])
    utils.print_info("Grouped %d files from %d directories into %d groups.\n" \
                     "(Threw out %d directories and %d files)" % \
                     (nintotal, nindirs, len(groups), nindirs-len(subdirs), \
                        nintotal-nused), 2)
    return subdirs, groups


def write_listing(subdirs, subints, outfn):
    """Write a text file containing a listing of subints
        that should be combined.

        Inputs:
            subdirs: List of sub-band directories containing 
                sub-ints to combine
            subints: List of subint files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed should appear in each of the
                    subdirs.)
            outfn: The name of the file to write the listing to.

        Outputs:
            None
    """
    # Ensure paths are absolute
    subdirs = [os.path.abspath(path) for path in subdirs]

    if os.path.exists(outfn):
        raise errors.InputError("A file already exists with the requested " \
                        "output file name (%s)!" % outfn)
    outfile = open(outfn, 'w')
    outfile.write("# Listing of sub-int files to combine\n" + \
                  "# Each file name listed below should appear " + \
                        "in each of the following directories.\n" + \
                  "# Each directory contains data from a different " + \
                        "frequency sub-band.\n")
    outfile.write("===== Frequency sub-band directories =====\n")
    for subdir in sorted(subdirs):
        outfile.write(subdir+"\n")
    outfile.write("========== Sub-integration files =========\n")
    for subint in sorted(subints):
        outfile.write(subint+"\n")
    outfile.close()


def read_listing(infn):
    """Read a text file containing a listing of sub-ints
        that should be combined, as was written by 
        'write_listing'.

        Inputs:
            infn: The name of the file containing the listing to read.

        Outputs:
            subdirs: List of sub-band directories containing 
                sub-ints to combine
            subints: List of subint files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed should appear in each of the
                    subdirs.)
    """
    subdirs = []
    subints = []
    collector = None
    infile = open(infn, 'r')
    for line in infile:
        # Strip out comments
        line = line.partition('#')[0].strip()
        # Skip empty lines
        if not line:
            continue
        if "Frequency sub-band directories" in line:
            collector = subdirs
        elif "Sub-integration files" in line:
            collector = subints
        elif collector is None:
            raise errors.FormatError("Non-comment line preceeds directory " \
                                    "section of file listing!")
        else:
            collector.append(line)
    infile.close()
    return subdirs, subints


def combine_subints(subdirs, subints, outdir=None):
    """Combine sub-ints from various freq sub-band directories.
        The input lists are as created by
        'group_subband_dirs' or read-in by 'read_listing'.

        Inputs:
            subdirs: List of sub-band directories containing 
                sub-ints to combine
            subints: List of subint files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed should appear in each of the
                    subdirs.)
            outdir: Directory to output combined file.
                (Default: Current working directory)
        
        Output:
            outfn: The name of the combined file.
    """
    if outdir is None:
        outdir = os.getcwd()
    subints = sorted(subints)
    tmpdir = tempfile.mkdtemp(suffix="_combine", \
                                    dir=config.tmp_directory)
    devnull = open(os.devnull)
    try:
        cmbsubints = []
        parfn = utils.get_norm_parfile(os.path.join(subdirs[0], subints[0]))
        utils.print_info("Adding freq sub-bands for each sub-int...", 2)
        for ii, subint in enumerate(utils.show_progress(subints, width=50)):
            to_combine = [os.path.join(path, subint) for path in subdirs]
            outfn = os.path.join(tmpdir, "combined_%s" % subint)
            cmbsubints.append(outfn)
            utils.execute(['psradd', '-q', '-R', '-E', parfn, '-o', outfn] + \
                        to_combine, stderr=devnull)
        outfn = os.path.join(outdir, "combined_%dsubints_%s" % \
                        (len(subints), subints[0]))
        utils.print_info("Combining %d sub-ints..." % len(cmbsubints), 1)
        utils.execute(['psradd', '-q', '-o', outfn] + cmbsubints, \
                     stderr=devnull)
    except:
        raise # Re-raise the exception
    finally:
        if debug.is_on('reduce'):
            warnings.warn("Not cleaning up temporary directory (%s)" % tmpdir, \
                        errors.CoastGuardWarning)
        else:
            utils.print_info("Removing temporary directory (%s)" % tmpdir, 2)
            shutil.rmtree(tmpdir)
    return outfn


def combine_all(infns, outfn, expected_nsubbands=None):
    """Given a list of ArchiveFile objects group them into sub-bands
        then remove the edges of each sub-band to remove the artifacts
        caused by aliasing. Finally, combine the sub-bands into a single 
        output file.

        The combined sub-band files are not saved.

        Inputs:
            infns: A list of input ArchiveFile objects.
            outfn: The output file's name.
            expected_nsubbands: The expected number of subbands for each 
                subintegration.

        Outputs:
            combinedfns: A list of output (combined) files.
    """
    if expected_nsubbands is None:
        expected_nsubbands = config.cfg.expected_nsubbands

    infns = check_files(infns, expected_nsubbands=expected_nsubbands)
    groups = group_files(infns)
    combinedfiles = []
    # Combine files from the same sub-band in the time direction
    for group in groups:
        subbands = []
        for ctr_freq, to_combine in utils.group_by_ctr_freq(group).iteritems():
            utils.print_info("Combining %d subints at ctr freq %d MHz" % \
                                (len(to_combine), ctr_freq), 3)
 
            # Combine sub-integrations for this sub-band
            subfn = utils.get_outfn(outfn+".%(freq)dMHz", to_combine[0])
            if subfn in [f.fn for f in subbands]:
                warnings.warn("'combined_all(...)' is overwritting files it " \
                                "previously created!")
            subband = combine_subints(to_combine, subfn)
            clean.trim_edge_channels(subband)
            subbands.append(subband)

        combinedfn = utils.get_outfn(outfn, subbands[0])
        utils.print_info("Combining %d subbands into %s" % \
                            (len(subbands), combinedfn), 3)
        if combinedfn in [f.fn for f in combinedfiles]:
            warnings.warn("'combined_all(...)' is overwritting files it " \
                            "previously created!")
        combinedfile = combine_subbands(subbands, combinedfn)
        combinedfiles.append(combinedfile)
    
        if not config.debug.INTERMEDIATE:
            # Remove the temporary combined files
            for sub in subbands:
                os.remove(sub.fn)
    return combinedfiles


def check_files(infns):
    """Check a list of input files to make sure their headers are
        consistent and to make sure subints include all subbands.

        Input:
            infns: A list of input files (ArchiveFile objects).

        Output:
            outfns: A list of complete, consistent files.
    """
    if expected_nsubbands is None:
        expected_nsubbands = config.cfg.expected_nsubbands
    if missing_subint_tolerance is None:
        missing_subint_tolerance = config.cfg.missing_subint_tolerance

    # Ensure all files have the same bandwidth and number of channels,
    # length, and source name
    # Raise errors if inconsistencies are found (i.e. 'warn=False')
    utils.enforce_file_consistency(infns, 'bw', warn=False)
    utils.enforce_file_consistency(infns, 'nchan', warn=False)
    utils.enforce_file_consistency(infns, 'length', warn=False)
    utils.enforce_file_consistency(infns, 'name', warn=False)


def main():
    print ""
    print "        combine.py"
    print "     Patrick  Lazarus"
    print ""
    file_list = args + options.from_glob
    to_exclude = options.excluded_files + options.excluded_by_glob
    to_combine = utils.exclude_files(file_list, to_exclude)
    print "Number of input files: %d" % len(to_combine)
    
    if not to_combine:
        raise errors.BadFile("No files to combine!")

    # Combine files
    raise NotImplementedError


if __name__=="__main__":
    raise NotImplementedError
    parser = utils.DefaultArguments(usage="%prog [OPTIONS] FILES ...", \
                        description="Given a list of frequency sub-band " \
                                    "directories containing sub-ints to " \
                                    "combine, group them and create " \
                                    "combined archives.")
    parser.add_argument('-o', '--outname', dest='outfn', type='string', \
                        help="The output (combined) file's name. " \
                            "(Default: '%(name)s_%(yyyymmdd)s_%(secs)05d_combined.ar')", \
                        default="%(name)s_%(yyyymmdd)s_%(secs)05d_combined.ar")
    parser.add_argument('-g', '--glob', dest='from_glob', action='callback', \
                        callback=utils.get_files_from_glob, default=[], \
                        type='string', \
                        help="Glob expression of input files. Glob expression " \
                            "should be properly quoted to not be expanded by " \
                            "the shell prematurely. (Default: no glob " \
                            "expression is used.)") 
    parser.add_argument('-x', '--exclude-file', dest='excluded_files', \
                        type='string', action='append', default=[], \
                        help="Exclude a single file. Multiple -x/--exclude-file " \
                            "options can be provided. (Default: don't exclude " \
                            "any files.)")
    parser.add_argument('--exclude-glob', dest='excluded_by_glob', action='callback', \
                        callback=utils.get_files_from_glob, default=[], \
                        type='string', \
                        help="Glob expression of files to exclude as input. Glob " \
                            "expression should be properly quoted to not be " \
                            "expanded by the shell prematurely. (Default: " \
                            "exclude any files.)")
    parser.add_argument('--nchan-to-trim', dest='nchan_to_trim', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="The number of channels to trim from the edge of each " \
                            "subband. (Default: %d)" % config.cfg.nchan_to_trim)
    parser.add_argument('--frac-to-trim', dest='frac_to_trim', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="The fraction of channels to trim from the edge of each " \
                            "subband. (Default: %g)" % config.cfg.frac_to_trim)
    parser.add_argument('--max-span', dest='combine_maxspan', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="Max number of seconds a combined archive can span. " \
                             "(Default: %d s)" % config.cfg.combine_maxspan)
    parser.add_argument('--max-gap', dest='combine_maxgap', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="Max gap (in seconds) between archives before starting " \
                             "a new combined archive. (Default %d s)" % \
                                config.cfg.combine_maxgap)
    args = parser.parse_args()
    main()
