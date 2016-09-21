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

from coast_guard import utils
from coast_guard import clean
from coast_guard import config
from coast_guard import errors
from coast_guard import debug


SUBINT_GLOB = '[0-9]'*4+'-'+'[0-9]'*2+'-'+'[0-9]'*2+'-' + \
                '[0-9]'*2+':'+'[0-9]'*2+':'+'[0-9]'*2 + \
                    '.ar'
SP_GLOB = "pulse_*.ar"


def get_start_from_subint(subint):
    subint = os.path.basename(subint)
    start = datetime.datetime.strptime(subint, "%Y-%m-%d-%H:%M:%S.ar")
    return start


def get_start_from_singlepulse(single):
    arf = utils.ArchiveFile(single)
    return arf.datetime


FILETYPE_SPECIFICS = {'subint': (SUBINT_GLOB, get_start_from_subint), \
                      'single': (SP_GLOB, get_start_from_singlepulse)}


def group_subband_dirs(subdirs, maxspan=None, maxgap=None, \
            tossfrac=None, filetype='subint'):
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
            filetype: Type of files being grouped. Can be 'subint',
                or 'single'. (Default: 'subint')

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
    if tossfrac is None:
        tossfrac = 1-config.cfg.missing_subint_tolerance

    if filetype not in FILETYPE_SPECIFICS:
        raise errors.InputError("File type (%s) is not recognized. " \
                                "Possible values are: '%s'" % \
                            (filetype, "', '".join(FILETYPE_SPECIFICS.keys())))
    else:
        globpat, get_start = FILETYPE_SPECIFICS[filetype]

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
        fns = glob.glob(os.path.join(subdir, globpat))
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

            fns = glob.glob(os.path.join(subdir, globpat))
            noccurs.subtract([os.path.basename(fn) for fn in fns])
            nsubbands -= 1

    # Remove subints that are no longer included in any subbands
    to_del = []
    for fn in noccurs:
        if not noccurs[fn]:
            to_del.append(fn)
    for fn in to_del:
        del noccurs[fn]
    
    # Now combine subints
    lastsubint = datetime.datetime.min
    filestart = datetime.datetime.min
    groups = []
    if nsubbands:
        for subint in sorted(noccurs):
            if noccurs[subint] < nsubbands:
                utils.print_info("Ignoring sub-int (%s). It doesn't apear in all " \
                                "subbands (only %d of %d)" % \
                                (subint, noccurs[subint], nsubbands), 2)
                continue
            start = get_start(os.path.join(subdirs[0], subint))
            if ((start - filestart).total_seconds() > maxspan) or \
                        ((start - lastsubint).total_seconds() > maxgap):
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


def prepare_subints(subdirs, subints, baseoutdir, trimpcnt=6.25, effix=False,
                    backend=None):
    """Prepare subints by
           - Copying them to the temporary working directory
           - De-weighting a percentage from each sub-band edge
           - Converting archive format to PSRFITS

        Inputs:
            subdirs: List of sub-band directories containing 
                sub-ints to combine
            subints: List of subint files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed should appear in each of the
                    subdirs.)
            baseoutdir: Directory containing the sub-directories
                of preprared files.
            trimpcnt: Percentage (ie between 0-100) of subband
                to trim from _each_ edge of the band. 
                (Default: 6.25%)
            effix: Change observation site to eff_psrix to correct 
                for asterix clock offsets. (Default: False)
            backend: Name of the backend. (Default: leave as is)

        Outputs:
            prepsubdirs: The sub-directories containing prepared files.
    """
    devnull = open(os.devnull)
    tmpsubdirs = []
    for subdir in utils.show_progress(subdirs, width=50):
        freqdir = os.path.split(os.path.abspath(subdir))[-1]
        freqdir = os.path.join(baseoutdir, freqdir)
        try:
            os.makedirs(freqdir)
        except OSError:
            # Directory already exists
            pass
        fns = [os.path.join(subdir, fn) for fn in subints]
        preproc = 'convert psrfits'
        if effix:
            preproc += ',edit site=eff_psrix'
        if backend:
            if ("," in backend) or ("=" in backend) or (' ' in backend):
                raise errors.UnrecognizedValueError("Backend value (%s) is "
                                                    "invalid. It cannot "
                                                    "contain ',' or '=' or "
                                                    "' '" % backend)
            preproc += ',edit be:name=%s' % backend
        utils.execute(['paz', '-j', preproc,
                       '-E', '%f' % trimpcnt, '-O', freqdir] + fns,
                      stderr=devnull)
        tmpsubdirs.append(freqdir)
    utils.print_info("Prepared %d subint fragments in %d freq sub-dirs" %
                    (len(subints), len(subdirs)), 3)
    return tmpsubdirs




def combine_subints(subdirs, subints, parfn=None, outdir=None):
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
            parfn: New ephemeris to install when combining subints.
                (Default: Use ephemeris in archive file's header)
            outdir: Directory to output combined file.
                (Default: Current working directory)
        
        Output:
            outfn: The name of the combined file.
    """
    if outdir is None:
        outdir = os.getcwd()
    subints = sorted(subints)
    tmpdir = tempfile.mkdtemp(suffix="_combine", dir=config.tmp_directory)
    devnull = open(os.devnull)
    try:
        cmbsubints = []
        
        # Try to normalise the archive's parfile
        try:
            if parfn is None:
                arfn = os.path.join(subdirs[0], subints[0])
                normparfn = utils.get_norm_parfile(arfn)
            else:
                normparfn = utils.normalise_parfile(parfn)
        except errors.InputError:
            # No parfile present
            parargs = []
        else:
            parargs = ['-E', normparfn]

        utils.print_info("Adding freq sub-bands for each sub-int...", 2)
        for ii, subint in enumerate(utils.show_progress(subints, width=50)):
            to_combine = [os.path.join(path, subint) for path in subdirs]
            outfn = os.path.join(tmpdir, "combined_%s" % subint)
            cmbsubints.append(outfn)
            utils.execute(['psradd', '-q', '-R', '-o', outfn] + parargs +
                          to_combine, stderr=devnull)
        arf = utils.ArchiveFile(os.path.join(tmpdir, "combined_%s" % subints[0]))
        outfn = os.path.join(outdir, "%s_%s_%s_%05d_%dsubints.cmb" %
                             (arf['name'], arf['band'], arf['yyyymmdd'],
                              arf['secs'], len(subints)))
        utils.print_info("Combining %d sub-ints..." % len(cmbsubints), 1)
        utils.execute(['psradd', '-q', '-o', outfn] + cmbsubints,
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
    
    if len(args.subdirs):
        print "Number of input sub-band directories: %d" % len(args.subdirs)
    elif args.group_file is None:
        raise errors.InputError("No sub-band directories to combine and no group file provided!")

    if args.group_file is not None:
        usedirs, subints = read_listing(args.group_file)
        groups = [subints]
    else:
        # Group directories
        usedirs, groups = group_subband_dirs(args.subdirs, \
                    maxspan=args.combine_maxspan, 
                    maxgap=args.combine_maxgap, \
                    filetype=args.filetype)
    
    # Work in a temporary directory
    tmpdir = tempfile.mkdtemp(suffix="_combine",
                              dir=config.tmp_directory)
    # Combine files
    outfns = []
    for subints in groups:
        if not args.no_combine:
            preppeddirs = prepare_subints(usedirs, subints,
                                          baseoutdir=os.path.join(tmpdir, 'data'),
                                          trimpcnt=6.25)
            outfn = combine_subints(preppeddirs, subints,
                                    outdir=os.getcwd())
            outfns.append(outfn)
        if args.write_listing:
            write_listing(usedirs, subints, "list.txt")
    shutil.rmtree(tmpdir)
    if outfns:
        print "Created %d combined files" % len(outfns)
        for outfn in outfns:
            print "    %s" % outfn


if __name__=="__main__":
    parser = utils.DefaultArguments(usage="%(prog)s [OPTIONS] DIRS-TO-COMBINE", \
                        description="Given a list of frequency sub-band " \
                                    "directories containing sub-ints to " \
                                    "combine, group them and create " \
                                    "combined archives.")
    parser.add_argument('subdirs', nargs='*', help="Sub-band directories " \
                            "containing subints to combine.")
    parser.add_argument('-f', '--group-file', dest='group_file', type=str,
                        help="Combine files/directories listed in group file. "
                             "These files can be output by combine.py. "
                             "(Default: Combine directories listed on command line.)")
#    parser.add_argument('-o', '--outname', dest='outfn', type=str, \
#                        help="The output (combined) file's name. " \
#                            "(Default: '%%(name)s_%%(yyyymmdd)s_%%(secs)05d_combined.ar')", \
#                        default="%(name)s_%(yyyymmdd)s_%(secs)05d_combined.ar")
    parser.add_argument('--max-span', dest='combine_maxspan', type=int, \
                        help="Max number of seconds a combined archive can span. " \
                             "(Default: %d s)" % config.cfg.combine_maxspan)
    parser.add_argument('--max-gap', dest='combine_maxgap', type=int, \
                        help="Max gap (in seconds) between archives before starting " \
                             "a new combined archive. (Default %d s)" % \
                                config.cfg.combine_maxgap)
    parser.add_argument('--type', dest='filetype', type=str, \
                        choices=FILETYPE_SPECIFICS.keys(), \
                        help="Type of files being grouped. Can be 'subint',"
                                "or 'single'. (Default: 'subint')", \
                        default='subint')
    parser.add_argument('--write-listing', dest='write_listing', action='store_true', 
                        help="Write text file containing listing of files to combine.")
    parser.add_argument('--no-combine', dest='no_combine', action='store_true',
                        help="Don't actually combine files.")
    args = parser.parse_args()
    main()
