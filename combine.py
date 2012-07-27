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

import numpy as np

import utils
import clean
import config
import errors

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


def group_files(infns, maxspan=None, maxgap=None):
    """Given a list of ArchiveFile objects group them.

        Input:
            infns: A list of input ArchiveFiles.
            maxspan: The maximum span (in seconds) or a group.
            maxgap: The maximum gap (in seconds) between subints
                before starting a new group.

        Output:
            groups: A list of lists. Each sub-list is a group
                of ArchiveFiles that should be combined.
    """
    if maxspan is None:
        maxspan = config.cfg.combine_maxspan
    if maxgap is None:
        maxgap = config.cfg.combine_maxgap
    if not infns:
        return []
    utils.print_debug("infns: %s\nTotal: %d files" % (infns, len(infns)), 'combine')
    mjds = np.array([fn['mjd'] for fn in infns])
    mjdind = np.argsort(mjds)

    # Sort infiles and MJDs based on MJD
    infns = [infns[ii] for ii in mjdind]
    mjds = mjds[mjdind]
    secsince = np.round((mjds-mjds[0])*24*3600).astype(int) # Seconds since the first sub-int
    
    # First group files into subints
    subints = {}
    for infn, secs in zip(infns, secsince):
        subint = subints.setdefault(secs, [])
        subint.append(infn)

    start_secs = sorted(subints.keys())
    subint0 = subints[start_secs[0]]
    groups = [subint0]
    last_subint_end = start_secs[0]+subint0[0]['length']
    span = subint0[0]['length']

    for secs in start_secs[1:]:
        gap = secs - last_subint_end
        subint = subints[secs]
        if gap >= maxgap:
            groups.append(subint)
            utils.print_info("Starting new subint (gap=%g >= %g)." % (gap, maxgap), 2)
            span = subint[0]['length']
        elif span >= maxspan:
            groups.append(subint)
            utils.print_info("Starting new subint (span=%g >= %g)." % (span, maxspan), 2)
            span = subint[0]['length']
        else:
            groups[-1].extend(subint)
            span += subint[0]['length']
        last_subint_end = secs + subint[0]['length']
    return groups


def combine_subints(infns, outfn):
    """Given a list of PSRCHIVE file names group them together using
        'psradd' assuming they are all sub-integrations from the same
        observing band.

        Inputs:
            infns: A list of intput sub-integration PSRCHIVE archive file names.
            outfn: The output file name to use.

        Output:
            outar: An output ArchiveFile object.
    """
    utils.execute("psradd -o %s %s" % (outfn, " ".join([f.fn for f in infns])))
    return utils.ArchiveFile(outfn)


def combine_subbands(infns, outfn):
    """Given a list of PSRCHIVE file names group them together using
        'psradd' assuming they are all sub-bands from the same
        observation.

        Inputs:
            infns: A list of intput sub-bands PSRCHIVE archive file names.
            outfn: The output file's name

        Outputs:
            outar: An output ArchiveFile object.
    """
    utils.execute("psradd -R -o %s %s" % (outfn, " ".join([f.fn for f in infns])))
    return utils.ArchiveFile(outfn)


def check_files(infns, expected_nsubbands=None, missing_subint_tolerance=None):
    """Check a list of input files to make sure their headers are
        consistent and to make sure subints include all subbands.

        Input:
            infns: A list of input files (ArchiveFile objects).
            expected_nsubbands: The expected number of subbands to check for.
            missing_subint_tolerance: The tolerance for missing subints before
                removing an entire subband.

        Output:
            outfns: A list of complete, consistent files.
    """
    if expected_nsubbands is None:
        expected_nsubbands = config.cfg.expected_nsubbands
    if missing_subint_tolerance is None:
        missing_subint_tolerance = config.cfg.missing_subint_tolerance

    # Ensure all files have the same bandwidth and number of channels
    # discard any sub-ints that are outliers
    infns = utils.enforce_file_consistency(infns, 'bw', discard=True)
    infns = utils.enforce_file_consistency(infns, 'nchan', discard=True)
    infns = utils.enforce_file_consistency(infns, 'length', discard=True)

    subints = {}
    subbands = {}
    for infn in infns:
        subint = subints.setdefault((infn['yyyymmdd'], infn['secs']), [])
        subint.append(infn)
        subband = subbands.setdefault(infn['freq'], [])
        subband.append(infn)

    numsubints = len(subints.keys())
    freqs_to_purge = []
    for freq, subband in subbands.iteritems():
        fracmissing = 1-len(subband)/float(numsubints)
        if fracmissing > missing_subint_tolerance:
            utils.print_debug("Frequency subband (%g MHz) is missing to many " \
                                "subints to be tolerated (%g > %g). It is " \
                                "being removed." % (freq, fracmissing, \
                                missing_subint_tolerance), 'grouping')
            freqs_to_purge.append(freq)
            expected_nsubbands -= 1
    
    if freqs_to_purge:
        utils.print_info("Purging incomplete subbands (%s)" % \
                            ", ".join(["%g MHz" % f for f in freqs_to_purge]), 2)
        for subint in subints.itervalues():
            for ii in reversed(range(len(subint))):
                fn = subint[ii]
                if fn['freq'] in freqs_to_purge:
                    subint.pop(ii)
                        
    outfns = []
    for key in sorted(subints.keys()):
        if len(subints[key]) == expected_nsubbands:
            outfns.extend(subints[key])
        else:
            date, secs = key
            utils.print_debug("Not correct number of subbands starting at " \
                              " %s %d (%d != %d)" % \
                    (date, secs, len(subints[key]), expected_nsubbands), 'grouping')
    return outfns


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

    to_combine = [utils.ArchiveFile(fn) for fn in to_combine]
     
    # Read configurations
    config.cfg.load_configs_for_archive(to_combine[0])
  
    # Combine files
    outfns = combine_all(to_combine, options.outfn)

    print "Output file names (%d files):" % len(outfns)
    for fn in sorted([outfn.fn for outfn in outfns]):
        print "    %s" % fn


if __name__=="__main__":
    parser = utils.DefaultOptions(usage="%prog [OPTIONS] FILES ...", \
                        description="Given a list of PSRCHIVE file names " \
                                    "group them into sub-bands then combine " \
                                    "the sub-bands into a single output file.")
    parser.add_option('-o', '--outname', dest='outfn', type='string', \
                        help="The output (combined) file's name. " \
                            "(Default: '%(name)s_%(yyyymmdd)s_%(secs)05d_combined.ar')", \
                        default="%(name)s_%(yyyymmdd)s_%(secs)05d_combined.ar")
    parser.add_option('-g', '--glob', dest='from_glob', action='callback', \
                        callback=utils.get_files_from_glob, default=[], \
                        type='string', \
                        help="Glob expression of input files. Glob expression " \
                            "should be properly quoted to not be expanded by " \
                            "the shell prematurely. (Default: no glob " \
                            "expression is used.)") 
    parser.add_option('-x', '--exclude-file', dest='excluded_files', \
                        type='string', action='append', default=[], \
                        help="Exclude a single file. Multiple -x/--exclude-file " \
                            "options can be provided. (Default: don't exclude " \
                            "any files.)")
    parser.add_option('--exclude-glob', dest='excluded_by_glob', action='callback', \
                        callback=utils.get_files_from_glob, default=[], \
                        type='string', \
                        help="Glob expression of files to exclude as input. Glob " \
                            "expression should be properly quoted to not be " \
                            "expanded by the shell prematurely. (Default: " \
                            "exclude any files.)")
    parser.add_option('--expected-nsubbands', dest='expected_nsubbands', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="The expected number of subband files for each subint. " \
                            "(Default: %d)" % config.cfg.expected_nsubbands)
    parser.add_option('--missing-subint-tolerance', dest='missing_subint_tolerance', \
                        action='callback', callback=parser.override_config, type='float', \
                        help="The fractional number of subint files that can be " \
                            "missing from a subband before removing the entire " \
                            "subband (Default: %g)" % config.cfg.missing_subint_tolerance)
    parser.add_option('--nchan-to-trim', dest='nchan_to_trim', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="The number of channels to trim from the edge of each " \
                            "subband. (Default: %d)" % config.cfg.nchan_to_trim)
    parser.add_option('--frac-to-trim', dest='frac_to_trim', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="The fraction of channels to trim from the edge of each " \
                            "subband. (Default: %g)" % config.cfg.frac_to_trim)
    parser.add_option('--max-span', dest='combine_maxspan', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="Max number of seconds a combined archive can span. " \
                             "(Default: %d s)" % config.cfg.combine_maxspan)
    parser.add_option('--max-gap', dest='combine_maxgap', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="Max gap (in seconds) between archives before starting " \
                             "a new combined archive. (Default %d s)" % \
                                config.cfg.combine_maxgap)
    options, args = parser.parse_args()
    main()
