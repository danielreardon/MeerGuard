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

def combine_all(infns, outfn, maxspan=1890, maxgap=300, num_to_trim=0):
    """Given a list of ArchiveFile objects group them into sub-bands
        then remove the edges of each sub-band to remove the artifacts
        caused by aliasing. Finally, combine the sub-bands into a single 
        output file.

        The combined sub-band files are not saved.

        Inputs:
            infns: A list of input ArchiveFile objects.
            outfn: The output file's name.
            maxspan: The largest span, in seconds, for a combined data file. 
                (Default: 1890 s)
            maxgap: The largest gap allowed, in seconds, between input archives. 
                (Default: 300 s)
            num_to_trim: The number of channels to zero-weight at the
                top and bottom of each subband. (Default: 0, no trimming).

        Outputs:
            combinedfns: A list of output (combined) files.
    """
    # Combine files from the same sub-band in the time direction
    tmp_combined_subbands = []
    for ctr_freq, to_combine in utils.group_subints(infns).iteritems():
        utils.print_info("Combining %d subints at ctr freq %d MHz" % \
                            (len(to_combine), ctr_freq), 2)
        # Combine sub-integrations for this sub-band
        subfns = combine_subints(to_combine, outfn+".%(freq)dMHz",  maxspan, maxgap)
        tmp_combined_subbands.extend(subfns)
    
    if num_to_trim > 0:
        utils.print_info("Trimming %d channels from each subband edge " % \
                        num_to_trim, 2)
        for subfn in tmp_combined_subbands:
            clean.trim_edge_channels(subfn, num_to_trim=num_to_trim)
 
    # Combine the temporary sub-bands together in the frequency direction
    combinedfns = []
    for subbands in utils.group_subbands(tmp_combined_subbands):
        combinedfn = utils.get_outfn(outfn, subbands[0].fn)
        utils.print_info("Combining %d subbands into %s" % \
                            (len(subbands), combinedfn), 2)
        if combinedfn in combinedfns:
            warnings.warn("'combined_all(...)' is overwritting files it " \
                            "previously created!")
        combine_subbands(subbands, combinedfn)
        combinedfns.append(combinedfn)
    combinedfns = [utils.ArchiveFile(fn) for fn in combinedfns]

    if not config.debug.INTERMEDIATE:
        # Remove the temporary combined files
        for subfn in tmp_combined_subbands:
            os.remove(subfn.fn)
    return combinedfns


def combine_subints(infns, outfn, maxspan, maxgap):
    """Given a list of PSRCHIVE file names group them together using
        'psradd' assuming they are all sub-integrations from the same
        observing band.

        Inputs:
            infns: A list of intput sub-integration PSRCHIVE archive file names.
            outfn: The output file name to use.
            maxspan: The largest span, in seconds, for a combined data file. 
                This value is passed to 'psradd' with the '-g' flag.
            maxgap: The largest gap allowed, in seconds, between input archives. 
                This value is passed to 'psradd' with the '-G' flag.

        Output:
            outfns: A list of output file names.
    """
    mjds = np.array([float(fn.mjd) for fn in infns])
    mjdind = np.argsort(mjds)

    # Sort infiles and MJDs based on MJD
    infns = [infns[ii] for ii in mjdind]
    mjds = mjds[mjdind]
    secsince = (mjds-mjds[0])*24*3600 # Seconds since the first sub-int
    
    # First identify gaps
    gaps = np.ediff1d(secsince, to_begin=maxgap+1, to_end=maxgap+1)
    edges = np.argwhere(gaps>maxgap).squeeze()
    
    grouped_infns = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        groupnums = secsince[lo:hi].astype(int)/maxspan
        groupnums -= groupnums[0]
        tmp = []
        for gnum, infn in zip(groupnums, infns[lo:hi]):
            utils.print_debug("Appending %s to group %d" % (infn.fn, gnum), 'combine')
            if gnum+1 > len(tmp):
                tmp.append([infn])
            else:
                tmp[gnum].append(infn)
        grouped_infns += tmp
            
    utils.print_info("Split %d input archives into %d groups." % \
                        (len(infns), len(grouped_infns)), 2)
    utils.print_info("Groups: %s" % grouped_infns, 3)

    outfns = []
    for ii, grp in enumerate(grouped_infns):
        fn = utils.get_outfn(outfn, grp[0].fn)
        utils.print_info("First archive in group #%d: %s -- " \
                            "Output filename: %s" % (ii, grp[0], fn), 2)
        if fn in outfns:
            warnings.warn("'combined_subints(...)' is overwritting files it " \
                            "previously created!")
        utils.execute("psradd -o %s %s" % (fn, " ".join([g.fn for g in grp])))
        outfns.append(fn)

    return [utils.ArchiveFile(fn) for fn in outfns]


def combine_subbands(infns, outfn):
    """Given a list of PSRCHIVE file names group them together using
        'psradd' assuming they are all sub-bands from the same
        observation.

        Inputs:
            infns: A list of intput sub-bands PSRCHIVE archive file names.
            outfn: The output file's name

        Outputs:
            None
    """
    utils.execute("psradd -R -o %s %s" % (outfn, " ".join([infn.fn for infn in infns])))


def main():
    print ""
    print "        combine.py"
    print "     Patrick  Lazarus"
    print ""
    file_list = args + options.from_glob
    to_exclude = options.excluded_files + options.excluded_by_glob
    to_combine = utils.exclude_files(file_list, to_exclude)
    print "Number of input files: %d" % len(to_combine)
    
    to_combine = [utils.ArchiveFile(fn) for fn in to_combine]
    
    # Read configurations
    cfg = config.CoastGuardConfigs()
    cfg.get_default_configs()
    cfg.get_configs_for_archive(to_combine[0])
  
    # Combine files
    outfns = combine_all(to_combine, options.outfn, maxspan=cfg.combine_maxspan, \
                    maxgap=cfg.combine_maxgap, num_to_trim=cfg.nchan_to_trim)
    print "Output file names:" 
    for outfn in outfns:
        print "    %s" % outfn.fn


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
    options, args = parser.parse_args()
    main()
