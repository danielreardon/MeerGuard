#!/usr/bin/env python

"""
Given a list of PSRCHIVE archives combine them.

Patrick Lazarus, Nov. 10, 2011
"""
import glob 
import os
import sys
import tempfile

import utils
import clean
import config

def combine_all(infns, outfn, num_to_trim=0):
    """Given a list of PSRCHIVE file names group them into sub-bands
        then remove the edges of each sub-band to remove the artifacts
        caused by aliasing. Finally, combine the sub-bands into a single 
        output file.

        The combined sub-band files are not saved.

        Inputs:
            infns: A list of input PSRCHIVE archive file names.
            outfn: The output file's name.
            num_to_trim: The number of channels to zero-weight at the
                top and bottom of each subband. (Default: 0, no trimming).

        Outputs:
            None
    """
    # Generate a filename
    hdr = utils.get_header_vals(infns[0], ['name', 'mjd'])
    basenm = "%s_MJD%.2f" % (hdr['name'], float(hdr['mjd']))

    # Combine files from the same sub-band in the time direction
    tmp_combined_subbands = []
    for ii, (ctr_freq, to_combine) in \
            enumerate(utils.group_by_ctr_freq(infns).iteritems()):
        subfn = basenm + ".sub%d.tmp" % ii
        # Combine sub-integrations for this sub-band
        combine_subints(to_combine, subfn)
        tmp_combined_subbands.append(subfn)
    
    if num_to_trim > 0:
        if config.verbosity > 1:
            print "Trimming %d channels from each subband edge " % \
                        num_to_trim
        for subfn in tmp_combined_subbands:
            clean.trim_edge_channels(subfn, num_to_trim=num_to_trim)
 
    # Combine the temporary sub-bands together in the frequency direction
    if config.verbosity:
        print "Combining %d subbands" % len(tmp_combined_subbands)
    combine_subbands(tmp_combined_subbands, outfn)
 
    if not config.debug.INTERMEDIATE:
        # Remove the temporary combined files
        for subfn in tmp_combined_subbands:
            os.remove(subfn)


def combine_subints(infns, outfn):
    """Given a list of PSRCHIVE file names group them together using
        'psradd' assuming they are all sub-integrations from the same
        observing band.

        Inputs:
            infns: A list of intput sub-integration PSRCHIVE archive file names.
            outfn: The output file's name

        Outputs:
            None
    """
    utils.execute("psradd -o %s %s" % (outfn, " ".join(infns)))


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
    utils.execute("psradd -R -o %s %s" % (outfn, " ".join(infns)))


def main():
    file_list = args + options.from_glob
    to_exclude = options.excluded_files + options.excluded_by_glob
    to_combine = utils.exclude_files(file_list, to_exclude)
    
    # Interpolate filename
    outfn = utils.get_outfn(options.outfn, to_combine[0])
    
    # Read configurations
    cfg = config.CoastGuardConfigs()
    cfg.get_default_configs()
    cfg.get_configs_for_archive(to_combine[0])
  
    print ""
    print "        combine.py"
    print "     Patrick  Lazarus"
    print ""
    print "Number of input files: %d" % len(to_combine)
    print "Output file name: %s" % outfn
    
    # Combine files
    combine_all(to_combine, outfn, num_to_trim=cfg.nchan_to_trim)


if __name__=="__main__":
    parser = utils.DefaultOptions(usage="%prog [OPTIONS] FILES ...", \
                        description="Given a list of PSRCHIVE file names " \
                                    "group them into sub-bands then combine " \
                                    "the sub-bands into a single output file.")
    parser.add_option('-o', '--outname', dest='outfn', type='string', \
                        help="The output (combined) file's name. " \
                            "(Default: 'combine.out.ar')", \
                        default="combine.out.ar")
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
