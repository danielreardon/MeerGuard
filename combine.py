#!/usr/bin/env python

"""
Given a list of PSRCHIVE archives combine them.

Patrick Lazarus, Nov. 10, 2011
"""
import glob 
import os
import sys
import tempfile

import numpy as np

import utils

def combine_all(infns, outfn):
    """Given a list of PSRCHIVE file names group them into sub-bands
        then combine the sub-bands into a single output file.

        The combined sub-band files are not saved.

        Inputs:
            infns: A list of input PSRCHIVE archive file names.
            outfn: The output file's name.

        Outputs:
            None
    """
    # Divide input file names by centre frequency
    ctr_freqs = np.asarray([get_ctr_freq(fn) for fn in infns])
    tmp_combined_subbands = []
    for ctr_freq in np.unique(ctr_freqs):
        # Collect the input files that are part of this sub-band
        indices = np.argwhere(ctr_freqs==ctr_freq)
        to_combine = []
        for index in indices:
            to_combine.append(infns[index])
        
        # Create a temporary output file
        tmphandle, tmpfn = tempfile.mkstemp(suffix=".%dMHz.tmp" % ctr_freq, \
                                            prefix="combined", dir=os.getcwd())
        os.close(tmphandle)

        # Combine sub-integrations for this sub-band
        combine_subints(to_combine, tmpfn)
        tmp_combined_subbands.append(tmpfn)

    # Combine the temporary sub-bands together in the frequency direction
    combine_subbands(tmp_combined_subbands, outfn)

    # Remove the temporary combined sub-band files
    for to_remove in tmp_combined_subbands:
        os.remove(to_remove)


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


def get_ctr_freq(infn):
    """Given a PSRCHIVE file find and return its centre frequency.

        This function calls PSRCHIVE's 'vap' and parses the output.

        Input:
            infn: The file for which the centre frequency will be found.

        Output:
            ctr_freq: The centre frequency of the file (in MHz).
    """
    out, err = utils.execute("vap -n -c freq %s" % infn)

    # Output format of 'vap -n -c freq <filename>' is: 
    #   <filename> <freq (in MHz)>
    freq = float(out.split()[1])
    return freq


def exclude_files(file_list, to_exclude):
    return [f for f in file_list if f not in to_exclude]


def main():
    file_list = args + options.from_glob
    to_exclude = options.excluded_files + options.excluded_by_glob
    to_combine = exclude_files(file_list, to_exclude)
    
    print ""
    print "        combine.py"
    print "     Patrick  Lazarus"
    print ""
    print "Number of input files: %d" % len(to_combine)
    print "Output file name: %s" % options.outfn
    combine_all(to_combine, options.outfn)


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
