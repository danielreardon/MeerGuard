#!/usr/bin/env python

"""
Given a list of PSRCHIVE archives combine them.

Patrick Lazarus, Nov. 10, 2011
"""
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
        utils.execute("psradd -o %s %s" % (tmpfn, " ".join(to_combine)))
        tmp_combined_subbands.append(tmpfn)

    # Combine the temporary sub-bands together in the frequency direction
    utils.execute("psradd -R -o %s %s" % \
                    (outfn, " ".join(tmp_combined_subbands)))

    # Remove the temporary combined sub-band files
    for to_remove in tmp_combined_subbands:
        os.remove(to_remove)
        

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


def main():
    to_combine = args
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
    options, args = parser.parse_args()
    main()
