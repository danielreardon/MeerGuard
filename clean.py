#!/usr/bin/env python

"""
Given a PSRCHIVE archive clean it up using 'paz'.

Patrick Lazarus, Nov. 11, 2011
"""
import optparse
import sys
import types

import utils

def trim_edge_channels(num_to_trim=2, *infns):
    """Trim the edge channels of each input file to remove 
        band-pass roll-off and the effect of aliasing. 
        The file is modified in-place. However, zero-weighting 
        is used for trimming, so the process is reversible.

        Inputs:
            *infns: input files are given as positional arguments.
            num_to_trim: The number of channels to remove from
                each edge of the sub-band. (Default: 2)

        Outputs:
            None
    """
    for infn in infns:
        numchans = get_num_chans(infn)
        utils.execute('paz -m -Z "0 %d" -Z "%d %d" %s' % \
                    (num_to_trim-1, numchans-num_to_trim, numchans-1, infn))


def get_num_chans(infn):
    """Given a PSRCHIVE file find and return the number of channels.

        This function calls PSRCHIVE's 'vap' and parses the output.

        Input:
            infn: The file for which the number of channels will be found.

        Output:
            nchans: The number of channels in the file.
    """
    out, err = utils.execute("vap -n -c nchan %s" % infn)

    # Output format of 'vap -n -c nchan <filename>' is: 
    #   <filename> <# channels>
    nchans = int(out.split()[1])
    return nchans
    # Output format of 'vap -n -c freq <filename>' is: 
    #   <filename> <freq (in MHz)>
    freq = float(out.split()[1])
    return freq


def main():
    infns = args
    print ""
    print "         clean.py"
    print "     Patrick  Lazarus"
    print ""
    # Trim edge channels
    if options.num_chans_to_trim > 0:
        print "Trimming the edges... (# Chans: %d)" % \
                                options.num_chans_to_trim
        trim_edge_channels(options.num_chans_to_trim, *infns)
   

if __name__=="__main__":
    parser = utils.DefaultOptions(usage="%prog [OPTIONS] FILES ...", \
                        description="Given a list of PSRCHIVE file names " \
                                    "clean RFI from each one. \nNOTE: " \
                                    "The files are cleaned non-desctructively " \
                                    "by applying zero-weighting.")
    parser.add_option('--trim-edge-channels', dest='num_chans_to_trim', \
                        help="Trim the edges of each input file to remove " \
                            "band-pass roll-off and the effect of aliasing. " \
                            "(Default: 0, don't trim edges.)", \
                        default=0, type='int')
    options, args = parser.parse_args()
    main()
