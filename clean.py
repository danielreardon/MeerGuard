#!/usr/bin/env python

"""
Given a PSRCHIVE archive clean it up using 'paz'.

Patrick Lazarus, Nov. 11, 2011
"""
import optparse
import sys
import types

import utils

def trim_edge_channels(infn, num_to_trim=2):
    """Trim the edge channels of an input file to remove 
        band-pass roll-off and the effect of aliasing. 
        The file is modified in-place. However, zero-weighting 
        is used for trimming, so the process is reversible.

        Inputs:
            infn: names of file to trim.
            num_to_trim: The number of channels to remove from
                each edge of the sub-band. (Default: 2)

        Outputs:
            None
    """
    numchans = utils.get_header_param(infn, 'nchan')
    utils.execute('paz -m -Z "0 %d" -Z "%d %d" %s' % \
                (num_to_trim-1, numchans-num_to_trim, numchans-1, infn))
    return infn

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
