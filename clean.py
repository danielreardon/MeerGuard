#!/usr/bin/env python

"""
Given a PSRCHIVE archive clean it up using 'paz'.

Patrick Lazarus, Nov. 11, 2011
"""
import optparse
import sys
import types

import utils

def deep_clean(ar):
    plot(ar, "before_deep_clean")
    
    # First clean channels
    chandata = get_chans(ar, remove_prof=True)
    chanweights = get_chan_weights(ar).astype(bool)
    chanmeans = scale_chans(chandata.mean(axis=1), chanweights=chanweights)
    chanmeans /= get_robust_std(chanmeans, chanweights)
    chanstds = scale_chans(chandata.std(axis=1), chanweights=chanweights)
    chanstds /= get_robust_std(chanstds, chanweights)

    badchans = np.concatenate((np.argwhere(chanmeans >= 5.0), \
                                    np.argwhere(chanstds >= 5.0)))
    for ichan in np.unique(badchans):
        print "De-weighting chan# %d" % ichan
        zero_weight_chan(ar, ichan)

    plot(ar, "mid-chans_deep_clean")

    # Next clean subints
    subintdata = get_subints(ar, remove_prof=True)
    subintweights = get_subint_weights(ar).astype(bool)
    subintmeans = scale_subints(subintdata.mean(axis=1), \
                                    subintweights=subintweights)
    subintmeans /= get_robust_std(subintmeans, subintweights)
    subintstds = scale_subints(subintdata.std(axis=1), \
                                    subintweights=subintweights)
    subintstds /= get_robust_std(subintstds, subintweights)

    badsubints = np.concatenate((np.argwhere(subintmeans >= 5.0), \
                                    np.argwhere(subintstds >= 5.0)))
    for isub in np.unique(badsubints):
        print "De-weighting subint# %d" % isub
        zero_weight_subint(ar, isub)

    plot(ar, "mid-subints_deep_clean")
    
    # Now replace hot bins
    clean_hot_bins(ar, thresh=2.0)
    plot(ar, "after_deep_clean")
    unloadfn = "%s.deepcleaned" % ar.get_filename()
    print "Unloading deep cleaned archive as %s" % unloadfn
    ar.unload(unloadfn)


def clean_simple(ar, timethresh=1.0, freqthresh=3.0):
    plot(ar, "before_simple_clean")
    # Get stats for subints
    subint_stats = get_subint_stats(ar)
    
    # Get stats for chans
    chan_stats = get_chan_stats(ar)

    for isub in np.argwhere(subint_stats >= timethresh):
        print "De-weighting subint# %d" % isub
        zero_weight_subint(ar, isub)
    for ichan in np.argwhere(chan_stats >= freqthresh):
        print "De-weighting chan# %d" % ichan
        zero_weight_chan(ar, ichan)
    plot(ar, "after_simple_clean")
    unloadfn = "%s.cleaned" % ar.get_filename()
    print "Unloading cleaned archive as %s" % unloadfn
    ar.unload(unloadfn)


def clean_iterative(ar, threshold=2.0):
    ii = 0
    while True:
        # Get stats for subints
        subint_stats = get_subint_stats(ar)
        worst_subint = np.argmax(subint_stats)
        
        # Get stats for chans
        chan_stats = get_chan_stats(ar)
        worst_chan = np.argmax(chan_stats)

        # Check that at least something should be masked
        if (chan_stats[worst_chan] < threshold) and \
                    (subint_stats[worst_subint] < threshold):
            break
        else:
            if subint_stats[worst_subint] > chan_stats[worst_chan]:
                print "De-weighting subint# %d" % worst_subint
                zero_weight_subint(ar, worst_subint)
            else:
                print "De-weighting chan# %d" % worst_chan
                zero_weight_chan(ar, worst_chan)
        plot(ar, "bogus_%d" % ii)
        ii += 1
    unloadfn = "%s.cleaned" % ar.get_filename()
    print "Unloading cleaned archive as %s" % unloadfn
    ar.unload(unloadfn)


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
