#!/usr/bin/env python

"""
Given a PSRCHIVE archive clean it up using 'paz'.

Patrick Lazarus, Nov. 11, 2011
"""
import optparse
import sys
import types

import numpy as np
import matplotlib.pyplot as plt
import psrchive

import utils
import clean_utils

def power_wash(ar):
    """Power wash RFI out of the data.

        Input:
            ar: The archive to be cleaned.
        Outputs:
            None - The archive is cleaned in place.
    """
    ar.pscrunch()
    ar.remove_baseline()
    ar.dedisperse()

    # Remove profile
    data = ar.get_data().squeeze()
    template = np.apply_over_axes(np.sum, data, (0,1)).squeeze()
    data = clean_utils.remove_profile(data, ar.get_nsubint(), ar.get_nchan(), \
                                        template, 4)

    bad_chans = []
    bad_subints = []
    bad_pairs = []
    std_sub_vs_chan = np.std(data, axis=2)
    print std_sub_vs_chan.shape
    #mean_sub_vs_chan = np.mean(data, axis=2)

    # Identify bad sub-int/channel pairs
    subintweights = clean_utils.get_subint_weights(ar).astype(bool)
    chanweights = clean_utils.get_chan_weights(ar).astype(bool)
    for isub in range(ar.get_nsubint()):
        for ichan in range(ar.get_nchan()):
            plt.figure()
            plt.subplot(2,1,1)
            plt.plot(std_sub_vs_chan[isub, :], 'k-')
            subint = clean_utils.scale_chans(std_sub_vs_chan[isub, :], \
                                                chanweights=chanweights)
            print clean_utils.get_hot_bins(subint)
            plt.subplot(2,1,2)
            plt.plot(subint, 'r-')
            plt.title("Subint #%d" % isub)
            plt.figure()
            plt.subplot(2,1,1)
            plt.plot(std_sub_vs_chan[:, ichan], 'k-')
            chan = clean_utils.scale_subints(std_sub_vs_chan[:, ichan], \
                                                subintweights=subintweights)
            print clean_utils.get_hot_bins(chan)
            plt.subplot(2,1,2)
            plt.plot(chan, 'r-')
            plt.title("Chan #%d" % ichan)
            plt.show() 
    
    chanstds = np.sum(std_sub_vs_chan, axis=0)
    plt.subplot(2,1,1)
    plt.plot(chanstds)
    chanstds = clean_utils.scale_chans(chanstds, chanweights=chanweights)
    plt.subplot(2,1,2)
    plt.plot(chanstds)
    bad_chans.extend(np.argwhere(chanstds > 1).squeeze())
    plt.show()


def deep_clean(ar, chanthresh=5.0, subintthresh=5.0, binthresh=2.0):
    #plot(ar, "before_deep_clean")
    
    # First clean channels
    chandata = clean_utils.get_chans(ar, remove_prof=True)
    chanweights = clean_utils.get_chan_weights(ar).astype(bool)
    chanmeans = clean_utils.scale_chans(chandata.mean(axis=1), chanweights=chanweights)
    chanmeans /= clean_utils.get_robust_std(chanmeans, chanweights)
    chanstds = clean_utils.scale_chans(chandata.std(axis=1), chanweights=chanweights)
    chanstds /= clean_utils.get_robust_std(chanstds, chanweights)

    #plt.figure()
    #plt.subplot(2,1,1)
    #plt.plot(chanstds, 'k')
    #plt.axhline(chanthresh, c='k', ls='--')
    #plt.ylabel("Scaled std")
    #plt.subplot(2,1,2)
    #plt.plot(chandata.std(axis=1))

    #plt.figure()
    #plt.subplot(2,1,1)
    #plt.plot(chanmeans, 'k')
    #plt.axhline(chanthresh, c='k', ls='--')
    #plt.ylabel("Scaled mean")
    #plt.subplot(2,1,2)
    #plt.plot(chandata.mean(axis=1))
    #plt.show()
    badchans = np.concatenate((np.argwhere(chanmeans >= chanthresh), \
                                    np.argwhere(chanstds >= chanthresh)))
    for ichan in np.unique(badchans):
        print "De-weighting chan# %d" % ichan
        clean_utils.zero_weight_chan(ar, ichan)

    #plot(ar, "mid-chans_deep_clean")

    # Next clean subints
    subintdata = clean_utils.get_subints(ar, remove_prof=True)
    subintweights = clean_utils.get_subint_weights(ar).astype(bool)
    subintmeans = clean_utils.scale_subints(subintdata.mean(axis=1), \
                                    subintweights=subintweights)
    subintmeans /= clean_utils.get_robust_std(subintmeans, subintweights)
    subintstds = clean_utils.scale_subints(subintdata.std(axis=1), \
                                    subintweights=subintweights)
    subintstds /= clean_utils.get_robust_std(subintstds, subintweights)

    badsubints = np.concatenate((np.argwhere(subintmeans >= subintthresh), \
                                    np.argwhere(subintstds >= subintthresh)))
    for isub in np.unique(badsubints):
        print "De-weighting subint# %d" % isub
        clean_utils.zero_weight_subint(ar, isub)

    #plot(ar, "mid-subints_deep_clean")
    
    # Now replace hot bins
    clean_utils.clean_hot_bins(ar, thresh=binthresh)
    #plot(ar, "after_deep_clean")
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
    for fn in infns:
        ar = psrchive.Archive_load(fn)
        deep_clean(ar)


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
