#!/usr/bin/env python

"""
Given a PSRCHIVE archive create diagnostic plots.

Patrick Lazarus, Dec. 12, 2011
"""

import sys

import numpy as np
import scipy.signal
import scipy.stats
import scipy.optimize as opt
import matplotlib.cm
import matplotlib.pyplot as plt

import psrchive


def get_hot_bins(data, normstat_thresh=6.3, max_num_hot=None, \
                    only_decreasing=True):
    """Return a list of indices that are bin numbers causing the
        given data to be different from normally distributed.
        The bins returned will contain the highest values in 'data'.

        Inputs:
            data: A 1-D array of data.
            normstat_thresh: The threshold for the Omnibus K^2
                statistic used to determine normality of data.
                (Default 6.3 -- 95% quantile for 50-100 data points)
            max_num_hot: The maximum number of hot bins to return.
                (Default: None -- no limit)
            only_decreasing: If True, stop collecting "hot" bins and return
                the current list if the K^2 statistic begins to increase
                as bins are removed. (Default: True)

        Outputs:
            hot_bins: A list of "hot" bins.
            status: A return status.
                    0 = Statistic is below threshold (success)
                    1 = Statistic was found to be increasing (OK)
                    2 = Max number of hot bins reached (not good)
    """
    hot_bins = []
    tmp_data = list(data)

    prev_stat = scipy.stats.normaltest(tmp_data)[0]
    while tmp_data:
        if prev_stat < normstat_thresh:
            # Statistic is below threshold
            return (hot_bins, 0)
        elif (max_num_hot is not None) and (len(hot_bins) >= max_num_hot):
            # Reached maximum number of hot bins
            return (hot_bins, 2)

        imax = np.argmax(tmp_data)
        tmp_data.pop(imax)
        curr_stat = scipy.stats.normaltest(tmp_data)[0]
        if only_decreasing and (curr_stat > prev_stat):
            # Stat is increasing and we don't want that!
            return (hot_bins, 1)
        hot_bins.append(imax)
        # Iterate
        prev_stat = curr_stat


def scale(data, weights=slice(None)):
    medfilt = scipy.signal.medfilt(data[weights], kernel_size=5)
    data[weights] -= medfilt
    return data 


def scale_subints(data, kernel_size=5, subintweights=None):
    scaled = np.empty(len(data))
    if subintweights is None:
        subintweights = np.ones(len(data), dtype=bool)
    else:
        subintweights = np.asarray(subintweights).astype(bool)
    for ii in range(len(data)):
        lobin = ii-int(kernel_size/2)
        if lobin < 0:
            lobin=None
        
        hibin = ii+int(kernel_size/2)+1
        if hibin > len(data):
            hibin=None
        neighbours = np.asarray(data[lobin:hibin])
        neighbour_weights = subintweights[lobin:hibin]
        scaled[ii] = data[ii] - np.median(neighbours[neighbour_weights])
    return scaled


def scale_chans(data, nchans=16, chanweights=None):
    """ Find the median of each subband and subtract it from
        the data.

        Inputs:
            data: The channel data to scale.
            nchans: The number of channels to combine together for
                each subband (Default: 16)
    """
    scaled = np.empty(len(data))
    if chanweights is None:
        chanweights = np.ones(len(data), dtype=bool)
    else:
        chanweights = np.asarray(chanweights).astype(bool)
    for lochan in range(0, len(data), nchans):
        subscaled = np.asarray(data[lochan:lochan+nchans])
        subweights = chanweights[lochan:lochan+nchans]

        median = np.median(subscaled[subweights])
        subscaled[subweights] -= median
        subscaled[~subweights] = 0
        scaled[lochan:lochan+nchans] = subscaled
    return scaled


def get_profile(data):
    return np.sum(data, axis=0)


def remove_profile(data):
    template = get_profile(data)
    num = data.shape[0]
    err = lambda amps: np.ravel(amps[:,np.newaxis]*template - data)
    amps = opt.leastsq(err, np.zeros(num))[0]
    data -= template*amps[:,np.newaxis]
    return data


def get_chan_stats(ar):
    nchans = ar.get_nchan()
    data = get_chans(ar, remove_prof=True)
    std = scale(data.std(axis=1), get_chan_weights(ar).astype(bool))
    return std/np.std(std)


def get_chans(ar, remove_prof=False):
    clone = ar.clone()
    clone.remove_baseline()
    clone.dedisperse()
    clone.pscrunch()
    clone.tscrunch()
    data = clone.get_data().squeeze()
    if remove_prof:
        data = remove_profile(data)
    return data
    

def get_subint_stats(ar):
    nsubs = ar.get_nsubint()
    data = get_subints(ar, remove_prof=True)
    #std = scale(data.std(axis=1), get_subint_weights(ar).astype(bool))
    normtest = scipy.stats.mstats.normaltest(data, axis=1)[0]
    return normtest


def get_subints(ar, remove_prof=False):
    clone = ar.clone()
    clone.remove_baseline()
    clone.set_dispersion_measure(0)
    clone.dedisperse()
    clone.pscrunch()
    clone.fscrunch()
    data = clone.get_data().squeeze() 
    if remove_prof:
        data = remove_profile(data)
    return data


def zero_weight_subint(ar, isub):
    subint = ar.get_Integration(int(isub))
    subint.uniform_weight(0.0)


def zero_weight_chan(ar, ichan):
    for isub in range(ar.get_nsubint()):
        subint = ar.get_Integration(int(isub))
        subint.set_weight(int(ichan), 0.0)


def get_subint_weights(ar):
    return ar.get_weights().sum(axis=1)


def get_chan_weights(ar):
    return ar.get_weights().sum(axis=0)


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


def clean_hot_bins(ar, thresh=2.0):
    subintdata = get_subints(ar, remove_prof=True)
    subintweights = get_subint_weights(ar).astype(bool)
    
    # re-disperse archive because subintdata is at DM=0
    orig_dm = ar.get_dispersion_measure()
    ar.set_dispersion_measure(0)
    ar.dedisperse()
    
    # Clean hot bins
    for isub, subintweight in enumerate(subintweights):
        if subintweight:
            # Identify hot bins
            subint = subintdata[isub,:]
            hot_bins = get_hot_bins(subint, normstat_thresh=2)[0]
            if len(hot_bins):
                print "Cleaning %d bins in subint# %d" % (len(hot_bins), isub)
                clean_subint(ar, isub, hot_bins)
        else:
            # Subint is masked. Nothing to do.
            pass

    # Re-dedisperse data using original DM
    ar.set_dispersion_measure(orig_dm)
    ar.dedisperse()


def clean_subint(ar, isub, bins):
    npol = ar.get_npol()
    nchan = ar.get_nchan()
    nbins = ar.get_nbin()
    mask = np.zeros(nbins)
    mask[bins] = 1

    subint = ar.get_Integration(int(isub))
    for ichan in range(nchan):
        for ipol in range(npol):
            prof = subint.get_Profile(ipol, ichan)
            if prof.get_weight():
                data = prof.get_amps()
                masked_data = np.ma.array(data, mask=mask)
                std = masked_data.std()
                mean = masked_data.mean()
                noise = scipy.stats.norm.rvs(loc=mean, scale=std, size=len(bins))
                for ii, newval in zip(bins, noise):
                    data[ii] = newval


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


def get_robust_std(data, weights, trimfrac=0.1):
    mdata = np.ma.masked_where(np.bitwise_not(weights), data)
    unmasked = mdata.compressed()
    mad = np.median(np.abs(unmasked-np.median(unmasked)))
    return 1.4826*mad
    #return scipy.stats.mstats.std(scipy.stats.mstats.trimboth(mdata, trimfrac))


def plot(ar, basename=None):
    """Plot.

        Inputs:
            ar: The archive to make the plot for.
            basename: The basename of the output plots.
                (Default: Do not output plots.)

        Outputs:
            None
    """
    clone = ar.clone()
    clone.remove_baseline()
    clone.set_dispersion_measure(0)
    clone.dedisperse()
    clone.pscrunch()
    clone.fscrunch()
    nsubs = clone.get_nsubint()
    data = clone.get_data().squeeze()
    data = remove_profile(clone.get_data().squeeze())
    weights = get_subint_weights(ar).astype(bool)

    num_hot = lambda row: len(get_hot_bins(row, normstat_thresh=3)[0])
    funcs = [lambda data: scale_subints(data.mean(axis=1), subintweights=weights), \
             lambda data: scale_subints(data.std(axis=1), subintweights=weights), \
             lambda data: data.ptp(axis=1), \
             lambda data: scipy.stats.skew(data, axis=1), \
             lambda data: scipy.stats.kurtosis(data, axis=1), \
             lambda data: scipy.stats.mstats.normaltest(data, axis=1)[0], \
             lambda data: np.apply_along_axis(num_hot, 1, data)]
    labels = ["Mean", "Std dev", "Max-min", "Skew", "Kurtosis", "Normality", "Num to Norm"]
    thresholds = [5, 5, 1, 1, 1, 5, 0]
    scales = [5, 5, 1, 1, 1, 0, 0]
    width = 0.45
    N = len(funcs)
    dw = width/N

    plt.figure(figsize=(11,8))
    ax = plt.axes([0.1,0.1,0.4,0.7])
    plt.imshow(data, origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.ylabel('subint number')
    plt.xlabel('bin number')
    plt.axis('tight')
    
    plt.axes([0.1,0.8,0.4,0.1], sharex=ax)
    plt.plot(np.sum(data, axis=0), 'k-')
    plt.ylabel('Intensity')
    plt.setp(plt.gca().xaxis.get_ticklabels(), visible=False)
    plt.axis('tight')

    for ii, (func, label, thresh, scl) in \
                    enumerate(zip(funcs, labels, thresholds, scales)):
        plt.axes([0.5+dw*ii,0.1,dw,0.7], sharey=ax)
        stat = func(data)
        
        # Print normality info for stat
        #print label
        #isorts = np.argsort(stat)[::-1]
        #for jj, isort in enumerate(isorts[:30]):
        #    normality = scipy.stats.normaltest(stat[isorts[jj:]], axis=None)[0]
        #    print "    %d (%d): %g" % (jj, isort, normality) 
        
        if scl:
            plt.plot(stat/get_robust_std(stat, weights), np.arange(nsubs), 'k-')
        else:
            plt.plot(stat, np.arange(nsubs), 'k-')
        plt.axvline(thresh, c='k', ls='--')
        plt.xlabel(label)
        plt.xticks(rotation=45, size='x-small')
        plt.setp(plt.gca().yaxis.get_ticklabels(), visible=False)
        plt.axis('tight')
    
    if basename is not None:
        plt.savefig(basename+"_time-vs-phase.png")
    
    clone = ar.clone()
    clone.remove_baseline()
    clone.dedisperse()
    clone.pscrunch()
    clone.tscrunch()
    nchans = clone.get_nchan()
    data = remove_profile(clone.get_data().squeeze())
    weights = get_chan_weights(ar).astype(bool)

    funcs = [lambda data: scale_chans(data.mean(axis=1), chanweights=weights), \
             lambda data: scale_chans(data.std(axis=1), chanweights=weights), \
             lambda data: data.ptp(axis=1), \
             lambda data: scipy.stats.skew(data, axis=1), \
             lambda data: scipy.stats.kurtosis(data, axis=1), \
             lambda data: scipy.stats.mstats.normaltest(data, axis=1)[0]]
    
    plt.figure(figsize=(11,8))
    ax = plt.axes([0.1,0.1,0.4,0.7])
    plt.imshow(data, origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.ylabel('chan number')
    plt.xlabel('bin number')
    plt.axis('tight')
    
    plt.axes([0.1,0.8,0.4,0.1], sharex=ax)
    plt.plot(np.sum(data, axis=0), 'k-')
    plt.ylabel('Intensity')
    plt.setp(plt.gca().xaxis.get_ticklabels(), visible=False)
    plt.axis('tight')
    
    for ii, (func, label, thresh, scl) in \
                    enumerate(zip(funcs, labels, thresholds, scales)):
        plt.axes([0.5+dw*ii,0.1,dw,0.7], sharey=ax)
        stat = func(data)
        if scl:
            plt.plot(stat/get_robust_std(stat, weights), np.arange(nchans), 'k-')
        else:
            plt.plot(stat, np.arange(nchans), 'k-')
        plt.axvline(thresh, c='k', ls='--')
        plt.xlabel(label)
        plt.xticks(rotation=45, size='x-small')
        plt.setp(plt.gca().yaxis.get_ticklabels(), visible=False)
        plt.axis('tight')

    if basename is not None:
        plt.savefig(basename+"_freq-vs-phase.png")


def main():
    ar = psrchive.Archive_load(sys.argv[1])
    #clean_simple(ar, timethresh=5.0, freqthresh=3.0)
    deep_clean(ar)
    #plot(ar, "%s.testplot" % ar.get_filename())


if __name__ == '__main__':
    main()
