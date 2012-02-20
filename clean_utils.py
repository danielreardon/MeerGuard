"""
Useful utility functions for cleaning a PSRCHIVE archive.

Patrick Lazarus, Feb. 14, 2012
"""
import warnings
import multiprocessing

import numpy as np
import scipy.stats
import scipy.optimize

def get_subint_weights(ar):
    return ar.get_weights().sum(axis=1)


def get_chan_weights(ar):
    return ar.get_weights().sum(axis=0)


def get_robust_std(data, weights, trimfrac=0.1):
    mdata = np.ma.masked_where(np.bitwise_not(weights), data)
    unmasked = mdata.compressed()
    mad = np.median(np.abs(unmasked-np.median(unmasked)))
    return 1.4826*mad
    #return scipy.stats.mstats.std(scipy.stats.mstats.trimboth(mdata, trimfrac))


def get_profile(data):
    return np.sum(data, axis=0)


def scale_data(data, weights, subband_size=16, time_kernel_size=5):
    nsubs, nchans, nbins = data.shape
    # First scale chans
    for ichan in nchans:
        for isub in nsubs:
            chans = data[isub, :]
            data[isub, :] = scale_chans(chans, subband_size, weights[isub, :])

    # Now scale subints
    for isub in nsubs:
        for ichan in nchans:
            subints = data[:, ichan]
            data[:, ichan] = scale_subints(subints, time_kernel_size, weights[:, ichan])
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


def get_chan_stats(ar):
    nchans = ar.get_nchan()
    data = get_chans(ar, remove_prof=True)
    std = scale(data.std(axis=1), get_chan_weights(ar).astype(bool))
    return std/np.std(std)


def get_chans(ar, remove_prof=False, use_weights=True):
    clone = ar.clone()
    clone.remove_baseline()
    clone.dedisperse()
    clone.pscrunch()
    #clone.tscrunch()
    data = clone.get_data().squeeze()
    if use_weights:
        data = apply_weights(data, ar.get_weights())
    template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
    if remove_prof:
        data = remove_profile(data, clone.get_nsubint(), clone.get_nchan(), \
                                template)
    data = data.sum(axis=0)
    return data
    

def get_subints(ar, remove_prof=False, use_weights=True):
    clone = ar.clone()
    clone.remove_baseline()
    clone.set_dispersion_measure(0)
    clone.dedisperse()
    clone.pscrunch()
    #clone.fscrunch()
    data = clone.get_data().squeeze()
    if use_weights:
        data = apply_weights(data, ar.get_weights())
    template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
    if remove_prof:
        data = remove_profile(data, clone.get_nsubint(), clone.get_nchan(), \
                                template)
    data = data.sum(axis=1)
    return data


def apply_weights(data, weights):
    nsubs, nchans, nbins = data.shape
    for isub in range(nsubs):
        data[isub] = data[isub]*weights[isub,...,np.newaxis]
    return data


def fft_rotate(data, bins):
    """Return data rotated by 'bins' places to the left. The
        rotation is done in the Fourier domain using the Shift Theorem.

        Inputs:
            data: A 1-D numpy array to rotate.
            bins: The (possibly fractional) number of bins to rotate by.

        Outputs:
            rotated: The rotated data.
    """
    freqs = np.arange(data.size/2+1, dtype=np.float)
    phasor = np.exp(complex(0.0, 2.0*np.pi) * freqs * bins / float(data.size))
    return np.fft.irfft(phasor*np.fft.rfft(data))


def remove_profile1d(prof, isub, ichan, template):
    #err = lambda (amp, phs): amp*fft_rotate(template, phs) - prof
    #params, status = scipy.optimize.leastsq(err, [1, 0])
    err = lambda amp: amp*template - prof
    params, status = scipy.optimize.leastsq(err, [1])
    if status not in (1,2,3,4):
        warnings.warn("Bad status for least squares fit when " \
                            "removing profile")
        return (isub, ichan), np.zeros_like(prof)
    else:
        return (isub, ichan), err(params)


def remove_profile(data, nsubs, nchans, template, nthreads=1):
    if nthreads is None:
        nthreads = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=nthreads)
    results = []
    for isub, ichan in np.ndindex(nsubs, nchans):
        results.append(pool.apply_async(remove_profile1d, \
                        args=(data[isub, ichan], isub, ichan, template)))
    pool.close()
    pool.join()
    for result in results:
        result.successful()
        (isub, ichan), prof = result.get()
        data[isub, ichan] = prof
    return data 


def zero_weight_subint(ar, isub):
    subint = ar.get_Integration(int(isub))
    subint.uniform_weight(0.0)


def zero_weight_chan(ar, ichan):
    for isub in range(ar.get_nsubint()):
        subint = ar.get_Integration(int(isub))
        subint.set_weight(int(ichan), 0.0)


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
            hot_bins = get_hot_bins(subint, normstat_thresh=thresh)[0]
            print "Cleaning %d bins in subint# %d" % (len(hot_bins), isub)
            if len(hot_bins):
                clean_subint(ar, isub, hot_bins)
        else:
            # Subint is masked. Nothing to do.
            pass

    # Re-dedisperse data using original DM
    print "Re-dedispersing data"
    ar.set_dispersion_measure(orig_dm)
    ar.dedisperse()
    print "Done re-dedispersing data"


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
    masked_data = np.ma.masked_array(data, mask=np.zeros_like(data))

    prev_stat = scipy.stats.normaltest(masked_data.compressed())[0]
    while masked_data.count():
        if prev_stat < normstat_thresh:
            # Statistic is below threshold
            return (np.flatnonzero(masked_data.mask), 0)
        elif (max_num_hot is not None) and (len(hot_bins) >= max_num_hot):
            # Reached maximum number of hot bins
            return (np.flatnonzero(masked_data.mask), 2)

        imax = np.argmax(masked_data)
        imin = np.argmin(masked_data)
        median = np.median(masked_data)
        # find which (max or min) has largest deviation from the median
        median_to_max = masked_data[imax] - median
        median_to_min = median - masked_data[imin]

        if median_to_max > median_to_min:
            to_mask = imax
        else:
            to_mask = imin
        masked_data.mask[to_mask] = True
        curr_stat = scipy.stats.normaltest(masked_data.compressed())[0]
        print "hottest bin: %d, stat before: %g, stat after: %g" % \
                        (to_mask, prev_stat, curr_stat)
        if only_decreasing and (curr_stat > prev_stat):
            # Stat is increasing and we don't want that!
            # Undo what we just masked and return the mask
            masked_data.mask[to_mask] = False
            return (np.flatnonzero(masked_data.mask), 1)
        # Iterate
        prev_stat = curr_stat


