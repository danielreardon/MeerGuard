"""
Useful utility functions for cleaning a PSRCHIVE archive.

Patrick Lazarus, Feb. 14, 2012
"""
import warnings
import multiprocessing

import numpy as np
import scipy.stats
import scipy.optimize

import utils
import config
import errors

def get_subint_weights(ar):
    return ar.get_weights().sum(axis=1)


def get_chan_weights(ar):
    return ar.get_weights().sum(axis=0)


def comprehensive_stats(data, axis, **kwargs):
    """The comprehensive scaled stats that are used for
        the "Surgical Scrub" cleaning strategy.

        Inputs:
            data: A 3-D numpy array.
            axis: The axis that should be used for computing stats.
            chanthresh: The threshold (in number of sigmas) a
                profile needs to stand out compared to others in the
                same channel for it to be removed.
                (Default: use value defined in config files)
            subintthresh: The threshold (in number of sigmas) a profile
                needs to stand out compared to others in the same
                sub-int for it to be removed.
                (Default: use value defined in config files)

        Output:
            stats: A 2-D numpy array of stats.
    """
    chanthresh = kwargs.pop('chanthresh', config.cfg.clean_chanthresh)
    subintthresh = kwargs.pop('subintthresh', config.cfg.clean_subintthresh)

    nsubs, nchans, nbins = data.shape
    diagnostic_functions = [
            np.ma.std, \
            np.ma.mean, \
            np.ma.ptp, \
            lambda data, axis: np.max(np.abs(np.fft.rfft(\
                                data-np.expand_dims(data.mean(axis=axis), axis=axis), \
                                    axis=axis)), axis=axis), \
            #lambda data, axis: scipy.stats.mstats.normaltest(data, axis=axis)[0], \
            ]
    # Compute diagnostics
    diagnostics = []
    for func in diagnostic_functions:
        diagnostics.append(func(data, axis=2))

    # Now step through data and identify bad profiles
    scaled_diagnostics = []
    for diag in diagnostics:
        chan_scaled = np.abs(channel_scaler(diag, **kwargs))/chanthresh
        subint_scaled = np.abs(subint_scaler(diag, **kwargs))/subintthresh
        #print diag[95,76], chan_scaled[95,76]*chanthresh, subint_scaled[95,76]*subintthresh, chan_scaled.dtype, subint_scaled.dtype
        scaled_diagnostics.append(np.max((chan_scaled, subint_scaled), axis=0))

    #for sd in scaled_diagnostics:
    #    print sd[95, 76]
    #sorted_tests = np.sort(scaled_diagnostics, axis=0)
    #test_results = scipy.stats.mstats.gmean(scaled_diagnostics[-2:], axis=0)
    test_results = np.median(scaled_diagnostics, axis=0)
    return test_results


def channel_scaler(array2d, **kwargs):
    """For each channel detrend and scale it.
    """
    # Grab key-word arguments. If not present use default configs.
    orders = kwargs.pop('chan_order', config.cfg.chan_order)
    breakpoints = kwargs.pop('chan_breakpoints', config.cfg.chan_breakpoints)
    numpieces = kwargs.pop('chan_numpieces', config.cfg.chan_numpieces)
    if breakpoints is None:
        breakpoints = [[]]*len(orders)
    if numpieces is None:
        numpieces = [None]*len(orders)

    scaled = np.empty_like(array2d)
    nchans = array2d.shape[1]
    for ichan in np.arange(nchans):
        detrended = array2d[:,ichan]
        for order, brkpnts, numpcs in zip(orders, breakpoints, numpieces):
            detrended = iterative_detrend(detrended, order=order, \
                                            bp=brkpnts, numpieces=numpcs)
        median = np.ma.median(detrended)
        mad = np.ma.median(np.abs(detrended-median))
        scaled[:, ichan] = (detrended-median)/mad
    return scaled


def subint_scaler(array2d, **kwargs):
    """For each sub-int detrend and scale it.
    """
    # Grab key-word arguments. If not present use default configs.
    orders = kwargs.pop('subint_order', config.cfg.subint_order)
    breakpoints = kwargs.pop('subint_breakpoints', config.cfg.subint_breakpoints)
    numpieces = kwargs.pop('subint_numpieces', config.cfg.subint_numpieces)
    if breakpoints is None:
        breakpoints = [[]]*len(orders)
    if numpieces is None:
        numpieces = [None]*len(orders)

    scaled = np.empty_like(array2d)
    nsubs = array2d.shape[0]
    for isub in np.arange(nsubs):
        detrended = array2d[isub,:]
        for order, brkpnts, numpcs in zip(orders, breakpoints, numpieces):
            detrended = iterative_detrend(detrended, order=order, \
                                            bp=brkpnts, numpieces=numpcs)
        median = np.ma.median(detrended)
        mad = np.ma.median(np.abs(detrended-median))
        scaled[isub,:] = (detrended-median)/mad
    return scaled


def get_robust_std(data, weights, trimfrac=0.1):
    mdata = np.ma.masked_where(np.bitwise_not(weights), data)
    unmasked = mdata.compressed()
    mad = np.median(np.abs(unmasked-np.median(unmasked)))
    return 1.4826*mad
    #return scipy.stats.mstats.std(scipy.stats.mstats.trimboth(mdata, trimfrac))


def fit_poly(ydata, xdata, order=1):
    """Fit a polynomial to data using scipy.linalg.lstsq().

        Inputs:
            ydata: A 1D array to be detrended.
            xdata: A 1D array of x-values to use
            order: Order of polynomial to use (Default: 1)

        Outputs:
            x: An array of polynomial order+1 coefficients
            poly_ydata: A array of y-values of the polynomial evaluated
                at the input xvalues.
    """
    # Convert inputs to masked arrays
    # Note these arrays still reference the original data/arrays
    xmasked = np.ma.asarray(xdata)
    ymasked = np.ma.asarray(ydata)
    if not np.ma.count(ymasked):
        # No unmasked values!
        raise ValueError("Cannot fit polynomial to data. " \
                        "There are no unmasked values!")
    ycomp = ymasked.compressed()
    xcomp = xmasked.compressed()

    powers = np.arange(order+1)

    A = np.repeat(xcomp, order+1)
    A.shape = (xcomp.size, order+1)
    A = A**powers

    x, resids, rank, s = scipy.linalg.lstsq(A, ycomp)

    # Generate decompressed detrended array
    A = np.repeat(xmasked.data, order+1)
    A.shape = (len(xmasked.data), order+1)
    A = A**powers

    poly_ydata = np.dot(A, x).squeeze()

    return x, poly_ydata

def detrend(ydata, xdata=None, order=1, bp=[], numpieces=None):
    """Detrend 'data' using a polynomial of given order.

        Inputs:
            ydata: A 1D array to be detrended.
            xdata: A 1D array of x-values to use
                (Default: Use indices at xdata).
            order: Order of polynomial to use (Default: 1)
            bp: Breakpoints. Break the input data into segments
                that are detrended independently. The values
                listed here determine the indices where new
                segments start. The data will be split into
                len(bp)+1 segments. (Default: do not break input data)
            numpieces: Automatically determine breakpoints by splitting
                input data into roughly equal parts. This option, if provided,
                will override 'bp'. (Default: treat data as 1 piece).

        Output:
            detrended: a 1D array.
    """
    ymasked = np.ma.masked_array(ydata, mask=np.ma.getmaskarray(ydata))
    if xdata is None:
        xdata = np.ma.masked_array(np.arange(ydata.size), mask=np.ma.getmaskarray(ydata))
    detrended = ymasked.copy()

    if numpieces is None:
        edges = [0]+bp+[len(ydata)]
    else:
        # Determine indices to split at based on desired numbers of pieces
        isplit = np.linspace(0, len(ydata), numpieces+1, endpoint=1)
        edges = np.round(isplit).astype(int)
    for start, stop in zip(edges[:-1], edges[1:]):
        if not np.ma.count(ymasked[start:stop]):
            # No unmasked values, skip this segment.
            # It will be masked in the output anyway.
            continue
        x, poly_ydata = fit_poly(ymasked[start:stop], xdata[start:stop], order)
        detrended.data[start:stop] -= poly_ydata
    if np.ma.isMaskedArray(ydata):
        return detrended
    else:
        return detrended.data


def iterative_detrend(ydata, thresh=5, reset_mask=True, *args, **kwargs):
    origmask = np.ma.getmaskarray(ydata)
    ymasked = np.ma.masked_array(ydata, mask=origmask)
    if not np.ma.count(ymasked):
        # No un-masked values
        return ymasked
    detrended = ymasked.copy()
    # mask outliers based on median and median absolute deviation
    median = np.ma.median(detrended)
    mad = np.ma.median(np.abs(detrended-median))
    detrended = np.ma.masked_where((detrended<(median-thresh*mad)) | \
                                        (detrended>(median+thresh*mad)), \
                                        detrended)
    while ymasked.count():
        # detrend
        detrended = detrend(ymasked, *args, **kwargs)
        # mask outliers based on median and median absolute deviation
        median = np.ma.median(detrended)
        mad = np.ma.median(np.abs(detrended-median))
        detrended = np.ma.masked_where((detrended<(median-thresh*mad)) | \
                                            (detrended>(median+thresh*mad)), \
                                            detrended)
        if np.all(detrended.mask==ymasked.mask):
            ymasked = detrended.copy()
            break
        else:
            ymasked = detrended.copy()
    if reset_mask:
        ymasked.mask = origmask
    return ymasked

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


def get_frequencies(ar):
    integ = ar.get_first_Integration()
    nchan = ar.get_nchan()
    freqs = np.empty(nchan)
    for ichan in xrange(nchan):
        freqs[ichan] = integ.get_Profile(0, ichan).get_centre_frequency()
    return freqs

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


def fit_template(prof, template):
    warnings.warn("Does this fitting work properly?", errors.CoastGuardWarning)
    # Define the error function for the leastsq fit
    err = lambda params: params[0]*template - prof - params[1]

    # Determine initial guesses
    init_offset = 0
    init_amp = np.max(prof)/float(np.max(template))

    # Fit
    params, status = scipy.optimize.leastsq(err, [init_amp, init_offset])
    if status not in (1,2,3,4):
        raise errors.FitError("Bad status for least squares fit of " \
                                "template to profile")
    return params


def remove_profile1d(prof, isub, ichan, template, phs):
    rotated_template = fft_rotate(template, phs)
    err = lambda amp: amp*rotated_template - prof
    params, status = scipy.optimize.leastsq(err, [1.0])

    #err = lambda amp: amp*template - prof
    #obj_func = lambda amp: np.sum(err(amp)**2)
    #params = scipy.optimize.fmin(obj_func, [1.0], ftol=1e-12, xtol=1e-12)
    #params, status = scipy.optimize.leastsq(err, [1.0])
    if status not in (1,2,3,4):
        warnings.warn("Bad status for least squares fit when " \
                            "removing profile", errors.CoastGuardWarning)
        return (isub, ichan), np.zeros_like(prof)
    else:
        return (isub, ichan), err(params)
    #return (isub, ichan), err(params)

def remove_profile(data, nsubs, nchans, template, nthreads=None):
    if nthreads is None:
        nthreads = config.cfg.nthreads
    if nthreads == 1:
        for isub, ichan in np.ndindex(nsubs, nchans):
            data[isub, ichan] = remove_profile1d(data[isub, ichan], \
                                            isub, ichan, template)[1]
    else:
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


def remove_profile1d_inplace(prof, isub, ichan, template):
    #err = lambda (amp, phs): amp*fft_rotate(template, phs) - prof
    #params, status = scipy.optimize.leastsq(err, [1, 0])
    err = lambda amp: amp*template - prof
    params, status = scipy.optimize.leastsq(err, [1])
    if status not in (1,2,3,4):
        warnings.warn("Bad status for least squares fit when " \
                            "removing profile", errors.CoastGuardWarning)
        return (isub, ichan), None
    else:
        return (isub, ichan), err(params)


def remove_profile_inplace(ar, template, phs, nthreads=1):
    data = ar.get_data()[:,0,:,:] # Select first polarization channel
                                  # archive is P-scrunched, so this is
                                  # total intensity, the only polarization
                                  # channel
    if nthreads is None:
        nthreads = config.cfg.nthreads
    if nthreads == 1:
        for isub, ichan in np.ndindex(ar.get_nsubint(), ar.get_nchan()):
            if len(np.shape(template)) > 1:  # multiple frequencies, find closest
                itemplate = template[ichan, :]  # assuming template is (nsubint x nchan)
            else:
                itemplate = template
            amps = remove_profile1d(data[isub, ichan], isub, ichan, itemplate, phs)[1]
            prof = ar.get_Profile(isub, 0, ichan)
            if amps is None:
                prof.set_weight(0)
            else:
                prof.get_amps()[:] = amps
    else:
        pool = multiprocessing.Pool(processes=nthreads)
        results = []
        for isub, ichan in np.ndindex(ar.get_nsubint(), ar.get_nchan()):
            if len(np.shape(template)) > 1:  # multiple frequencies, find closest
                itemplate = template[ichan, :]  # assuming template is (nsubint x nchan)
            else:
                itemplate = template
            results.append(pool.apply_async(remove_profile1d, \
                            args=(data[isub, ichan], isub, ichan, itemplate, phs)))
        pool.close()
        pool.join()
        for result in results:
            result.successful()
            (isub, ichan), amps = result.get()
            prof = ar.get_Profile(isub, 0, ichan)
            if amps is None:
                prof.set_weight(0)
            else:
                prof.get_amps()[:] = amps


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
            utils.print_info("Cleaning %d bins in subint# %d" % (len(hot_bins), isub), 2)
            if len(hot_bins):
                clean_subint(ar, isub, hot_bins)
        else:
            # Subint is masked. Nothing to do.
            pass

    # Re-dedisperse data using original DM
    utils.print_debug("Re-dedispersing data", 'clean')
    ar.set_dispersion_measure(orig_dm)
    ar.dedisperse()
    utils.print_debug( "Done re-dedispersing data", 'clean')


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
        utils.print_debug("hottest bin: %d, stat before: %g, stat after: %g" % \
                        (to_mask, prev_stat, curr_stat), 'clean')
        if only_decreasing and (curr_stat > prev_stat):
            # Stat is increasing and we don't want that!
            # Undo what we just masked and return the mask
            masked_data.mask[to_mask] = False
            return (np.flatnonzero(masked_data.mask), 1)
        # Iterate
        prev_stat = curr_stat


def write_psrsh_script(arf, outfn=None):
    """Write a psrsh script that applies the same weighting
        as in the given ArchiveFile.

        Inputs:
            arf: An ArchiveFile object
            outfn: The name of the file to write to.
                (default: return psrsh commands as a single string)

        Outputs:
            outfn: The name of the file written.
    """
    lines = ["#!/usr/bin/env psrsh",
             "",
             "# Run with psrsh -e <ext> <script.psh> <archive.ar>",
             ""]
    # First write zapped channels
    zapped_chans = (get_chan_weights(arf.get_archive())==0)
    ma = np.ma.array(zapped_chans, mask=~zapped_chans)
    if any(zapped_chans):
        line = "zap chan "
        for interval in np.ma.flatnotmasked_contiguous(ma):
            lo = interval.start
            hi = interval.stop-1
            if lo==hi:
                line += "%d " % lo
            elif lo < hi:
                line += "%d-%d " % (lo, hi)
            else:
                raise ValueError("Interval start (%d) > end (%d)" % (lo, hi))
        lines.append(line)
    # Now write zapped subints
    zapped_ints = (get_subint_weights(arf.get_archive())==0)
    ma = np.ma.array(zapped_ints, mask=~zapped_ints)
    if any(zapped_ints):
        line = "zap subint "
        for interval in np.ma.flatnotmasked_contiguous(ma):
            lo = interval.start
            hi = interval.stop-1
            if lo==hi:
                line += "%d " % lo
            elif lo < hi:
                line += "%d-%d " % (lo, hi)
            else:
                raise ValueError("Interval start (%d) > end (%d)" % (lo, hi))
        lines.append(line)
    # Now write zapped pairs
    zapped = arf.get_archive().get_weights()==0
    nsub, nchan = zapped.shape
    npairs = 0
    line = "zap such "
    for isub in xrange(nsub):
        if zapped_ints[isub]:
            continue
        for ichan in xrange(nchan):
            if zapped_chans[ichan]:
                continue
            if zapped[isub, ichan]:
                line += "%d,%d " % (isub, ichan)
                npairs += 1
    if npairs:
        lines.append(line)
    if outfn is None:
        return "\n".join(lines)
    else:
        # Write file
        with open(outfn, 'w') as ff:
            ff.write("\n".join(lines))

def write_ebpp_chan_zap_script(arf, outfn=None):
    """Write a psrsh script that applies the same channel zapping
        as the EBPP archive provided.

        Inputs:
            arf: An EBPP ArchiveFile object
            outfn: The name of the file to write to.
                (default: return psrsh commands as a single string)

        Outputs:
            outfn: The name of the file written.
    """
    lines = ["#!/usr/bin/env psrsh",
             "",
             "# Run with psrsh -e <ext> <script.psh> <archive.ar>",
             ""]
    ar = arf.get_archive().clone()
    ar.tscrunch()
    # First write zapped channels
    zapped_chans = (get_chan_weights(ar)==0)
    freqs = get_frequencies(ar)
    chbw = np.mean(np.diff(freqs))
    # Trim band to EBPP band
    lines.append("zap freq >%f" % (np.max(freqs)+0.5*chbw))
    lines.append("zap freq <%f" % (np.min(freqs)-0.5*chbw))

    # Zap individual channels
    for ii, (iszapped, freq) in enumerate(zip(zapped_chans, freqs)):
        if iszapped:
            lines.append("zap freq %f:%f" % (freq-0.5*chbw, freq+0.5*chbw))

    if outfn is None:
        return "\n".join(lines)
    else:
        # Write file
        with open(outfn, 'w') as ff:
            ff.write("\n".join(lines))
