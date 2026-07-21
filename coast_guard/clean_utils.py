"""
Useful utility functions for cleaning a PSRCHIVE archive.

Patrick Lazarus, Feb. 14, 2012

------------------------------------------------------
"""
import warnings
import multiprocessing

import numpy as np
import scipy.optimize
import scipy.linalg

from coast_guard import config
from coast_guard import errors

def get_subint_weights(ar):
    """Return the summed weight of each sub-int (summed over channels)."""
    return ar.get_weights().sum(axis=1)


def get_chan_weights(ar):
    """Return the summed weight of each channel (summed over sub-ints)."""
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
    aggressive = kwargs.pop('aggressive', False)

    diagnostic_functions = [
            np.ma.std, \
            np.ma.mean, \
            np.ma.ptp, \
            lambda data, axis: np.ma.max(np.abs(np.fft.rfft(\
                                data-np.expand_dims(np.ma.mean(data, axis=axis), axis=axis), \
                                    axis=axis)), axis=axis),
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
        scaled_diagnostics.append(np.max((chan_scaled, subint_scaled), axis=0))

    if aggressive:
        # max of 5 diagnostics
        test_results = np.max(scaled_diagnostics, axis=0)
    else:
        # average of 5 diagnostics
        test_results = np.mean(scaled_diagnostics, axis=0)

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
    if not (len(orders) == len(breakpoints) == len(numpieces)):
        # zip() below silently truncates to the shortest sequence, dropping
        # detrend passes. Warn so the mismatch is not silent.
        warnings.warn("chan_order/chan_breakpoints/chan_numpieces have "
                      "mismatched lengths (%d/%d/%d); only the first %d "
                      "detrend pass(es) will run."
                      % (len(orders), len(breakpoints), len(numpieces),
                         min(len(orders), len(breakpoints), len(numpieces))),
                      errors.CoastGuardWarning)

    scaled = np.empty_like(array2d)
    nchans = array2d.shape[1]
    for ichan in np.arange(nchans):
        detrended = array2d[:,ichan]
        for order, brkpnts, numpcs in zip(orders, breakpoints, numpieces):
            detrended = iterative_detrend(detrended, order=order, \
                                            bp=brkpnts, numpieces=numpcs)
        median = np.ma.median(detrended)
        mad = np.ma.median(np.abs(detrended-median))
        scaled[:, ichan] = (detrended-median)/(mad * 1.4826)  # Scale MAD to be consistent with std for normal distribution
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
    if not (len(orders) == len(breakpoints) == len(numpieces)):
        # zip() below silently truncates to the shortest sequence, dropping
        # detrend passes. Warn so the mismatch is not silent.
        warnings.warn("subint_order/subint_breakpoints/subint_numpieces have "
                      "mismatched lengths (%d/%d/%d); only the first %d "
                      "detrend pass(es) will run."
                      % (len(orders), len(breakpoints), len(numpieces),
                         min(len(orders), len(breakpoints), len(numpieces))),
                      errors.CoastGuardWarning)

    scaled = np.empty_like(array2d)
    nsubs = array2d.shape[0]
    for isub in np.arange(nsubs):
        detrended = array2d[isub,:]
        for order, brkpnts, numpcs in zip(orders, breakpoints, numpieces):
            detrended = iterative_detrend(detrended, order=order, \
                                            bp=brkpnts, numpieces=numpcs)
        median = np.ma.median(detrended)
        mad = np.ma.median(np.abs(detrended-median))
        scaled[isub,:] = (detrended-median)/(mad * 1.4826)  # Scale MAD to be consistent with std for normal distribution
    return scaled


def get_robust_std(data, weights, trimfrac=0.1):
    """Return a robust estimate of the standard deviation of 'data'.

        The estimate is based on the median absolute deviation (MAD) of the
        unmasked (weighted) data, scaled by 1.4826 to be consistent with the
        standard deviation for normally distributed data.

        Inputs:
            data: The data array.
            weights: A boolean array; False entries are masked out.
            trimfrac: Unused. (Default: 0.1)

        Output:
            std: The robust standard-deviation estimate.
    """
    mdata = np.ma.masked_where(np.bitwise_not(weights), data)
    unmasked = mdata.compressed()
    mad = np.median(np.abs(unmasked-np.median(unmasked)))
    return 1.4826*mad


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

def detrend(ydata, xdata=None, order=1, bp=None, numpieces=None):
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
    if bp is None:
        bp = []
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
    """Detrend 'ydata' iteratively, masking outliers between iterations.

        Outliers (points more than 'thresh' MADs from the median) are
        masked and the data re-detrended until the mask stops changing.

        Inputs:
            ydata: A 1D array to be detrended.
            thresh: Outlier threshold in units of the median absolute
                deviation. (Default: 5)
            reset_mask: If True, restore the input's original mask on the
                returned array. (Default: True)
            args, kwargs: Additional arguments passed to detrend().

        Output:
            detrended: The detrended (masked) 1D array.
    """
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
    """Return the profile obtained by summing 'data' over its first axis."""
    return np.sum(data, axis=0)


def scale_subints(data, kernel_size=5, subintweights=None):
    """Scale sub-int data by subtracting a running median of neighbours.

        Inputs:
            data: A 1D array of per-sub-int values.
            kernel_size: The number of neighbouring sub-ints (centred on
                each point) used to compute the median. (Default: 5)
            subintweights: A boolean array marking which sub-ints are good.
                (Default: treat all sub-ints as good)

        Output:
            scaled: The scaled 1D array.
    """
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


def get_frequencies(ar):
    """Return an array of the centre frequencies of each channel.

        Input:
            ar: The psrchive archive object.

        Output:
            freqs: A 1D array of channel centre frequencies (MHz).
    """
    integ = ar.get_first_Integration()
    nchan = ar.get_nchan()
    freqs = np.empty(nchan)
    for ichan in range(nchan):
        freqs[ichan] = integ.get_Profile(0, ichan).get_centre_frequency()
    return freqs

def apply_weights(data, weights):
    """Multiply data by its per-sub-int/channel weights.

        Inputs:
            data: A 3D array (nsub x nchan x nbin).
            weights: A 2D array (nsub x nchan) of weights.

        Output:
            data: The weighted data array.
    """
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
    # Use floor division so the phasor length matches the rfft output for
    # both even- and odd-length inputs (rfft returns data.size//2 + 1 bins).
    freqs = np.arange(data.size//2+1, dtype=np.float64)
    phasor = np.exp(complex(0.0, 2.0*np.pi) * freqs * bins / float(data.size))
    # Pass n=data.size so odd-length arrays are reconstructed at their
    # original length (irfft otherwise assumes an even-length signal).
    return np.fft.irfft(phasor*np.fft.rfft(data), n=data.size)

def remove_profile1d(prof, isub, ichan, template, phs, return_params=False):
    """Fit and subtract a (rotated, scaled) template from a single profile.

        Inputs:
            prof: The 1D profile data.
            isub: The sub-int index (returned for bookkeeping).
            ichan: The channel index (returned for bookkeeping).
            template: The 1D template profile.
            phs: The phase (in bins) by which to rotate the template.
            return_params: If True, also return the fit parameters.
                (Default: False)

        Outputs:
            (isub, ichan): The input indices.
            residual: The profile with the fitted template removed (zeros
                if the fit failed, None if the template was degenerate).
            params: (only if return_params) The best-fit amplitude(s), or
                None if the template was degenerate.
    """
    if not np.any(template):
        
        warnings.warn("All-zero template for (isub=%d, ichan=%d); no fit "
                            "possible, zero-weighting this profile."
                            % (isub, ichan), errors.CoastGuardWarning)
        if return_params:
            return (isub, ichan), None, None
        else:
            return (isub, ichan), None

    rotated_template = fft_rotate(template, phs)

    #safer than median estimation method
    denom = np.dot(rotated_template, rotated_template)
    amp_guess = np.dot(rotated_template, prof) / denom

    err = lambda amp: amp*rotated_template - prof
    params, status = scipy.optimize.leastsq(err, [amp_guess])

    if status not in (1,2,3,4):
        warnings.warn("Bad status for least squares fit when " \
                            "removing profile", errors.CoastGuardWarning)
        if return_params:
            return (isub, ichan), np.zeros_like(prof), params
        else:
            return (isub, ichan), np.zeros_like(prof)
    else:
        if return_params:
            return (isub, ichan), err(params), params
        else:
            return (isub, ichan), err(params)


def remove_profile(data, nsubs, nchans, template, nthreads=None):
    """Remove a template profile from every sub-int/channel of 'data'.

        Inputs:
            data: A 3D array (nsub x nchan x nbin) modified in place.
            nsubs: The number of sub-ints.
            nchans: The number of channels.
            template: The 1D template profile to remove.
            nthreads: Number of worker processes to use. If >1 the work is
                parallelised. (Default: use value from config files)

        Output:
            data: The data array with the profile removed.
    """
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
    """Fit and subtract an (unrotated) scaled template from a profile.

        Inputs:
            prof: The 1D profile data.
            isub: The sub-int index (returned for bookkeeping).
            ichan: The channel index (returned for bookkeeping).
            template: The 1D template profile.

        Outputs:
            (isub, ichan): The input indices.
            residual: The profile with the fitted template removed, or
                None if the fit failed.
    """
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
    """Remove a template profile from an archive in place.

        For each sub-int/channel the template (a 1D profile, or a 2D array
        indexed by channel) is fitted and subtracted. Profiles whose fit
        fails are zero-weighted.

        Inputs:
            ar: The (P-scrunched) psrchive archive object, modified in
                place.
            template: The template profile. Either 1D (nbin) or 2D
                (nchan x nbin).
            phs: The phase (in bins) by which to rotate the template.
            nthreads: Number of worker processes to use. If >1 the work is
                parallelised. (Default: 1)

        Outputs:
            None - The archive is modified in place.
    """
    data = ar.get_data()[:,0,:,:] # Select first polarization channel
                                  # archive is P-scrunched, so this is
                                  # total intensity, the only polarization
                                  # channel
    if nthreads is None:
        nthreads = config.cfg.nthreads
    if nthreads == 1:
        for isub, ichan in np.ndindex(ar.get_nsubint(), ar.get_nchan()):
            if len(np.shape(template)) > 1:  # multiple frequencies, take ichan slice
                itemplate = template[ichan, :]  # assuming template is (nchan x nbin)
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
    """Set the weight of sub-int 'isub' to zero (de-weight it entirely)."""
    #numpy2-safe:
    subint = ar.get_Integration(int(np.asarray(isub).item()))
    subint.uniform_weight(0.0)

def zero_weight_chan(ar, ichan):
    """Set the weight of channel 'ichan' to zero in every sub-int."""
    #numpy2-safe:
    ichan = int(np.asarray(ichan).item())
    for isub in range(ar.get_nsubint()):
        subint = ar.get_Integration(int(isub))
        subint.set_weight(ichan, 0.0)

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
    for isub in range(nsub):
        if zapped_ints[isub]:
            continue
        for ichan in range(nchan):
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
