#!/usr/bin/env python

"""
Given a PSRCHIVE archive clean it up.

Patrick Lazarus, Nov. 11, 2011
"""
import optparse
import sys
import types
import re
import shutil
import os
import tempfile
import argparse
import warnings

import numpy as np
import scipy.stats
import matplotlib.pyplot as plt

from coast_guard import config
from coast_guard import utils
from coast_guard import clean_utils
from coast_guard import errors
from coast_guard import cleaners
from coast_guard import colour

def dummy(ar):
    """A do-nothing dummy cleaning function.
        
        Input:
            ar: The archive to be cleaned.
        Outputs:
            None - The archive is cleaned in place.
    """
    return ar


def clean_hotbins(ar, thresh=None, fscrunchfirst=None, onpulse=[]):
    """Replace hot bits with white noise.

        Inputs:
            ar: The archive to be cleaned
            thresh: The threshold (in number of sigmas) for a 
                bin to be removed.
            fscrunchfirst: Determine which bins to removed by
                looking at frequency scrunched data. Remove
                the hot bins in all frequency channels.
            onpulse: On-pulse regions to be ignored when computing
                profile statistics. A list of 2-tuples is expected.

        Outputs:
            None - The archive is cleaned in place.
    """
    if thresh is None:
        thresh = config.cfg.clean_hotbins_thresh
    if fscrunchfirst is None:
        fscrunchfirst = config.cfg.clean_hotbins_fscrunchfirst
    nbins = ar.get_nbin()
    indices = np.arange(nbins)
    offbins = np.ones(nbins, dtype='bool')
    offbin_indices = indices[offbins]
    for lobin, hibin in onpulse:
        offbins[lobin:hibin] = False
  
    if fscrunchfirst:
        reference = ar.clone()
        reference.set_dispersion_measure(0)
        reference.fscrunch()
    else:
        reference = ar
    nsub = reference.get_nsubint()
    for isub in np.arange(nsub):
        for ichan in np.arange(reference.get_nchan()):
            for ipol in np.arange(reference.get_npol()):
                prof = reference.get_Profile(int(isub), int(ipol), int(ichan))
                data = prof.get_amps()
                offdata = data[offbins]
                med = np.median(offdata)
                mad = np.median(np.abs(offdata-med))
                std = mad*1.4826 # This is the approximate relation between the
                                 # standard deviation and the median absolute
                                 # deviation (assuming normally distributed data).
                ioffbad = np.abs(offdata-med) > std*thresh
                ibad = offbin_indices[ioffbad]
                igood = offbin_indices[~ioffbad]
                nbad = np.sum(ioffbad)
                # Replace data in cleaned archive with noise
                if fscrunchfirst:
                    # We need to clean all frequency channels
                    for jchan in np.arange(ar.get_nchan()):
                        cleanedprof = ar.get_Profile(int(isub), int(ipol), int(jchan))
                        cleaneddata = cleanedprof.get_amps()
                        gooddata = cleaneddata[igood]
                        avg = gooddata.mean()
                        std = gooddata.std()
                        if std > 0:
                            noise = np.random.normal(avg, std, size=nbad).astype('float32')
                            cleaneddata[ibad] = noise
                else:
                    gooddata = data[igood]
                    avg = gooddata.mean()
                    std = gooddata.std()
                    if std > 0:
                        noise = np.random.normal(avg, std, size=nbad).astype('float32')
                        data[ibad] = noise


def surgical_scrub(ar, chanthresh=None, subintthresh=None, binthresh=None):
    """Surgically scrub RFI from the data.
        
        Input:
            ar: The archive to be cleaned.
        Outputs:
            None - The archive is cleaned in place.
    """
    import psrchive # Temporarily, because python bindings 
                    # are not available on all computers
    
    patient = ar.clone()
    patient.pscrunch()
    patient.remove_baseline()
    
    # Remove profile from dedispersed data
    patient.dedisperse()
    data = patient.get_data().squeeze()
    template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
    clean_utils.remove_profile_inplace(patient, template)
    # re-set DM to 0
    patient.dededisperse()
    
    # Get weights
    weights = patient.get_weights()
    # Get data (select first polarization - recall we already P-scrunched)
    data = patient.get_data()[:,0,:,:]
    data = clean_utils.apply_weights(data, weights)
   
    # Mask profiles where weight is 0
    mask_2d = np.bitwise_not(np.expand_dims(weights, 2).astype(bool))
    mask_3d = mask_2d.repeat(ar.get_nbin(), axis=2)
    data = np.ma.masked_array(data, mask=mask_3d)
    
    # RFI-ectomy must be recommended by average of tests
    avg_test_results = clean_utils.comprehensive_stats(data, axis=2, \
                                chanthresh=chanthresh, \
                                subintthresh=subintthresh, \
                                binthresh=binthresh)
    for (isub, ichan) in np.argwhere(avg_test_results>=1):
        # Be sure to set weights on the original archive, and
        # not the clone we've been working with.
        integ = ar.get_Integration(int(isub))
        integ.set_weight(int(ichan), 0.0)


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
    clean_utils.remove_profile_inplace(ar, template, None)

    ar.dededisperse()

    bad_chans = []
    bad_subints = []
    bad_pairs = []
    std_sub_vs_chan = np.std(data, axis=2)
    print(std_sub_vs_chan.shape)
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
            print(clean_utils.get_hot_bins(subint))
            plt.subplot(2,1,2)
            plt.plot(subint, 'r-')
            plt.title("Subint #%d" % isub)
            plt.figure()
            plt.subplot(2,1,1)
            plt.plot(std_sub_vs_chan[:, ichan], 'k-')
            chan = clean_utils.scale_subints(std_sub_vs_chan[:, ichan], \
                                                subintweights=subintweights)
            print(clean_utils.get_hot_bins(chan))
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


def deep_clean(toclean, chanthresh=None, subintthresh=None, binthresh=None):
    import psrchive # Temporarily, because python bindings 
                    # are not available on all computers
    
    if chanthresh is None:
        chanthresh = config.cfg.clean_chanthresh
    if subintthresh is None:
        subintthresh = config.cfg.clean_subintthresh
    if binthresh is None:
        binthresh = config.cfg.clean_binthresh
   
    ar = toclean.clone()

    ar.pscrunch()
    ar.remove_baseline()
    ar.dedisperse()

    # Remove profile
    data = ar.get_data().squeeze()
    template = np.apply_over_axes(np.sum, data, (0,1)).squeeze()
    clean_utils.remove_profile_inplace(ar, template, None)

    ar.dededisperse()

    # First clean channels
    chandata = clean_utils.get_chans(ar, remove_prof=True)
    chanweights = clean_utils.get_chan_weights(ar).astype(bool)
    chanmeans = clean_utils.scale_chans(chandata.mean(axis=1), chanweights=chanweights)
    chanmeans /= clean_utils.get_robust_std(chanmeans, chanweights)
    chanstds = clean_utils.scale_chans(chandata.std(axis=1), chanweights=chanweights)
    chanstds /= clean_utils.get_robust_std(chanstds, chanweights)

    badchans = np.concatenate((np.argwhere(np.abs(chanmeans) >= chanthresh), \
                                    np.argwhere(np.abs(chanstds) >= chanthresh)))
    badchans = np.unique(badchans)
    utils.print_info("Number of channels to be de-weighted: %d" % len(badchans), 2)
    for ichan in badchans:
        utils.print_info("De-weighting chan# %d" % ichan, 3)
        clean_utils.zero_weight_chan(ar, ichan)
        clean_utils.zero_weight_chan(toclean, ichan)

    # Next clean subints
    subintdata = clean_utils.get_subints(ar, remove_prof=True)
    subintweights = clean_utils.get_subint_weights(ar).astype(bool)
    subintmeans = clean_utils.scale_subints(subintdata.mean(axis=1), \
                                    subintweights=subintweights)
    subintmeans /= clean_utils.get_robust_std(subintmeans, subintweights)
    subintstds = clean_utils.scale_subints(subintdata.std(axis=1), \
                                    subintweights=subintweights)
    subintstds /= clean_utils.get_robust_std(subintstds, subintweights)

    badsubints = np.concatenate((np.argwhere(np.abs(subintmeans) >= subintthresh), \
                                    np.argwhere(np.abs(subintstds) >= subintthresh)))
    
    if config.debug.CLEAN:
        plt.subplots_adjust(hspace=0.4)
        chanax = plt.subplot(4,1,1)
        plt.plot(np.arange(len(chanmeans)), chanmeans, 'k-')
        plt.axhline(chanthresh, c='k', ls='--')
        plt.axhline(-chanthresh, c='k', ls='--')
        plt.xlabel('Channel Number', size='x-small')
        plt.ylabel('Average', size='x-small')
        
        plt.subplot(4,1,2, sharex=chanax)
        plt.plot(np.arange(len(chanstds)), chanstds, 'k-')
        plt.axhline(chanthresh, c='k', ls='--')
        plt.axhline(-chanthresh, c='k', ls='--')
        plt.xlabel('Channel Number', size='x-small')
        plt.ylabel('Standard Deviation', size='x-small')
        
        subintax = plt.subplot(4,1,3)
        plt.plot(np.arange(len(subintmeans)), subintmeans, 'k-')
        plt.axhline(subintthresh, c='k', ls='--')
        plt.axhline(-subintthresh, c='k', ls='--')
        plt.xlabel('Sub-int Number', size='x-small')
        plt.ylabel('Average', size='x-small')

        plt.subplot(4,1,4, sharex=subintax)
        plt.plot(np.arange(len(subintstds)), subintstds, 'k-')
        plt.axhline(subintthresh, c='k', ls='--')
        plt.axhline(-subintthresh, c='k', ls='--')
        plt.xlabel('Sub-int Number', size='x-small')
        plt.ylabel('Standard Deviation', size='x-small')
        plt.show()

    badsubints = np.unique(badsubints)
    utils.print_info("Number of sub-ints to be de-weighted: %d" % len(badsubints), 2)
    for isub in badsubints:
        utils.print_info("De-weighting subint# %d" % isub, 3)
        clean_utils.zero_weight_subint(ar, isub)
        clean_utils.zero_weight_subint(toclean, isub)
    
    # Re-dedisperse the data
    ar.dedisperse()

    # Now replace hot bins
    utils.print_info("Will find and clean 'hot' bins", 2)
    clean_utils.clean_hot_bins(toclean, thresh=binthresh)


def clean_simple(ar, timethresh=1.0, freqthresh=3.0):
    # Get stats for subints
    subint_stats = get_subint_stats(ar)
    
    # Get stats for chans
    chan_stats = get_chan_stats(ar)

    for isub in np.argwhere(subint_stats >= timethresh):
        print("De-weighting subint# %d" % isub)
        zero_weight_subint(ar, isub)
    for ichan in np.argwhere(chan_stats >= freqthresh):
        print("De-weighting chan# %d" % ichan)
        zero_weight_chan(ar, ichan)


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
                print("De-weighting subint# %d" % worst_subint)
                zero_weight_subint(ar, worst_subint)
            else:
                print("De-weighting chan# %d" % worst_chan)
                zero_weight_chan(ar, worst_chan)
        plot(ar, "bogus_%d" % ii)
        ii += 1


def prune_band(infn, response=None):
    """Prune the edges of the band. This is useful for
        removing channels where there is no response.
        The file is modified in-place. However, zero-weighting 
        is used for pruning, so the process is reversible.

        Inputs:
            infn: name of file to trim.
            response: A tuple specifying the range of frequencies 
                outside of which should be de-weighted.

        Outputs:
            None
    """
    if response is None:
        response = config.cfg.rcvr_response_lims

    if response is None:
        utils.print_info("No freq range specified for band pruning. Skipping...", 2)
    else:
        # Use absolute value in case band is flipped (BW<0)
        lofreq = infn['freq'] - np.abs(0.5*infn['bw'])
        hifreq = infn['freq'] + np.abs(0.5*infn['bw'])
        utils.print_info("Pruning frequency band to (%g-%g MHz)" % response, 2)
        pazcmd = 'paz -m %s ' % infn.fn
        runpaz = False # Only run paz if either of the following clauses are True
        if response[0] > lofreq:
            # Part of archive's low freqs are outside rcvr's response
            pazcmd += '-F "%f %f" ' % (lofreq, response[0])
            runpaz = True
        if response[1] < hifreq:
            # Part of archive's high freqs are outside rcvr's response
            pazcmd += '-F "%f %f" ' % (response[1], hifreq)
            runpaz = True
        if runpaz:        
            utils.execute(pazcmd)
        else:
            warnings.warn("Not pruning band edges! All data are " \
                            "within the receiver's response.", \
                            errors.CoastGuardWarning)


def trim_edge_channels(infn, nchan_to_trim=None, frac_to_trim=None):
    """Trim the edge channels of an input file to remove 
        band-pass roll-off and the effect of aliasing. 
        The file is modified in-place. However, zero-weighting 
        is used for trimming, so the process is reversible.

        Inputs:
            infn: name of file to trim.
            nchan_to_trim: The number of channels to de-weight at
                each edge of the band.
            frac_to_trim: The fraction of the edge of each bad to
                de-weight (a floating-point number between 0 and 0.5).

        Outputs:
            None
    """
    if nchan_to_trim is None:
        nchan_to_trim=config.cfg.nchan_to_trim
    if frac_to_trim is None:
        frac_to_trim=config.cfg.frac_to_trim

    if nchan_to_trim > 0:
        #utils.print_info("Trimming %d channels from subband edges " % \
        #                nchan_to_trim, 2)
        numchans = int(infn['nchan'])
        utils.execute('paz -m -Z "0 %d" -Z "%d %d" %s' % \
                    (nchan_to_trim-1, numchans-nchan_to_trim, numchans-1, infn.fn))
    if frac_to_trim > 0:
        #utils.print_info("Trimming %g %% from subband edges " % \
        #                frac_to_trim*100, 2)
        utils.execute('paz -m -E %f %s' % (frac_to_trim*100, infn.fn))


def remove_bad_subints(infn, badsubints=None, badsubint_intervals=None):
    """Zero-weights bad subints.
        The file is modified in-place. However, zero-weighting 
        is used for trimming, so the process is reversible.

        Note: Subints are indexed starting at 0.

        Inputs:
            infn: name of time to remove subints from.
            badchans: A list of subints to remove 
            badchan_intervals: A list of subint intervals 
                (inclusive) to remove
    
        Outputs:
            None
    """
    if badsubints is None:
        badsubints = config.cfg.badsubints
    if badsubint_intervals is None:
        badsubint_intervals = config.cfg.badsubint_intervals

    zaplets = []
    if badsubints:
        zaplets.append("-w '%s'" % " ".join(['%d' % ww for ww in badsubints]))
    if badsubint_intervals:
        zaplets.extend(["-W '%d %d'" % lohi for lohi in badsubint_intervals])

    if zaplets:
        utils.print_info("Removing bad subints.", 2)
        utils.execute("paz -m %s %s" % (" ".join(zaplets), infn.fn))


def remove_bad_channels(infn, badchans=None, badchan_intervals=None, 
                            badfreqs=None, badfreq_intervals=None):
    """Zero-weight bad channels and channels containing bad
        frequencies.
        The file is modified in-place. However, zero-weighting 
        is used for trimming, so the process is reversible.

        Note: Channels are indexed starting at 0.

        Inputs:
            infn: name of time to remove channels from.
            badchans: A list of channels to remove 
            badchan_intervals: A list of channel intervals 
                (inclusive) to remove
            badfreqs: A list of frequencies. The channels
                containing these frequencies will be removed.
            badfreq_intervals: A list of frequency ranges 
                to remove. The channels containing these
                frequencies will be removed.
    
        Outputs:
            None
    """
    if badchans is None:
        badchans = config.cfg.badchans
    if badchan_intervals is None:
        badchan_intervals = config.cfg.badchan_intervals
    if badfreqs is None:
        badfreqs = config.cfg.badfreqs
    if badfreq_intervals is None:
        badfreq_intervals = config.cfg.badfreq_intervals

    zaplets = []
    if badchans:
        zaplets.append("-z '%s'" % " ".join(['%d' % zz for zz in badchans]))
    if badchan_intervals:
        zaplets.extend(["-Z '%d %d'" % lohi for lohi in badchan_intervals])
    if badfreqs:
        zaplets.append("-f '%s'" % " ".join(['%f' % ff for ff in badfreqs]))
    if badfreq_intervals:
        zaplets.extend(["-F '%f %f'" % lohi for lohi in badfreq_intervals])

    if zaplets:
        utils.print_info("Removing bad channels.", 2)
        utils.execute("paz -m %s %s" % (" ".join(zaplets), infn.fn))


def clean_archive(inarf, outfn, clean_re=None, *args, **kwargs):
    import psrchive # Temporarily, because python bindings 
                    # are not available on all computers
    
    if clean_re is None:
        clean_re = config.cfg.clean_strategy
    try:
        outfn = utils.get_outfn(outfn, inarf)
        shutil.copy(inarf.fn, outfn)
        
        outarf = utils.ArchiveFile(outfn)
 
        trim_edge_channels(outarf)
        prune_band(outarf)
        remove_bad_channels(outarf)
        remove_bad_subints(outarf)
        
        matching_cleaners = [clnr for clnr in cleaners if clean_re and re.search(clean_re, clnr)]
        if len(matching_cleaners) == 1:
            ar = psrchive.Archive_load(outarf.fn)
            cleaner = eval(matching_cleaners[0])
            utils.print_info("Cleaning using '%s(...)'." % matching_cleaners[0], 2)
            cleaner(ar, *args, **kwargs)
            ar.unload(outfn)
        elif len(matching_cleaners) == 0:
            utils.print_info("No cleaning strategy selected. Skipping...", 2)
        else:
            raise errors.CleanError("Bad cleaner selection. " \
                                    "'%s' has %d matches." % \
                                    (clean_re, len(matching_cleaners)))
    except:
        # An error prevented cleaning from being successful
        # Remove the output file because it may confuse the user
        if os.path.exists(outfn):
            os.remove(outfn)
        raise
    return outarf


def main():
    print("")
    print("         clean.py")
    print("     Patrick  Lazarus")
    print("")
    file_list = args.files + args.from_glob
    to_exclude = args.excluded_files + args.excluded_by_glob
    to_clean = utils.exclude_files(file_list, to_exclude)
    print("Number of input files: %d" % len(to_clean))
    
    
    # Read configurations
    for infn in to_clean:
        inarf = utils.ArchiveFile(infn)
        config.cfg.load_configs_for_archive(inarf)
        outfn = utils.get_outfn(args.outfn, inarf)
        shutil.copy(inarf.fn, outfn)
        
        outarf = utils.ArchiveFile(outfn)
        ar = outarf.get_archive()
        
        try:
            for name, cfgstrs in args.cleaner_queue:
                # Set up the cleaner
                cleaner = cleaners.load_cleaner(name)
                for cfgstr in cfgstrs:
                    cleaner.parse_config_string(cfgstr)
                cleaner.run(ar)
        except:
            # An error prevented cleaning from being successful
            # Remove the output file because it may confuse the user
            #if os.path.exists(outfn):
            #    os.remove(outfn)
            raise
        finally:
            ar.unload(outfn)
            print("Cleaned archive: %s" % outfn)
        
    
class CleanerArguments(utils.DefaultArguments):
    def __init__(self, *args, **kwargs):
        super(CleanerArguments, self).__init__(add_help=False, \
                                                *args, **kwargs)
        self.add_argument('-h', '--help', nargs='?', dest='help_topic', \
                            metavar='CLEANER', \
                            action=self.HelpAction, type=str, \
                            help="Display this help message. If provided "
                                "with the name of a cleaner, display "
                                "its help.")

    class HelpAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            if values is None:
                parser.print_help()
            else:
                cleaner = cleaners.load_cleaner(values)
                print(cleaner.get_help(full=True))
            sys.exit(1)

    class ListCleanersAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            colour.cprint("Available Cleaners:", \
                            bold=True, underline=True) 
            for name in sorted(cleaners.registered_cleaners):
                cleaner = cleaners.load_cleaner(name)
                print(cleaner.get_help())
            sys.exit(1)

    class AppendCleanerAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            # Append the name of the cleaner and an empty list for
            # configuration strings
            getattr(namespace, self.dest).append((values, []))

    class ConfigureCleanerAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            # Append configuration string to most recently added
            # cleaner
            getattr(namespace, 'cleaner_queue')[-1][1].append(values)


if __name__=="__main__":
    parser = CleanerArguments(usage="%(prog)s [OPTIONS] FILES ...", \
                        description="Given a list of PSRCHIVE file names " \
                                    "clean RFI from each one.")
    parser.set_defaults(cleaner_queue=[])
    parser.add_argument('files', nargs='*', \
                        help="Files to clean.")
    parser.add_argument('-o', '--outname', dest='outfn', type=str, \
                        help="The output (reduced) file's name. " \
                            "(Default: '%%(name)s_%%(yyyymmdd)s_%%(secs)05d_cleaned.ar')", \
                        default="%(name)s_%(yyyymmdd)s_%(secs)05d_cleaned.ar")
    parser.add_file_selection_group()
    parser.add_argument('-F', '--cleaner', dest='cleaner_queue', \
                        action=parser.AppendCleanerAction, type=str, \
                        help="A string that matches one of the names of " \
                             "the available cleaning functions.")
    parser.add_argument('-c', dest='cfgstr', \
                        action=parser.ConfigureCleanerAction, type=str, \
                        help="A string of Cleaner configurations to " \
                            "apply to the cleaner most recently added " \
                            "to the queue.")
    parser.add_argument('--list-cleaners', nargs=0, \
                        action=parser.ListCleanersAction, \
                        help="List available cleaners and descriptions, then exit.")
    args = parser.parse_args()

    main()
