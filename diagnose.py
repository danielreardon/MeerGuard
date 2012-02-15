#!/usr/bin/env python

"""
Given a PSRCHIVE archive create diagnostic plots.

Patrick Lazarus, Dec. 12, 2011
"""

import sys
import os.path
import optparse

import numpy as np
import scipy.signal
import scipy.stats
import scipy.optimize as opt
import matplotlib.cm
import matplotlib.pyplot as plt

import psrchive

import clean_utils


func_info = {'std': ("Standard Deviation", np.std), \
             'mean': ("Average", np.mean), \
             'median': ("Median", np.median), \
             'ptp': ("Max - Min", np.ptp), \
             'normality': ("Test of Normality", \
                    lambda data, axis: scipy.stats.mstats.normaltest(data, axis=axis)[0])}


# Set plotting defaults
plt.rc(('xtick.major', 'ytick.major'), size=6)
plt.rc(('xtick.minor', 'ytick.minor'), size=3)
plt.rc('axes', labelsize='small')
plt.rc(('xtick', 'ytick'), labelsize='x-small')


def plot(ar, data, func_key='std', log=False, vmin=0, vmax=1):
    """Plot.

        Inputs:
            ar: The archive (this is used only to print text information).
            data: The archive data to make the plot for.
            func_key: A key indicating which function to plot.
                (Default: 'std')
            log: A boolean value. True to plot colours in log scale.
                (Default: Use linear scale)
            vmin: Fraction of colour range to show as black.
                (Default: 0.0)
            vmax: Fraction of colour range to show as white.
                (Default: 1.0_)

        Outputs:
            None
    """
    nsubs, nchans, nbins = data.shape
    
    title, func = func_info[func_key]
    
    sub_chan = func(data, axis=2)
    sub_phs = func(data, axis=1)
    chan_phs = func(data, axis=0)

    plt.figure(figsize=(11,8))
    
    # Create colour normaliser
    if log:
        normcls = matplotlib.colors.LogNorm
    else:
        normcls = matplotlib.colors.Normalize

    # Add text
    plt.figtext(0.02, 0.95, title, size='large', ha='left', va='center')
    plt.figtext(0.02, 0.925, os.path.split(ar.get_filename())[-1], \
                    size='x-small', ha='left', va='center')
    plt.figtext(0.02, 0.85, "Number of sub-ints: %d" % nsubs, \
                    size='small', ha='left', va='center')
    plt.figtext(0.02, 0.83, "Number of channels: %d" % nchans, \
                    size='small', ha='left', va='center')
    plt.figtext(0.02, 0.81, "Number of phase bins: %d" % nbins, \
                    size='small', ha='left', va='center')
    plt.figtext(0.02, 0.79, "Dedispered at: %.2f pc cm$^{\mathrm{-3}}$" % \
                    ar.get_dispersion_measure(), \
                    size='small', ha='left', va='center')
    plt.figtext(0.02, 0.77, "Centre Frequency: %.2f MHz" % \
                    ar.get_centre_frequency(), \
                    size='small', ha='left', va='center')
    plt.figtext(0.02, 0.75, "Bandwidth: %.2f MHz" % \
                    ar.get_bandwidth(), \
                    size='small', ha='left', va='center')
    # Plot profile
    prof_ax = plt.axes((0.05, 0.55, 0.4, 0.15))
    prof = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
    plt.plot(np.arange(nbins), prof, 'k-')
    plt.axis('tight')
    plt.xlabel("Phase Bins")
    plt.ylabel("Intensity")

    # Plot image data
    sub_chan_ax = plt.axes((0.05, 0.05, 0.45, 0.45))
    loval = np.min(sub_chan)
    ptp = np.ptp(sub_chan)
    norm = normcls(loval+ptp*vmin, loval+ptp*vmax, clip=True)
    plt.imshow(sub_chan, origin='bottom', aspect='auto', norm=norm, \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.xlabel("Channels")
    plt.ylabel("Sub-ints")
    
    sub_phs_ax = plt.axes((0.5, 0.05, 0.45, 0.45))
    loval = np.min(sub_phs)
    ptp = np.ptp(sub_phs)
    norm = normcls(loval+ptp*vmin, loval+ptp*vmax, clip=True)
    plt.imshow(sub_phs, origin='bottom', aspect='auto', norm=norm, \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.xlabel("Phase bins")
    plt.setp(sub_phs_ax.yaxis.get_ticklabels(), visible=False)
    
    chan_phs_ax = plt.axes((0.5, 0.5, 0.45, 0.45))
    loval = np.min(chan_phs)
    ptp = np.ptp(chan_phs)
    norm = normcls(loval+ptp*vmin, loval+ptp*vmax, clip=True)
    plt.imshow(chan_phs, origin='bottom', aspect='auto', norm=norm, \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.setp(chan_phs_ax.xaxis.get_ticklabels(), visible=False)
    plt.ylabel("Channels")
   
    # Link the axes
    prof_ax.callbacks.connect('xlim_changed', lambda ax: (chan_phs_ax.get_xlim()==ax.get_xlim() or \
                                                            chan_phs_ax.set_xlim(ax.get_xlim()), \
                                                          sub_phs_ax.get_xlim()==ax.get_xlim() or \
                                                            sub_phs_ax.set_xlim(ax.get_xlim())))
    sub_chan_ax.callbacks.connect('xlim_changed', lambda ax: (chan_phs_ax.get_ylim()==ax.get_xlim() or \
                                                                chan_phs_ax.set_ylim(ax.get_xlim())))
    sub_chan_ax.callbacks.connect('ylim_changed', lambda ax: (sub_phs_ax.get_ylim()==ax.get_ylim() or \
                                                                sub_phs_ax.set_ylim(ax.get_ylim())))
    sub_phs_ax.callbacks.connect('xlim_changed', lambda ax: (chan_phs_ax.get_xlim()==ax.get_xlim() or \
                                                                chan_phs_ax.set_xlim(ax.get_xlim()), \
                                                              prof_ax.get_xlim()==ax.get_xlim() or \
                                                                prof_ax.set_xlim(ax.get_xlim())))
    sub_phs_ax.callbacks.connect('ylim_changed', lambda ax: (sub_chan_ax.get_ylim()==ax.get_ylim() or \
                                                                sub_chan_ax.set_ylim(ax.get_ylim())))
    chan_phs_ax.callbacks.connect('xlim_changed', lambda ax: (sub_phs_ax.get_xlim()==ax.get_xlim() or \
                                                                sub_phs_ax.set_xlim(ax.get_xlim()), \
                                                              prof_ax.get_xlim()==ax.get_xlim() or \
                                                                prof_ax.set_xlim(ax.get_xlim())))
    chan_phs_ax.callbacks.connect('ylim_changed', lambda ax: (sub_chan_ax.get_xlim()==ax.get_ylim() or \
                                                                sub_chan_ax.set_xlim(ax.get_ylim())))


def plot_box(data, basename=None):
    """Plot.

        Inputs:
            data: The archive data to make the plot for.
            basename: The basename of the output plots.
                (Default: Do not output plots.)

        Outputs:
            None
    """
    nsubs, nchans, nbins = data.shape
    
    print data.shape
    print nsubs, nchans, nbins
    print np.std(data, axis=2).shape
    func = np.std
    title = "Standard Deviation"
    
    sub_chan = func(data, axis=2)
    sub_phs = func(data, axis=1)
    chan_phs = func(data, axis=0)

    plt.figure(figsize=(8,8))
    plt.axes((.375, .5, .25, .25))
    plt.imshow(sub_phs, origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.xticks([])
    plt.yticks([])
    plt.axes((.375, .75, .25, .25))
    plt.imshow(chan_phs, origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.xticks([])
    plt.yticks([])
    plt.axes((.625, .5, .25, .25))
    plt.imshow(sub_chan, origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.xticks([])
    plt.yticks([])
    plt.axes((.125, .5, .25, .25))
    plt.imshow(np.fliplr(sub_chan), origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.xticks([])
    plt.yticks([])
    plt.axes((.375, .25, .25, .25))
    plt.imshow(np.flipud(chan_phs), origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.xticks([])
    plt.yticks([])
    plt.axes((.375, 0, .25, .25))
    plt.imshow(np.flipud(sub_phs), origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.xticks([])
    plt.yticks([])
    if basename is not None:
        plt.savefig(basename+"_datacube.png", dpb=300, papertype="a4", format="png")
        plt.savefig(basename+"_datacube.pdf", dpb=300, papertype="a4", format="pdf")
    plt.show()


def foo():
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
    ar.pscrunch()
   
    if options.remove_baseline:
        print "Removing baseline..."
        ar.remove_baseline()
    if options.dedisperse:
        print "Dedispersing..."
        ar.dedisperse()
    else:
        ar.set_dispersion_measure(0)
        ar.dedisperse()
    data = ar.get_data().squeeze()
    if options.remove_profile:
        print "Removing profile..."
        template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
        data = clean_utils.remove_profile(data, ar.get_nsubint(), ar.get_nchan(), \
                                            template, options.nthreads)
    for func_key in options.funcs_to_plot:
        plot(ar, data, func_key, log=options.log_colours, \
            vmin=options.black_level, vmax=options.white_level)
    plt.gcf().canvas.mpl_connect('key_press_event', \
                lambda ev: (ev.key in ('q', 'Q')) and plt.close())
    plt.show()


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-d', '--dedisperse', dest='dedisperse', \
        action='store_true', default=False, \
        help="Dedisperse archive before producing diagnostics. " \
             "(Default: Keep archive in dispersed (DM=0) state)")
    parser.add_option('-b', '--remove-baseline', dest='remove_baseline', \
        action='store_true', default=False, \
        help="Remove baselines from all profiles using archive's " \
                "'remove_baseline()' method. (Default: Do not " \
                "remove baseline)")
    parser.add_option('-p', '--remove-profile', dest='remove_profile', \
        action='store_true', default=False, \
        help="Remove profile. (Default: Do not remove profile)")
    parser.add_option('--num-subbands', dest='num_subbands', \
        type='int', default=None, \
        help="The number of subbands. This is used for scaling channels. " \
             "(Default: Do not scale channels)")
    parser.add_option('-n', '--num-threads', dest='nthreads', \
        type='int', default=None, \
        help="The number of threads to use when removing profiles. " \
                "(Default: Use as many threads as there are CPUs)")
    parser.add_option('-f', '--func-to-plot', dest='funcs_to_plot', \
        action='append', default=[], \
        help="Plot the given function. Possible choices are: " + \
             "; ".join(["%s: %s" % (key, info[0]) for key, info \
                                            in func_info.iteritems()]))
    parser.add_option('--log-colours', dest='log_colours', \
        action='store_true', default=False, \
        help="Plot colours on a logarithmic scale. (Default: use linear scale)")
    parser.add_option('--white-level', dest='white_level', \
        type='float', default=1.0, \
        help="Values whose normalised colour is larger than this value " \
                "(on a 0-1 scale) will be shown as white. (Default: 1.0)")
    parser.add_option('--black-level', dest='black_level', \
        type='float', default=0.0, \
        help="Values whose normalised clour is smaller than this value " \
                "(on a 0-1 scale) will be shown as black. (Default: 0.0)")
    options, args = parser.parse_args()
    if not options.funcs_to_plot:
        options.funcs_to_plot = ['std']
    main()
