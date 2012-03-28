#!/usr/bin/env python

"""
Given a PSRCHIVE archive create diagnostic plots.

Patrick Lazarus, Dec. 12, 2011
"""

import re
import sys
import os.path
import optparse

import numpy as np
import scipy.signal
import scipy.stats
import scipy.optimize as opt
import matplotlib
import matplotlib.pyplot as plt

import psrchive

import utils
import clean_utils
import config
import errors

func_info = {'std': ("Standard Deviation", np.std), \
             'mean': ("Average", np.mean), \
             'median': ("Median", np.median), \
             'ptp': ("Max - Min", np.ptp), \
             'normality': ("Omnibus test of Normality", \
                    lambda data, axis: scipy.stats.mstats.normaltest(data, axis=axis)[0]), \
             'periodicity': ("Periodic Signal Strength", \
                    lambda data, axis: np.max(np.abs(np.fft.rfft(\
                                data-np.expand_dims(data.mean(axis=axis), axis=axis), \
                                    axis=axis)), axis=axis)), \
             'mad': ("Median Absolute Deviation", \
                    lambda data, axis: np.median(np.abs(data - \
                                np.expand_dims(np.median(data, axis=axis), axis=axis)), axis=axis))}

diagnostics = ['SlicerDiagnosticFigure', 'ComprehensiveDiagnosticFigure']

# Set plotting defaults
plt.rc(('xtick.major', 'ytick.major'), size=6)
plt.rc(('xtick.minor', 'ytick.minor'), size=3)
plt.rc('axes', labelsize='small')
plt.rc(('xtick', 'ytick'), labelsize='x-small')

class SlicerDiagnosticFigure(matplotlib.figure.Figure):
    def __init__(self, ar, data, func_key, log=None, vmin=None, vmax=None, \
                    *args, **kwargs):
        super(SlicerDiagnosticFigure, self).__init__(*args, **kwargs)
        self.ar = ar
        self.data = data
        if log is None:
            self.log = config.cfg.logcolours
        else:
            self.log = log
        if vmin is None:
            self.vmin = config.cfg.vmin
        else:
            self.vmin = vmin
        if vmax is None:
            self.vmax = config.cfg.vmax
        else:
            self.vmax = vmax

        self.nsubs, self.nchans, self.nbins = self.data.shape
        self.title, self.func = func_info[func_key]
        
        # Current slices
        self.chan = None
        self.subint = None

        self.hcrosshairs = []
        self.vcrosshairs = []

        # Prep image data
        self.imdata = self.func(self.data, axis=2)
        
        # Get weights
        self.weights = self.ar.get_weights()

    def connect_event_triggers(self):
        # Connect trigger
        self.canvas.mpl_connect('button_press_event', self.update_slice)

    def plot(self):
        utils.print_info("Plotting %s..." % self.title.lower(), 2)
        
        self.clear() # Clear the figure

        # Add text
        self.text(0.02, 0.95, self.ar.get_source(), size='large', ha='left', va='center')
        self.text(0.02, 0.925, os.path.split(self.ar.get_filename())[-1], \
                        size='x-small', ha='left', va='center')
        self.slice_info = self.text(0.725, 0.84, "Click on image to view slice", \
                        size='small', ha='left', va='center')

        # Make axes
        self.dspec_ax = self.add_axes((0.1,0.1,0.6,0.6))
        self.prof_ax = self.add_axes((0.725,0.7425,0.2,0.075))
        self.hsum_ax = self.add_axes((0.1,0.7,0.6,0.075), sharex=self.dspec_ax)
        self.vsum_ax = self.add_axes((0.7,0.1,0.075,0.6), sharey=self.dspec_ax)
        self.hslice_ax = self.add_axes((0.1,0.785,0.6,0.075), sharex=self.dspec_ax)
        self.vslice_ax = self.add_axes((0.785,0.1,0.075,0.6), sharey=self.dspec_ax)
        self.cb_ax = self.add_axes((0.88,0.1,0.025,0.6), frameon=False)

        # Turn off unused labels
        plt.setp(self.hsum_ax.xaxis.get_ticklabels(), visible=False)
        plt.setp(self.vsum_ax.yaxis.get_ticklabels(), visible=False)
        plt.setp(self.hslice_ax.xaxis.get_ticklabels(), visible=False)
        plt.setp(self.vslice_ax.yaxis.get_ticklabels(), visible=False)

        # Rotate tick labels
        plt.setp(self.vsum_ax.xaxis.get_ticklabels(), rotation=45)
        plt.setp(self.vslice_ax.xaxis.get_ticklabels(), rotation=45)

        # Shift tick location
        self.prof_ax.yaxis.set_ticks_position('right')
        self.prof_ax.yaxis.set_label_position('right')

        # Label axes
        self.prof_ax.set_xlabel("Phase bin")
        self.prof_ax.set_ylabel("Intensity")

        # Create colour normaliser
        if self.log:
            normcls = matplotlib.colors.LogNorm
        else:
            normcls = matplotlib.colors.Normalize
        
        # Sub-ints vs. Channels
        loval = np.min(self.imdata)
        ptp = np.ptp(self.imdata)
        norm = normcls(loval+ptp*self.vmin, loval+ptp*self.vmax, clip=True)
        mask = (self.weights==0)
        masked_image = np.ma.masked_array(self.imdata, mask=mask)
        # In the following subtract 0.5 from extent values so indices refer to 
        # bin centres
        im = self.dspec_ax.imshow(masked_image, origin='bottom', zorder=1, \
                aspect='auto', norm=norm, extent=(-0.5,self.nchans-0.5,-0.5,self.nsubs-0.5), \
                cmap=matplotlib.cm.GnBu, interpolation='nearest')

        cb = self.colorbar(im, cax=self.cb_ax, orientation='vertical')
        cb.set_label(self.title, size='x-small', rotation=90)
        plt.setp(self.cb_ax.yaxis.get_ticklabels(), size='x-small', rotation=90)

        # Show what regions are masked
        maskim = np.zeros((self.nsubs, self.nchans, 4))
        for ichan,isub in np.argwhere(mask):
            maskim[ichan,isub,:] = (1,0,0,0.4)
        self.dspec_ax.imshow(maskim, origin='bottom', \
                aspect='auto', norm=norm, extent=(-0.5,self.nchans-0.5,-0.5,self.nsubs-0.5), \
                interpolation='nearest')
        self.dspec_ax.format_coord = lambda x,y: "Chan: %d, Sub-int: %d, Value: %s" % \
                (x+0.5, y+0.5, (self.weights[int(y+0.5),int(x+0.5)] and \
                                    '%g' % self.imdata[int(y+0.5),int(x+0.5)]) or 'masked')
        self.dspec_ax.set_xlabel("Channel")
        self.dspec_ax.set_ylabel("Sub-integration")
       
        # Sum of rows
        mask = (self.weights.sum(axis=0)==0)
        toplot = np.ma.masked_array(masked_image.mean(axis=0), mask=mask)
        indices = np.repeat(np.arange(-0.5, self.nchans+0.5, 1),2)[1:-1]
        invertedmask = np.ma.masked_array(np.ones(self.nchans), mask=np.bitwise_not(mask))
        self.hsum_ax.plot(indices, np.repeat(toplot,2), 'k-')
        segments = np.ma.flatnotmasked_contiguous(invertedmask)
        if segments:
            for segment in segments:
                self.hsum_ax.axvspan(segment.start-0.5, segment.stop+0.5, fc='r', lw=0, alpha=0.2)

        # Sum of cols
        mask = (self.weights.sum(axis=1)==0)
        toplot = np.ma.masked_array(masked_image.mean(axis=1), mask=mask)
        indices = np.repeat(np.arange(-0.5, self.nsubs+0.5, 1),2)[1:-1]
        invertedmask = np.ma.masked_array(np.ones(self.nsubs), mask=np.bitwise_not(mask))
        self.vsum_ax.plot(np.repeat(toplot,2), indices, 'k-')
        segments = np.ma.flatnotmasked_contiguous(invertedmask)
        if segments:
            for segment in segments:
                self.vsum_ax.axhspan(segment.start-0.5, segment.stop+0.5, fc='r', lw=0, alpha=0.2)
        
        # In the following subtract 0.5 from extent values so indices refer to 
        # bin centres
        self.dspec_ax.axis([-0.5, self.nchans-0.5, -0.5, self.nsubs-0.5])
        self.prof_ax.set_xlim(0, self.nbins)

    def update_slice(self, event):
        if event.inaxes==self.dspec_ax and \
                (event.button==2 or (event.key=='shift' and event.button==1)):
            self.chan = int(np.round(event.xdata))
            self.subint = int(np.round(event.ydata))
            self.slice_info.set_text("Slicing along Chan: %d, Subint: %d" % \
                        (self.chan, self.subint))

            imaxlims = self.dspec_ax.axis()
            profxlims = self.prof_ax.get_xlim()

            self.hslice_ax.cla()
            mask = (self.weights[self.subint, :]==0)
            toplot = np.ma.masked_array(self.imdata[self.subint, :], mask=mask)
            indices = np.repeat(np.arange(-0.5, self.nchans+0.5, 1),2)[1:-1]
            invertedmask = np.ma.masked_array(np.ones(self.nchans), mask=np.bitwise_not(mask))
            self.hslice_ax.plot(indices, np.repeat(toplot,2), 'k-')
            segments = np.ma.flatnotmasked_contiguous(invertedmask)
            if segments:
                for segment in segments:
                    self.hslice_ax.axvspan(segment.start-0.5, segment.stop+0.5, fc='r', lw=0, alpha=0.2)

            # Plot median and MAD
            median = np.median(toplot)
            mad = np.median(np.abs(toplot-median))
            self.hslice_ax.axhline(median, c='k', ls='-')
            self.hslice_ax.axhline(median+mad, c='k', ls='--')
            self.hslice_ax.axhline(median-mad, c='k', ls='--')

            self.vslice_ax.cla()
            mask = (self.weights[:, self.chan]==0)
            toplot = np.ma.masked_array(self.imdata[:,self.chan], mask=mask)
            indices = np.repeat(np.arange(-0.5, self.nsubs+0.5, 1),2)[1:-1]
            invertedmask = np.ma.masked_array(np.ones(self.nsubs), mask=np.bitwise_not(mask))
            self.vslice_ax.plot(np.repeat(toplot,2), indices, 'k-')
            segments = np.ma.flatnotmasked_contiguous(invertedmask)
            if segments:
                for segment in np.ma.flatnotmasked_contiguous(invertedmask):
                    self.vslice_ax.axhspan(segment.start-0.5, segment.stop+0.5, fc='r', lw=0, alpha=0.2)
       
            # Plot median and MAD
            median = np.median(toplot)
            mad = np.median(np.abs(toplot-median))
            self.vslice_ax.axvline(median, c='k', ls='-')
            self.vslice_ax.axvline(median+mad, c='k', ls='--')
            self.vslice_ax.axvline(median-mad, c='k', ls='--')

            self.prof_ax.cla()
            self.prof_ax.plot(np.arange(self.nbins), self.data[self.subint, self.chan], 'k-')

            self.dspec_ax.axis(imaxlims)
            self.prof_ax.set_xlim(profxlims)
            
            # Draw/move crosshairs
            if self.hcrosshairs:
                for ch in self.hcrosshairs:
                    ch.set_ydata((self.subint, self.subint))
            else:
                self.hcrosshairs.append(self.dspec_ax.axhline(self.subint, \
                                                    c='k', ls='-', alpha=0.5))
                self.hcrosshairs.append(self.vsum_ax.axhline(self.subint, \
                                                    c='k', ls='-', alpha=0.5))
            
            if self.vcrosshairs:
                for ch in self.vcrosshairs:
                    ch.set_xdata((self.chan, self.chan))
            else:
                self.vcrosshairs.append(self.dspec_ax.axvline(self.chan, \
                                                    c='k', ls='-', alpha=0.5))
                self.vcrosshairs.append(self.hsum_ax.axvline(self.chan, \
                                                    c='k', ls='-', alpha=0.5))
            
            self.hslice_ax.axvline(self.chan, c='k', ls='-', alpha=0.5)
            self.vslice_ax.axhline(self.subint, c='k', ls='-', alpha=0.5)

            # Label axes
            self.prof_ax.set_xlabel("Phase bin")
            self.prof_ax.set_ylabel("Intensity")

            # Turn off unused labels
            plt.setp(self.hslice_ax.xaxis.get_ticklabels(), visible=False)
            plt.setp(self.vslice_ax.yaxis.get_ticklabels(), visible=False)
      
            # Rotate tick labels
            plt.setp(self.vsum_ax.xaxis.get_ticklabels(), rotation=45)
            plt.setp(self.vslice_ax.xaxis.get_ticklabels(), rotation=45)

            # Shift tick location
            self.prof_ax.yaxis.set_ticks_position('right')
            self.prof_ax.yaxis.set_label_position('right')

            self.canvas.draw()


class ComprehensiveDiagnosticFigure(matplotlib.figure.Figure):
    def __init__(self, ar, data, func_key, log=None, vmin=None, vmax=None, \
                    *args, **kwargs):
        super(ComprehensiveDiagnosticFigure, self).__init__(*args, **kwargs)
        self.ar = ar
        self.data = data
        if log is None:
            self.log = config.cfg.logcolours
        else:
            self.log = log
        if vmin is None:
            self.vmin = config.cfg.vmin
        else:
            self.vmin = vmin
        if vmax is None:
            self.vmax = config.cfg.vmax
        else:
            self.vmax = vmax

        self.nsubs, self.nchans, self.nbins = self.data.shape
        self.sub_lims = (0, self.nsubs)
        self.chan_lims = (0, self.nchans)
        self.bin_lims = (0, self.nbins)

        self.title, self.func = func_info[func_key]
        
        utils.print_info("Plotting %s..." % self.title.lower(), 2)
        
        # Add text
        self.text(0.02, 0.95, ar.get_source(), size='large', ha='left', va='center')
        self.text(0.02, 0.925, os.path.split(ar.get_filename())[-1], \
                        size='x-small', ha='left', va='center')
        self.text(0.02, 0.87, "Plotting: %s" % self.title.lower(), \
                        size='small', ha='left', va='center')
        self.text(0.02, 0.85, "Number of sub-ints: %d" % self.nsubs, \
                        size='small', ha='left', va='center')
        self.text(0.02, 0.83, "Number of channels: %d" % self.nchans, \
                        size='small', ha='left', va='center')
        self.text(0.02, 0.81, "Number of phase bins: %d" % self.nbins, \
                        size='small', ha='left', va='center')
        self.text(0.02, 0.79, "Dedispersed at: %.2f pc cm$^{\mathrm{-3}}$" % \
                        self.ar.get_dispersion_measure(), \
                        size='small', ha='left', va='center')
        self.text(0.02, 0.77, "Centre Frequency: %.2f MHz" % \
                        self.ar.get_centre_frequency(), \
                        size='small', ha='left', va='center')
        self.text(0.02, 0.75, "Bandwidth: %.2f MHz" % \
                        self.ar.get_bandwidth(), \
                        size='small', ha='left', va='center')
        
        # Make axes
        self.prof_ax = self.add_axes((0.05, 0.55, 0.4, 0.15)) 
        self.sub_chan_ax = self.add_axes((0.05, 0.05, 0.45, 0.45))
        self.sub_phs_ax = self.add_axes((0.5, 0.05, 0.45, 0.45))
        self.chan_phs_ax = self.add_axes((0.5, 0.5, 0.45, 0.45))
        
    def connect_event_triggers(self):
        self.prof_xlim_evid = self.prof_ax.callbacks.connect('xlim_changed', \
                                    lambda ax: self.change_bin_lims(*ax.get_xlim()))
        self.sub_chan_xlim_evid = self.sub_chan_ax.callbacks.connect('xlim_changed', \
                                    lambda ax: self.change_chan_lims(*ax.get_xlim()))
        self.sub_chan_ylim_evid = self.sub_chan_ax.callbacks.connect('ylim_changed', \
                                    lambda ax: self.change_sub_lims(*ax.get_ylim()))
        self.sub_phs_xlim_evid = self.sub_phs_ax.callbacks.connect('xlim_changed', \
                                    lambda ax: self.change_bin_lims(*ax.get_xlim()))
        self.sub_phs_ylim_evid = self.sub_phs_ax.callbacks.connect('ylim_changed', \
                                    lambda ax: self.change_sub_lims(*ax.get_ylim()))
        self.chan_phs_xlim_evid = self.chan_phs_ax.callbacks.connect('xlim_changed', \
                                    lambda ax: self.change_bin_lims(*ax.get_xlim()))
        self.chan_phs_ylim_evid = self.chan_phs_ax.callbacks.connect('ylim_changed', \
                                    lambda ax: self.change_chan_lims(*ax.get_ylim()))

    def disconnect_event_triggers(self):
        self.prof_ax.callbacks.disconnect(self.prof_xlim_evid)
        self.sub_chan_ax.callbacks.disconnect(self.sub_chan_xlim_evid)
        self.sub_chan_ax.callbacks.disconnect(self.sub_chan_ylim_evid)
        self.sub_phs_ax.callbacks.disconnect(self.sub_phs_xlim_evid)
        self.sub_phs_ax.callbacks.disconnect(self.sub_phs_ylim_evid)
        self.chan_phs_ax.callbacks.disconnect(self.chan_phs_xlim_evid)
        self.chan_phs_ax.callbacks.disconnect(self.chan_phs_ylim_evid)

    def change_bin_lims(self, lobin, hibin):
        #print "Changing bin lims: (%f, %f)" % (lobin, hibin)
        lobin = int(np.round(lobin).clip(0, self.nbins))
        hibin = int(np.round(hibin).clip(0, self.nbins))
        if self.bin_lims != (lobin, hibin):
            self.bin_lims = (lobin, hibin)
            self.plot()

    def change_chan_lims(self, lochan, hichan):
        #print "Changing chan lims: (%f, %f)" % (lochan, hichan)
        lochan = int(np.round(lochan).clip(0, self.nchans))
        hichan = int(np.round(hichan).clip(0, self.nchans))
        if self.chan_lims != (lochan, hichan):
            self.chan_lims = (lochan, hichan)
            self.plot()

    def change_sub_lims(self, losub, hisub):
        #print "Changing sub lims: (%f, %f)" % (losub, hisub)
        losub = int(np.round(losub).clip(0, self.nsubs))
        hisub = int(np.round(hisub).clip(0, self.nsubs))
        if self.sub_lims != (losub, hisub):
            self.sub_lims = (losub, hisub)
            self.plot()

    def plot(self):
        self.disconnect_event_triggers()
        #print self.sub_lims, self.chan_lims, self.bin_lims
        subset = self.data[slice(*self.sub_lims), \
                            slice(*self.chan_lims), \
                            slice(*self.bin_lims)]
        prof = np.apply_over_axes(np.sum, subset, (0, 1)).squeeze()
        sub_chan = self.func(subset, axis=2)
        sub_phs = self.func(subset, axis=1)
        chan_phs = self.func(subset, axis=0)
      
        # The following plotting code is meant to test some new
        # peak finding function added to scipy.signal in early 2012
        #
        # plt.subplot(3,1,1)
        # plt.plot(sub_chan.sum(axis=0), 'k-')
        # peaks = scipy.signal.find_peaks_cwt(sub_chan.sum(axis=0), [1,2,3,4])
        # plt.plot(peaks, sub_chan.sum(axis=0)[peaks], 'r.')
        # plt.subplot(3,1,2)
        # plt.plot(sub_phs.sum(axis=0), 'k-')
        # peaks = scipy.signal.find_peaks_cwt(sub_phs.sum(axis=0), [1,2,3,4])
        # plt.plot(peaks, sub_phs.sum(axis=0)[peaks], 'r.')
        # plt.subplot(3,1,3)
        # plt.plot(chan_phs.sum(axis=0), 'k-')
        # peaks = scipy.signal.find_peaks_cwt(chan_phs.sum(axis=0), [1,2,3,4])
        # plt.plot(peaks, chan_phs.sum(axis=0)[peaks], 'r.')
        # plt.show()

        # Create colour normaliser
        if self.log:
            normcls = matplotlib.colors.LogNorm
        else:
            normcls = matplotlib.colors.Normalize
        
        # Profile
        self.prof_ax.cla()
        self.prof_ax.plot(np.arange(*self.bin_lims), prof, 'k-')
        self.prof_ax.relim()
        self.prof_ax.autoscale_view(tight=True)
        self.prof_ax.set_xlabel("Phase Bins")
        self.prof_ax.set_ylabel("Intensity")

        # Sub-ints vs. Channels
        loval = np.min(sub_chan)
        ptp = np.ptp(sub_chan)
        norm = normcls(loval+ptp*self.vmin, loval+ptp*self.vmax, clip=True)
        self.sub_chan_ax.cla()
        self.sub_chan_ax.imshow(sub_chan, origin='bottom', aspect='auto', norm=norm, \
                    extent=self.chan_lims+self.sub_lims, \
                    cmap=matplotlib.cm.gist_heat, interpolation='nearest')
        self.sub_chan_ax.format_coord = lambda x,y: "Chan: %d, Sub-int: %d, Value: %g" % \
                                            (x,y,sub_chan[int(y)-self.sub_lims[0],
                                                            int(x)-self.chan_lims[0]])
        self.sub_chan_ax.set_xlabel("Channels")
        self.sub_chan_ax.set_ylabel("Sub-ints")
        
        # Sub-ints vs. Phase
        loval = np.min(sub_phs)
        ptp = np.ptp(sub_phs)
        norm = normcls(loval+ptp*self.vmin, loval+ptp*self.vmax, clip=True)
        self.sub_phs_ax.cla()
        self.sub_phs_ax.imshow(sub_phs, origin='bottom', aspect='auto', norm=norm, \
                    extent=self.bin_lims+self.sub_lims, \
                    cmap=matplotlib.cm.gist_heat, interpolation='nearest')
        self.sub_phs_ax.format_coord = lambda x,y: "Bin: %d, Sub-int: %d, Value: %g" % \
                                            (x,y,sub_phs[int(y)-self.sub_lims[0],
                                                            int(x)-self.bin_lims[0]])
        self.sub_phs_ax.set_xlabel("Phase bins")
        plt.setp(self.sub_phs_ax.yaxis.get_ticklabels(), visible=False)

        # Channels vs. Phase
        loval = np.min(chan_phs)
        ptp = np.ptp(chan_phs)
        norm = normcls(loval+ptp*self.vmin, loval+ptp*self.vmax, clip=True)
        self.chan_phs_ax.cla()
        self.chan_phs_ax.imshow(chan_phs, origin='bottom', aspect='auto', norm=norm, \
                    extent=self.bin_lims+self.chan_lims, \
                    cmap=matplotlib.cm.gist_heat, interpolation='nearest')
        self.chan_phs_ax.format_coord = lambda x,y: "Bin: %d, Chan: %d, Value: %g" % \
                                            (x,y,chan_phs[int(y)-self.chan_lims[0],
                                                            int(x)-self.bin_lims[0]])
        plt.setp(self.chan_phs_ax.xaxis.get_ticklabels(), visible=False)
        self.chan_phs_ax.set_ylabel("Channels")
        
        self.canvas.draw()
        self.connect_event_triggers()


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


def make_diagnostic_figure(arf, func_re, diag_re='comprehensive', \
                            rmbaseline=None, dedisp=None, \
                            rmprof=None, centre_prof=None, **kwargs):
    if rmbaseline is None:
        rmbaseline = config.cfg.rmbaseline
    if dedisp is None:
        dedisp = config.cfg.dedisp
    if rmprof is None:
        rmprof = config.cfg.rmprof
    if centre_prof is None:
        centre_prof = config.cfg.centre_prof

    matching_func_keys = [fk for fk in func_info.keys() if re.search(func_re, fk)]
    if len(matching_func_keys) == 1:
        func_key = matching_func_keys[0]
        utils.print_info("Using %s as diagnostic function." % func_info[func_key][0], 2)
    else:
        raise errors.DiagnosticError("Bad diagnostic function selection. " \
                                     "'%s' has %d matches." % \
                                     (func_re, len(matching_func_keys)))

    matching_diagnostics = [diag for diag in diagnostics \
                                if re.search(diag_re, diag, re.IGNORECASE)]
    if len(matching_diagnostics) == 1:
        diagnostic_class = eval(matching_diagnostics[0])
        utils.print_info("Generating diagnostic figure of type %s." % \
                                    matching_diagnostics[0], 2)
    else:
        raise errors.DiagnosticError("Bad diagnostic figure type selection. " \
                                     "'%s' has %d matches." % \
                                     (diag_re, len(matching_diagnostics)))

    ar = psrchive.Archive_load(arf.fn)
    ar.pscrunch()
   
    if centre_prof:
        utils.print_info("Centering profile...", 2)
        ar.centre_max_bin()
    if rmbaseline:
        utils.print_info("Removing baseline...", 2)
        ar.remove_baseline()
    if rmprof:
        ar.dedisperse()
        data = ar.get_data().squeeze()
        utils.print_info("Removing profile...", 2)
        template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
        clean_utils.remove_profile_inplace(ar, template)
    
    if dedisp:
        utils.print_info("Dedispersing...", 2)
        ar.dedisperse()
    else:
        utils.print_info("Dedispersing to DM=0...", 2)
        ar.set_dispersion_measure(0)
        ar.dedisperse()
    
    data = ar.get_data().squeeze()
    data = clean_utils.apply_weights(data, ar.get_weights())
    fig = plt.figure(figsize=(11,8), FigureClass=diagnostic_class, 
                ar=ar, data=data, func_key=func_key, **kwargs)
    fig.connect_event_triggers()
    # Plot data
    fig.plot()
    return fig


def main():
    inarf = utils.ArchiveFile(args[0])
    fig = make_diagnostic_figure(inarf, \
                func_re=options.func_to_plot, diag_re=options.diagnostic)
    if options.savefn:
        savefn = utils.get_outfn(options.savefn, inarf) 
        plt.savefig(savefn, dpi=600)
    if options.interactive:
        fig.canvas.mpl_connect('key_press_event', \
                lambda ev: (ev.key in ('q', 'Q')) and plt.close(fig))
        plt.show()


if __name__ == '__main__':
    parser = utils.DefaultOptions()
    parser.add_option('-D', '--dedisperse', dest='dedisp', \
        action='callback', callback=parser.set_override_config, \
        help="Dedisperse archive before producing diagnostics. " \
             "(Default: %s)" % ((config.cfg.dedisp and "this is the default") or "use DM=0"))
    parser.add_option('--no-dedisperse', dest='dedisp', \
        action='callback', callback=parser.unset_override_config, \
        help="Dedisperse archive to DM=0 before producing diagnostics. " \
             "(Default: %s)" % ((not config.cfg.dedisp and "this is the default") or "use DM in emphemeris"))
    parser.add_option('-b', '--remove-baseline', dest='rmbaseline', \
        action='callback', callback=parser.set_override_config, \
        help="Remove baselines from all profiles using archive's " \
                "'remove_baseline()' method. (Default: %s)" % \
                ((config.cfg.rmbaseline and "this is the default") or "do not remove baselines"))
    parser.add_option('--no-remove-baseline', dest='rmbaseline', \
        action='callback', callback=parser.unset_override_config, \
        help="Do not perform any baseline removal. (Default: %s)" % \
                ((not config.cfg.rmbaseline and "this is the default") or "remove baselines"))
    parser.add_option('-r', '--remove-profile', dest='rmprof', \
        action='callback', callback=parser.set_override_config, \
        help="Remove profile. (Default: %s)" % \
                ((config.cfg.rmprof and "this is the default") or "leave profile"))
    parser.add_option('--no-remove-profile', dest='rmprof', \
        action='callback', callback=parser.unset_override_config, \
        help="Do not subtract profile. (Default: %s)" % \
                ((not config.cfg.rmprof and "this is the default") or "remove profile"))
    parser.add_option('--centre-profile', dest='centre_prof', \
        action='callback', callback=parser.set_override_config, \
        help="Centre profile. (Default: %s)" % \
                ((config.cfg.centre_prof and "this is the default") or "do not rotate profile"))
    parser.add_option('--no-centre-profile', dest='centre_prof', \
        action='callback', callback=parser.unset_override_config, \
        help="Do not rotate profile. (Default: %s)" % \
                ((not config.cfg.centre_prof and "this is the default") or "rotate profile"))
    parser.add_option('--num-threads', dest='nthreads', action='callback', \
        callback=parser.override_config, type='int', \
        help="The number of threads to use when removing profiles. " \
                "(Default: %d)" % config.cfg.nthreads)
    parser.add_option('-f', '--func-to-plot', dest='func_to_plot', \
        default='std', action='store', \
        help="Function to plot. Possible choices are: %s. " \
             "(Default: std)" % \
             "; ".join(["%s: '%s'" % (key, info[0]) for key, info \
                                            in func_info.iteritems()]))
    parser.add_option('-t', '--diagnostic-type', dest='diagnostic', \
        default='comprehensive', action='store', \
        help="Diagnostic type to display. Possible choices are: %s. "  \
             "(Default: ComprehensiveDiagnosticFigure)" % \
             "; ".join(diagnostics))
    parser.add_option('-s', '--savefn', dest='savefn', \
        default=False,
        help="Save plot. Argument is file name to save as.")
    parser.add_option('-n', '--non-interactive', dest='interactive', \
        default=True, action='store_false', \
        help="Do not interactively show the plot. (Default: Show the plot.)")
    parser.add_option('--log-colours', dest='logcolours', \
        action='callback', callback=parser.set_override_config, \
        help="Plot colours on a logarithmic scale. (Default: %s)" % \
                ((config.cfg.logcolours and "this is the default") or "colour scale is linear"))
    parser.add_option('--linear-colours', dest='logcolours', \
        action='callback', callback=parser.unset_override_config, \
        help="Plot colours on a linear scale. (Default: %s)" % \
                ((not config.cfg.logcolours and "this is the default") or "colour scale is logarithmic"))
    parser.add_option('--white-level', dest='vmax', action='callback', \
        callback=parser.override_config, type='float', \
        help="Values whose normalised colour is larger than this value " \
                "(on a 0-1 scale) will be shown as white. (Default: %g)" % \
                config.cfg.vmax)
    parser.add_option('--black-level', dest='vmin', action='callback', \
        callback=parser.override_config, type='float', \
        help="Values whose normalised clour is smaller than this value " \
                "(on a 0-1 scale) will be shown as black. (Default: %g)" % \
                config.cfg.vmin)
    options, args = parser.parse_args()
    main()
