#!/usr/bin/env python

"""
Given a PSRCHIVE archive create diagnostic plots.

Patrick Lazarus, Dec. 12, 2011
"""

import re
import sys
import os
import os.path
import optparse
import copy
import tempfile
import shutil

import numpy as np
import scipy.signal
import scipy.stats
import scipy.optimize as opt
import matplotlib
import matplotlib.pyplot as plt

import utils
import clean_utils
import config
import errors

func_info = {'std': ("Standard Deviation", np.ma.std), \
             'mean': ("Average", np.ma.mean), \
             #'median': ("Median", np.ma.median), \
             'ptp': ("Max - Min", np.ma.ptp), \
             #'normality': ("Omnibus test of Normality", \
             #       lambda data, axis: scipy.stats.mstats.normaltest(data, axis=axis)[0]), \
             'periodicity': ("Periodic Signal Strength", \
                    lambda data, axis: np.max(np.abs(np.fft.rfft(\
                                data-np.expand_dims(data.mean(axis=axis), axis=axis), \
                                    axis=axis)), axis=axis)), \
             #'mad': ("Median Absolute Deviation", \
             #       lambda data, axis: np.ma.median(np.abs(data - \
             #                   np.expand_dims(np.ma.median(data, axis=axis), axis=axis)), axis=axis)),
             'surgical': ('Comprehensive Surgical Stats', clean_utils.comprehensive_stats)}

diagnostics = ['SlicerDiagnosticFigure', 'ComprehensiveDiagnosticFigure']

# Set plotting defaults
plt.rc(('xtick.major', 'ytick.major'), size=6)
plt.rc(('xtick.minor', 'ytick.minor'), size=3)
plt.rc('axes', labelsize='small')
plt.rc(('xtick', 'ytick'), labelsize='x-small')

class SlicerDiagnosticFigure(matplotlib.figure.Figure):
    def __init__(self, arf, func_key, rmbaseline=None, dedisp=None, \
                            rmprof=None, centre_prof=None, \
                            log=None, vmin=None, vmax=None, \
                    *args, **kwargs):
        super(SlicerDiagnosticFigure, self).__init__(*args, **kwargs)
        self.arf = arf
        self.ar = self.arf.get_archive()
        self.data = preprocess_archive_file(self.arf, rmbaseline, dedisp, \
                                    rmprof, centre_prof)

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
        
        # Current slices
        self.chan = None
        self.subint = None

        # Get weights
        self.weights = self.ar.get_weights()

        # Set default behaviour
        self.scale = False
        self.show_stats = False

        # Compute diagnostics
        utils.print_info("Computing diagnostic information...", 2)
        self.avail_diagnostics = {}
        mask_2d = np.bitwise_not(np.expand_dims(self.weights, 2).astype(bool))
        mask_3d = mask_2d.repeat(self.nbins, axis=2)
        masked_data = np.ma.masked_array(self.data, mask=mask_3d)
        for key, (title, func) in func_info.iteritems():
            utils.print_info("Working on %s..." % title, 3)
            self.avail_diagnostics[key] = func(masked_data, axis=2)

        self.apply_diagnostic_function(func_key)

    def apply_diagnostic_function(self, func_key):
        self.func_key = func_key
        self.title = func_info[func_key][0]
        utils.print_info("Loading %s..." % self.title, 2)
        self.imdata = self.avail_diagnostics[func_key]
        
    def connect_event_triggers(self):
        # Before setting up our own event handlers delete matplotlib's
        # default 'key_press_event' handler.
        defcids = self.canvas.callbacks.callbacks['key_press_event'].keys()
        for cid in defcids:
            self.canvas.callbacks.disconnect(cid)

        # Connect trigger
        self.canvas.mpl_connect('button_press_event', self.buttonpress)
        self.canvas.mpl_connect('key_press_event', self.keypress)

    def plot(self):
        utils.print_info("Plotting %s..." % self.title.lower(), 2)
        
        self.clear() # Clear the figure
        
        # Initialise list of static crosshairs
        self.hcrosshairs = []
        self.vcrosshairs = []

        # Add text
        psrname = utils.get_prefname(self.ar.get_source()) 
        self.text(0.02, 0.95, psrname, size='large', ha='left', va='center')
        self.text(0.02, 0.925, os.path.split(self.ar.get_filename())[-1], \
                        size='x-small', ha='left', va='center')
        self.text(0.25, 0.875, "Telescope: %s" % self.arf['telescop'], \
                        size='small', ha='left', va='center')
        self.text(0.25, 0.855, "Receiver: %s" % self.arf['rcvr'], \
                        size='small', ha='left', va='center')
        self.text(0.25, 0.835, "Backend: %s" % self.arf['backend'], \
                        size='small', ha='left', va='center')
                        
        if self.chan is None and self.subint is None:
                # Slices haven't been selected
            self.slice_info = self.text(0.02, 0.875, "Click on image to view slice", \
                        size='small', ha='left', va='center')
            self.selected_val = self.text(0.02, 0.855, "", \
                        size='small', ha='left', va='center')
        else:
            self.slice_info = self.text(0.02, 0.875, "Slicing along Chan: %s, Subint: %s" % \
                        (self.chan, self.subint), \
                        size='small', ha='left', va='center')
            self.selected_val = self.text(0.02, 0.855, "Value: %s" % \
                        (("%g" % self.imdata[self.subint, self.chan]) or 'masked'), \
                        size='small', ha='left', va='center')
        self.scale_info = self.text(0.9, 0.65, "Scaling: %s" % \
                        (self.scale and "on" or "off"), \
                        size='small', ha='left', va='center')
        self.stats_info = self.text(0.9, 0.62, "Stats: %s" % \
                        (self.show_stats and "shown" or "hidden"), \
                        size='small', ha='left', va='center')

        # Make axes
        self.dspec_ax = self.add_axes((0.05,0.05,0.6,0.6))
        self.prof_ax = self.add_axes((0.675,0.8225,0.275,0.065))
        self.fft_ax = self.add_axes((0.675,0.7125,0.275,0.065))
        self.hsum_ax = self.add_axes((0.05,0.65,0.6,0.075), sharex=self.dspec_ax)
        self.vsum_ax = self.add_axes((0.65,0.05,0.075,0.6), sharey=self.dspec_ax)
        self.hslice_ax = self.add_axes((0.05,0.735,0.6,0.075), sharex=self.dspec_ax)
        self.vslice_ax = self.add_axes((0.735,0.05,0.075,0.6), sharey=self.dspec_ax)
        self.cb_ax = self.add_axes((0.83,0.05,0.025,0.6), frameon=False)

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
        self.fft_ax.yaxis.set_ticks_position('right')
        self.fft_ax.yaxis.set_label_position('right')

        # Label axes
        self.prof_ax.set_xlabel("Phase bin")
        self.prof_ax.set_ylabel("Intensity")
        self.fft_ax.set_xlabel("Frequency (Hz)")
        self.fft_ax.set_ylabel("Power")

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
            
        self.canvas.draw()

    def buttonpress(self, event):
        if event.inaxes==self.dspec_ax and \
                (event.button==2 or (event.key=='shift' and event.button==1)):
            self.chan = int(np.round(event.xdata))
            self.subint = int(np.round(event.ydata))
            self.update_slice()

    def keypress(self, event):
        if event.key in ('s', 'S'):
            self.scale = not self.scale
            self.update_slice()
        elif event.key in ('m', 'm'):
            self.show_stats = not self.show_stats
            self.update_slice()
        elif event.key=='>':
            keys = sorted(func_info.keys())
            currind = keys.index(self.func_key)
            newind = (currind + 1)%len(keys)
            newkey = keys[newind]
            self.apply_diagnostic_function(newkey)
            self.plot()
            self.update_slice()
        elif event.key=='<':
            keys = sorted(func_info.keys())
            currind = keys.index(self.func_key)
            newind = (currind - 1)%len(keys)
            newkey = keys[newind]
            self.apply_diagnostic_function(newkey)
            self.plot()
            self.update_slice()
        elif event.key in ('z', 'Z'):
            event.canvas.toolbar.zoom()
        elif event.key == (' '):
            event.canvas.toolbar.home()

    def update_slice(self):
        self.scale_info.set_text("Scaling: %s" % \
                    (self.scale and "on" or "off"))
        self.stats_info.set_text("Stats: %s" % \
                    (self.show_stats and "shown" or "hidden"))

        if self.chan is None and self.subint is None:
            # Slices haven't been selected
            self.canvas.draw()
            return
        self.slice_info.set_text("Slicing along Chan: %s, Subint: %s" % \
                    (self.chan, self.subint))
        select_str = "Value: %s" % \
                (("%g" % self.imdata[self.subint, self.chan]) or 'masked')
        imaxlims = self.dspec_ax.axis()
        profxlims = self.prof_ax.get_xlim()

        self.hslice_ax.cla()
        mask = (self.weights[self.subint, :]==0)
        toplot = np.ma.masked_array(self.imdata[self.subint, :], mask=mask)
        if self.scale:
            toplot = clean_utils.subint_scaler(toplot[np.newaxis])[0]
        select_str += " %g" % toplot[self.chan]
        indices = np.repeat(np.arange(-0.5, self.nchans+0.5, 1),2)[1:-1]
        invertedmask = np.ma.masked_array(np.ones(self.nchans), mask=np.bitwise_not(mask))
        self.hslice_ax.plot(indices, np.repeat(toplot,2), 'k-')
        segments = np.ma.flatnotmasked_contiguous(invertedmask)
        if segments:
            for segment in segments:
                self.hslice_ax.axvspan(segment.start-0.5, segment.stop+0.5, fc='r', lw=0, alpha=0.2)
        
        if self.show_stats:
            # Plot median and MAD
            median = np.ma.median(toplot)
            mad = np.ma.median(np.abs(toplot-median))
            self.hslice_ax.axhline(median, c='k', ls='-')
            self.hslice_ax.axhline(median+mad, c='k', ls='--')
            self.hslice_ax.axhline(median-mad, c='k', ls='--')

        self.vslice_ax.cla()
        mask = (self.weights[:, self.chan]==0)
        toplot = np.ma.masked_array(self.imdata[:,self.chan], mask=mask)
        if self.scale:
            toplot = clean_utils.channel_scaler(toplot[:,np.newaxis])[:,0]
        select_str += " %g" % toplot[self.subint]
        indices = np.repeat(np.arange(-0.5, self.nsubs+0.5, 1),2)[1:-1]
        invertedmask = np.ma.masked_array(np.ones(self.nsubs), mask=np.bitwise_not(mask))
        self.vslice_ax.plot(np.repeat(toplot,2), indices, 'k-')
        segments = np.ma.flatnotmasked_contiguous(invertedmask)
        if segments:
            for segment in np.ma.flatnotmasked_contiguous(invertedmask):
                self.vslice_ax.axhspan(segment.start-0.5, segment.stop+0.5, fc='r', lw=0, alpha=0.2)
       
        if self.show_stats:
            # Plot median and MAD
            median = np.ma.median(toplot)
            mad = np.ma.median(np.abs(toplot-median))
            self.vslice_ax.axvline(median, c='k', ls='-')
            self.vslice_ax.axvline(median+mad, c='k', ls='--')
            self.vslice_ax.axvline(median-mad, c='k', ls='--')

        self.prof_ax.cla()
        profile = self.data[self.subint, self.chan]
        self.prof_ax.plot(np.arange(self.nbins), profile, 'k-')
        
        self.fft_ax.cla()
        profile -= profile.mean()
        powers = np.abs(np.fft.rfft(profile))[:self.nbins/2]
        period = self.ar.get_Integration(int(self.subint)).get_folding_period()
        freqs = np.fft.fftfreq(self.nbins, period/self.nbins)[:self.nbins/2]
        self.fft_ax.plot(freqs, powers, 'k-')
        self.fft_ax.set_xlim(0, np.max(freqs))

        self.dspec_ax.axis(imaxlims)
        self.prof_ax.set_xlim(profxlims)
        
        self.selected_val.set_text(select_str)
        
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
        self.fft_ax.set_xlabel("Frequency (Hz)")
        self.fft_ax.set_ylabel("Power")

        # Turn off unused labels
        plt.setp(self.hslice_ax.xaxis.get_ticklabels(), visible=False)
        plt.setp(self.vslice_ax.yaxis.get_ticklabels(), visible=False)
      
        # Rotate tick labels
        plt.setp(self.vsum_ax.xaxis.get_ticklabels(), rotation=45)
        plt.setp(self.vslice_ax.xaxis.get_ticklabels(), rotation=45)

        # Shift tick location
        self.prof_ax.yaxis.set_ticks_position('right')
        self.prof_ax.yaxis.set_label_position('right')
        self.fft_ax.yaxis.set_ticks_position('right')
        self.fft_ax.yaxis.set_label_position('right')

        self.canvas.draw()


class ComprehensiveDiagnosticFigure(matplotlib.figure.Figure):
    def __init__(self, arf, func_key, rmbaseline=None, dedisp=None, \
                            rmprof=None, centre_prof=None, \
                            log=None, vmin=None, vmax=None, \
                    *args, **kwargs):
        super(ComprehensiveDiagnosticFigure, self).__init__(*args, **kwargs)
        self.arf = arf
        self.ar = self.arf.get_archive()
        self.data = preprocess_archive_file(self.arf, rmbaseline, dedisp, \
                                    rmprof, centre_prof)

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
        self.text(0.02, 0.95, self.arf['name'], size='large', ha='left', va='center')
        self.text(0.02, 0.925, os.path.split(self.ar.get_filename())[-1], \
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


def make_diagnostic_figure(arf, func_re, diag_re='comprehensive', **kwargs):
    arf_copy = copy.deepcopy(arf)
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

    fig = plt.figure(figsize=(11,8), FigureClass=diagnostic_class, 
                arf=arf_copy, func_key=func_key, **kwargs)
    fig.connect_event_triggers()
    # Plot data
    fig.plot()
    return fig
    

def preprocess_archive_file(arf, rmbaseline=None, dedisp=None, \
                            rmprof=None, centre_prof=None):
    if rmbaseline is None:
        rmbaseline = config.cfg.rmbaseline
    if dedisp is None:
        dedisp = config.cfg.dedisp
    if rmprof is None:
        rmprof = config.cfg.rmprof
    if centre_prof is None:
        centre_prof = config.cfg.centre_prof

    ar = arf.get_archive()
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
        ar.dededisperse()
    
    data = ar.get_data()[:,0,:,:] # Select first polarization channel
                                  # archive is P-scrunched, so this is
                                  # total intensity, the only polarization
                                  # channel
    return clean_utils.apply_weights(data, ar.get_weights())


def make_polprofile_plot(arf, preproc='C,D,F,T', outfn=None):
    utils.print_info("Creating polarization profile plot for %s" % arf.fn, 3)
    if outfn is None:
        outfn = "%s.Scyl.ps" % arf.fn
    utils.print_info("Output plot name: %s" % outfn, 2)
    suffix = os.path.splitext(outfn)[-1]
    handle, tmpfn = tempfile.mkstemp(suffix=suffix)
    
    if suffix == '.ps':
        grdev = "%s/CPS" % tmpfn
    elif suffix == '.png':
        grdev = "%s/PNG" % tmpfn
    else:
        raise errors.InputError("Output file name extension for " \
                        "polarization profile plot (%s) is not " \
                        "recognized. Valid " \
                        "extensions are '.png' and '.ps'." % outfn)

    utils.execute(['psrplot', '-p', 'Scyl', '-j', preproc, \
                            arf.fn, '-D', grdev])
    # Rename tmpfn to requested output filename
    shutil.move(tmpfn, outfn)


def make_composite_summary_plot(arf, outfn=None):
    utils.print_info("Creating composite summary plot for %s" % arf.fn, 3)
    if outfn is None:
        outfn = "%s.ps" % arf.fn
    utils.print_info("Output plot name: %s" % outfn, 2)
    
    data = preprocess_archive_file(arf, True, True, False, False)
    nsubs, nchans, nbins = data.shape

    # Create figure
    plt.figure(figsize=(10.5, 8))
    if (arf['nsub'] > 1) and (arf['nchan'] > 1):
        __plot_all(arf, data)
    elif (arf['nsub'] > 1) and (arf['nchan'] == 1):
        assert nchans == 1
        __plot_nofreq(arf, data)
    elif (arf['nsub'] == 1) and (arf['nchan'] > 1):
        assert nsubs == 1
        __plot_notime(arf, data)
    elif  (arf['nsub'] == 1) and (arf['nchan'] == 1):
        assert nsubs == 1, nchans == 1
        __plot_profonly(arf, data)
    else:
        raise errors.FileError("Not sure how to plot diagnostic for file. " \
                                "(nsub: %d; nchan: %d)" % \
                                (ar['nsub'], ar['nchan']))
    # Save figure
    plt.savefig(outfn, papertype='a4', orientation='landscape')


def __add_text_info(arf):
    plt.figtext(0.02, 0.975, "%s\n%s    %s (%s)\n" 
                             "Length=%.1f s    BW=%.1f MHz\n" 
                             "N$_\mathrm{bin}$=%d    " 
                             "N$_\mathrm{chan}$=%d    "
                             "N$_\mathrm{sub}$=%d" % \
                             (os.path.split(arf.fn)[-1], \
                              arf['telescop'], arf['rcvr'], \
                              arf['backend'], arf['length'], arf['bw'],
                              arf['nbin'], arf['nchan'], arf['nsub']),
                size='small', ha='left', va='top')

def __add_prof(arf, data):
    nsubs, nchans, nbins = data.shape
    ph = np.linspace(0, 2, 2*nbins, endpoint=False)
    prof = data.sum(axis=1).sum(axis=0)
    plt.plot(ph, np.tile(prof, 2), 'k-')
    plt.xlabel("Phase")
    plt.ylabel("Intensity")


def __add_time(arf, data):
    nsubs, nchans, nbins = data.shape
    ph = np.linspace(0, 2, 2*nbins, endpoint=False)
    time = data.sum(axis=1)
    plt.imshow(np.tile(time, (1,2)), origin='bottom', aspect='auto',
               extent=(0, 2, 0, nsubs), cmap='Blues_r',
               interpolation='nearest')
    plt.xlabel("Phase")
    plt.ylabel("Sub-Integration")


def __add_freq(arf, data):
    nsubs, nchans, nbins = data.shape
    ph = np.linspace(0, 2, 2*nbins, endpoint=False)
    freq = data.sum(axis=0)
    plt.imshow(np.tile(freq, (1,2)), origin='bottom', aspect='auto',
               extent=(0, 2, 0, nchans), cmap='Blues_r',
               interpolation='nearest')
    plt.xlabel("Phase")
    plt.ylabel("Channel")


def __plot_profonly(arf, data):
    ax = plt.axes([0.075, 0.15, 0.875, 0.55])
    __add_prof(arf, data)
    __add_text_info(arf)
    

def __plot_nofreq(arf, data):
    # Plot profile
    ax = plt.axes([0.075, 0.5, 0.875, 0.2])
    plt.setp(ax.xaxis.get_ticklabels(), visible=False)
    __add_prof(arf, data)
    # Plot time vs phase
    plt.axes([0.075, 0.15, 0.875, 0.35])
    __add_time(arf, data)
    __add_text_info(arf)
    

def __plot_notime(arf, data):
    # Plot profile
    ax = plt.axes([0.075, 0.5, 0.875, 0.2])
    plt.setp(ax.xaxis.get_ticklabels(), visible=False)
    __add_prof(arf, data)
    # Plot freq vs phase
    plt.axes([0.075, 0.15, 0.875, 0.35])
    __add_freq(arf, data)
    __add_text_info(arf)
    info = __get_info(ar)
    

def __plot_all(arf, data):
    # Plot profile
    ax = plt.axes([0.575, 0.75, 0.4, 0.2])
    plt.setp(ax.xaxis.get_ticklabels(), visible=False)
    __add_prof(arf, data)
    # Plot freq vs phase
    plt.axes([0.075, 0.075, 0.4, 0.8])
    __add_freq(arf, data)
    # Plot time vs phase
    plt.axes([0.575, 0.075, 0.4, 0.675])
    __add_time(arf, data)
    __add_text_info(arf)


def make_composite_summary_plot_psrplot(ar, preproc='C,D', outfn=None):
    utils.print_info("Creating composite summary plot for %s" % ar.fn, 3)
    if outfn is None:
        outfn = "%s.ps" % ar.fn
    utils.print_info("Output plot name: %s" % outfn, 2)
    suffix = os.path.splitext(outfn)[-1]
    handle, tmpfn = tempfile.mkstemp(suffix=suffix)
    
    if suffix == '.ps':
        grdev = "%s/CPS" % tmpfn
    elif suffix == '.png':
        grdev = "%s/PNG" % tmpfn
    else:
        raise errors.InputError("Output file name extension for " \
                        "composite plot (%s) is not recognized. Valid " \
                        "extensions are '.png' and '.ps'." % outfn)

    if (ar['nsub'] > 1) and (ar['nchan'] > 1):
        __plot_all_psrplot(grdev, ar, preproc)
    elif (ar['nsub'] > 1) and (ar['nchan'] == 1):
        __plot_nofreq_psrplot(grdev, ar, preproc)
    elif (ar['nsub'] == 1) and (ar['nchan'] > 1):
        __plot_notime_psrplot(grdev, ar, preproc)
    elif  (ar['nsub'] == 1) and (ar['nchan'] == 1):
        __plot_profonly_psrplot(grdev, ar, preproc)
    else:
        raise errors.FileError("Not sure how to plot diagnostic for file. " \
                                "(nsub: %d; nchan: %d)" % \
                                (ar['nsub'], ar['nchan']))
    # Rename tmpfn to requested output filename
    shutil.move(tmpfn, outfn)


def __get_info(ar):
    info = "above:l=%s\n" \
                   "%s    %s (%s)\n" \
                   "Length=%.1f s    BW=%.1f MHz\n" \
                   "N\\dbin\\u=$nbin    N\\dchan\\u=$nchan    N\\dsub\\u=$nsubint," \
           "above:off=3.5" % \
                    (os.path.split(ar.fn)[-1], \
                     ar['telescop'], ar['rcvr'], \
                     ar['backend'], ar['length'], ar['bw'])
    return info

def __plot_profonly_psrplot(grdev, ar, preproc="D"):
    info = __get_info(ar)
    cmd = ["psrplot", "-O", "-j", preproc, "-c", "above:c=,x:range=0:2", \
            ar.fn, "-D", grdev, \
            "-p", "flux", "-c", ":0:x:view=0.075:0.95," \
                                     "y:view=0.15:0.7," \
                                     "subint=I," \
                                     "chan=I," \
                                     "pol=I," \
                                     "below:l=," \
                                     "%s" % info]
    utils.execute(cmd)
    
def __plot_nofreq_psrplot(grdev, ar, preproc="D"):
    info = __get_info(ar)
    cmd = ["psrplot", "-O", "-j", preproc, "-c", "above:c=,x:range=0:2", \
            ar.fn, "-D", grdev, \
            "-p", "flux", "-c", ":0:x:view=0.075:0.95," \
                                    "y:view=0.5:0.7," \
                                    "subint=I," \
                                    "chan=I," \
                                    "pol=I," \
                                    "x:opt=BCTS," \
                                    "x:lab=," \
                                    "below:l=," \
                                    "%s" % info, \
            "-p", "time", "-c", ":1:x:view=0.075:0.95," \
                                    "y:view=0.15:0.5," \
                                    "chan=I," \
                                    "pol=I," \
                                    "cmap:map=plasma"]
    utils.execute(cmd)
    
def __plot_notime_psrplot(grdev, ar, preproc="D"):
    info = __get_info(ar)
    cmd = ["psrplot", "-O", "-j", preproc, "-c", "above:c=,x:range=0:2", \
            ar.fn, "-D", grdev, \
            "-p", "flux", "-c", ":0:x:view=0.075:0.95," \
                                   "y:view=0.5:0.7," \
                                   "subint=I," \
                                   "chan=I," \
                                   "pol=I," \
                                   "x:opt=BCTS," \
                                   "x:lab=," \
                                   "below:l=," \
                                   "%s" % info, \
            "-p", "freq", "-c", ":1:x:view=0.075:0.95," \
                                   "y:view=0.15:0.5," \
                                   "subint=I," \
                                   "pol=I," \
                                   "cmap:map=plasma"]
    utils.execute(cmd)
    
def __plot_all_psrplot(grdev, ar, preproc="D"):
    info = __get_info(ar)
    cmd = ["psrplot", "-O", "-j", preproc, "-c", "above:c=,x:range=0:2", \
            ar.fn, "-D", grdev, \
            "-p", "flux", "-c", ":0:x:view=0.575:0.95," \
                                   "y:view=0.7:0.9," \
                                   "subint=I," \
                                   "chan=I," \
                                   "pol=I," \
                                   "x:opt=BCTS," \
                                   "x:lab=," \
                                   "below:l=", \
            "-p", "freq", "-c", ":1:x:view=0.075:0.45," \
                                   "y:view=0.15:0.7," \
                                   "subint=I," \
                                   "pol=I," \
                                   "%s," \
                                   "cmap:map=plasma" % info, \
            "-p", "time", "-c", ":2:x:view=0.575:0.95," \
                                   "y:view=0.15:0.7," \
                                   "chan=I," \
                                   "pol=I," \
                                   "cmap:map=plasma"]
    utils.execute(cmd)


def main():
    inarf = utils.ArchiveFile(args[0])
    config.cfg.load_configs_for_archive(inarf)
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
