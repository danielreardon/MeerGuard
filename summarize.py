#!/usr/bin/env python

import sys
import os.path
import datetime
import warnings

import numpy as np
import matplotlib
import matplotlib.pyplot as plt

import config
import utils
import errors
import toas
import clean_utils

TOP = 0.95
BOT = 0.05
LEFT = 0.05
RIGHT = 0.98


def get_archives(arfns, sortkeys=['mjd', 'rcvr', 'name']):
    arfs = [utils.ArchiveFile(arfn) for arfn in arfns]
    for sortkey in sortkeys:
        if sortkey.endswith("_rev"):
            sortkey = sortkey[:-4]
            rev = True
            utils.print_info("Sorting (in reverse) by %s..." % sortkey, 2)
        else:
            rev = False
            utils.print_info("Sorting by %s..." % sortkey, 2)
        if utils.header_param_types.get(sortkey) == str:
            arfs.sort(key=lambda x: x[sortkey].lower(), reverse=rev)
        else:
            arfs.sort(key=lambda x: x[sortkey], reverse=rev)
    
    # Pre-process archives
    for arf in arfs:
        ar = arf.get_archive()
        ar.dedisperse()
        ar.remove_baseline()
        ar.fscrunch()
        ar.tscrunch()
        ar.pscrunch()
    return arfs


class SummaryFigure(matplotlib.figure.Figure):
    def __init__(self, arfs, scale_indep=False, show_template=False, \
                    centre_prof=None, margins=(TOP, BOT, LEFT, RIGHT), \
                    layout=(2,10), infotext="MJD: %(mjd).2f\n%(rcvr)s  (%(length)d s)", \
                    *args, **kwargs):
        if centre_prof is None:
            centre_prof = config.cfg.centre_prof

        if len(layout) != 2:
            raise errors.DiagnosticError("Bad number of layout dimensions " \
                                         "(%d != 2)." % len(layout))
        if len(margins) != 4:
            raise errors.DiagnosticError("Bad number of margins provided " \
                                         "(%d != 4)." % len(margins))
        npanels = layout[0]*layout[1]
        if len(arfs) > npanels:
            raise errors.DiagnosticError("Cannot include more than %d " \
                                            "profiles." % npanels) 
        super(SummaryFigure, self).__init__(*args, **kwargs)
        self.arfs = arfs
        self.ax_to_arf = {}
        self.show_template = show_template
        self.centre_prof = centre_prof
        self.scale_indep = scale_indep
        self.numperrow, self.numpercol = layout
        self.top, self.bot, self.left, self.right = margins
        self.infotext = infotext

        # Count the number of pulsars, observing dates, and receivers
        self.psrs = set([arf['name'] for arf in arfs])
        self.dates = set([arf['yyyymmdd'] for arf in arfs])
        self.rcvrs = set([arf['rcvr'] for arf in arfs])
        
        # Compute the sizes of various elements of the summary plot
        self.panel_height = (self.top-self.bot)/self.numpercol
        self.panel_width = (self.right-self.left)/self.numperrow
        if len(self.psrs) > 1:
            self.text_width = 0.225
        else:
            self.text_width = 0.15
        self.plot_width = self.panel_width-self.text_width 
        self.plot_height = self.panel_height

    def buttonpress(self, event):
        if (event.button==2 or (event.key=='shift' and event.button==1)) and \
                        (event.inaxes in self.ax_to_arf):
            arf = self.ax_to_arf[event.inaxes]
            print "Filename:", arf.fn
            print "Source name:", arf['name']
            print "Telescope:", arf['telescop']
            print "Receiver:", arf['rcvr']
            print "Backend:", arf['backend']
            print "Observation length (s):", arf['length']
            print "Bandwidth (MHz):", arf['bw']
            print "MJD:", arf['mjd']
            print "Date (yyyymmdd):", arf['yyyymmdd']
            print "Seconds since midnight:", arf['secs']

    def connect_event_triggers(self):
        self.canvas.mpl_connect("button_press_event", self.buttonpress)

    def plot(self):
        utils.print_info("Plotting summary of %d archives..." % \
                            len(self.arfs), 2)
        first = True
        oldcol = 0
        for ii, arf in enumerate(self.arfs):
            col = ii/self.numpercol
            row = ii % self.numpercol
         
            panel_top = self.top - self.panel_height*row
            panel_left = self.left + self.panel_width*col
         
            if oldcol < col:
                # We've moved onto a new column, label the previous axes
                # Turn on tick labels for the bottom plot, and add a label
                plt.xlabel("Phase", size='small')
                plt.setp(plt.gca().xaxis.get_ticklabels(), visible=True, \
                            size='x-small', rotation=30)
                oldcol = col
         
            if first:
                ax0 = plt.axes([panel_left, panel_top-self.plot_height, \
                                self.plot_width, self.plot_height])
                plt.setp(ax0.xaxis.get_ticklabels(), visible=False)
                plt.setp(ax0.yaxis.get_ticklabels(), visible=False)
                first = False
                ax = ax0
            else:
                if self.scale_indep:
                    ax = plt.axes([panel_left, panel_top-self.plot_height, \
                                    self.plot_width, self.plot_height], \
                                    sharex=ax0)
                else:
                    ax = plt.axes([panel_left, panel_top-self.plot_height, \
                                    self.plot_width, self.plot_height], \
                                    sharex=ax0, sharey=ax0)
                plt.setp(ax.xaxis.get_ticklabels(), visible=False)
                plt.setp(ax.yaxis.get_ticklabels(), visible=False)
            self.plot_panel(arf, ax)
            # Record mapping between axes and ArchiveFile
            self.ax_to_arf[ax] = arf

            # Add some text
            towrite = self.infotext.decode('string-escape') % arf
            plt.figtext(panel_left+self.plot_width+0.02, \
                        panel_top-0.001, "%(name)s" % arf, \
                        va='top', ha='left', size='x-small')
            plt.figtext(panel_left+self.plot_width+0.02, \
                        panel_top-0.013, towrite, \
                        va='top', ha='left', size='xx-small')

        # Turn on tick labels for the bottom plot, and add a label
        plt.xlabel("Phase", size='small')
        plt.setp(plt.gca().xaxis.get_ticklabels(), visible=True, \
                        size='x-small', rotation=30)

        plt.xlim(0, 1)
        plt.suptitle(self.get_title())

    def plot_panel(self, arf, ax):
        # Get and prep archive
        ar = arf.get_archive()
        usetemplate = self.show_template
        if usetemplate:
            stdfn = toas.get_standard(arf, analytic=False)
            if os.path.exists(stdfn):
                # Standard exists
                stdarf = utils.ArchiveFile(stdfn)
                # Scrunch it fully
                stdar = stdarf.get_archive()
                stdar.pscrunch()
                stdar.fscrunch()
                stdar.tscrunch()
                if self.centre_prof:
                    stdar.centre_max_bin()
                # Align profile with standard profile
                phs, err = ar.get_Profile(0,0,0).shift(stdar.get_Profile(0,0,0))
                ar.rotate_phase(phs)
                # Get and scale profile
                prof = ar.get_data().squeeze()
                prof -= np.median(prof)
                prof /= np.median(np.abs(prof))
                template = stdar.get_data().squeeze()
                try:
                    amp, offset = clean_utils.fit_template(prof, template)
                except errors.FitError:
                    warnings.warn("Error when scaling template. " \
                                    "Template will not be shown.", \
                                    errors.CoastGuardWarning)
                    usetemplate = False
                else:
                    template = amp*template - offset
            else:
                warnings.warn("No template available. " \
                                "template will not be shown.", \
                                errors.CoastGuardWarning)
                usetemplate = False
        if not usetemplate:
            # This isn't an else-clause of the above because there are
            # cases where we turn template-showing off.
            if self.centre_prof:
                utils.print_info("Centering profile...", 2)
                ar.centre_max_bin()
            # Get and scale profile
            prof = ar.get_data().squeeze()
            prof -= np.median(prof)
            prof /= np.median(np.abs(prof))

        # plot
        phases = np.linspace(0, 1.0, len(prof), endpoint=False)
        ax.plot(phases, prof, 'k-')
        if usetemplate:
            ax.plot(phases, template, 'r-', lw=1.5, alpha=0.5)
        #ax.format_coord = lambda x,y: "%s; x=%g, y=%g" % \
        #                    (os.path.split(arf.fn)[-1], x, y)

    def get_title(self):
        title = "Summary of "
        title += "%d Pulsars" % len(self.psrs)
        fmt_date = lambda datestr: datetime.datetime.strptime(datestr, \
                                        "%Y%m%d").strftime("%b %d, %Y")
        title += ", %s to %s" % (fmt_date(min(self.dates)), \
                                        fmt_date(max(self.dates)))
        if len(self.rcvrs) > 1:
            title += ", %d receivers" % len(self.rcvrs)
        else:
            title += ", %(rcvr)s receiver" % self.arfs[0]
        return title


def main():
    arfns = args
    if not len(arfns):
        raise errors.InputError("No input archives provided! " \
                                "Here's your summary: NOTHING!")
    print "Making summary plot of %d files" % len(arfns)
    if options.numrows is None:
        numrows = int(np.ceil(len(arfns)/float(options.numcols)))
    else:
        numrows = options.numrows
    layout = (options.numcols, numrows)
    numpanels = np.prod(layout)
    arfs = get_archives(arfns, options.sortkeys)
    numfigs = int(np.ceil(len(arfns)/float(numpanels)))
    for fignum in range(numfigs):
        arfs_toplot = arfs[fignum*numpanels:(fignum+1)*numpanels]
        fig = plt.figure(figsize=(8,11), FigureClass=SummaryFigure, 
                arfs=arfs_toplot, scale_indep=options.scale_indep, \
                show_template=options.show_template, \
                centre_prof=options.centre_prof, layout=layout, \
                infotext=options.info_text)
        fig.text(0.94, 0.02, "%d / %d" % (fignum+1, numfigs), size='small')
        fig.connect_event_triggers()
        fig.plot()
        if options.savefn:
            if numfigs > 1:
                fn, ext = os.path.splitext(options.savefn)
                plt.savefig(fn+("_page%d"%(fignum+1))+ext, papertype='a4')
            else:
                plt.savefig(options.savefn, papertype='a4')
        fig.canvas.mpl_connect('key_press_event', \
                lambda ev: ev.key in ('q', 'Q') and plt.close(fig))
    if options.interactive:
        plt.show()


if __name__ == "__main__":
    parser = utils.DefaultOptions()
    parser.add_option('-s', "--savefn", default=None, \
        help="Filename to save the plot as. " \
            "(Default: do not save plot).")
    parser.add_option('-n', '--non-interactive', dest='interactive', \
        help="Do not show the plot interactively. " \
            "(Default: Show interactively.)", \
        default=True, action='store_false')
    parser.add_option('--centre-profile', dest='centre_prof', \
        action='callback', callback=parser.set_override_config, \
        help="Centre profile. (Default: %s)" % \
                ((config.cfg.centre_prof and "this is the default") or "do not rotate profile"))
    parser.add_option('--no-centre-profile', dest='centre_prof', \
        action='callback', callback=parser.unset_override_config, \
        help="Do not rotate profile. (Default: %s)" % \
                ((not config.cfg.centre_prof and "this is the default") or "rotate profile"))
    parser.add_option('--scale-indep', dest='scale_indep', \
        action='store_true', default=False, \
        help="Scale all profiles independently. (Default: use same " \
            "scale for all profiles.)")
    parser.add_option('--sort', dest='sortkeys', \
        action='append', default=[], \
        help="Sort plots (top to bottom is increasing). Keys are " \
            "vap-recognized keywords. Multiple --sort options can " \
            "be provided. Options provided later will take precedent " \
            "over previous options. (Default: Sort by MJD, receiver, " \
            "then source name.)")
    parser.add_option('--numcols', dest='numcols', \
        default=2, type='int', \
        help="Number of columns to arrange plots into. (Default: use " \
            "a one-column format.)")
    parser.add_option('--numrows', dest='numrows', \
        default=10, type='int', \
        help="Number of rows to arrange plots into. (Default: use " \
            "as many rows as necessary to fit onto one page.)")
    parser.add_option('-t', '--show-template', dest='show_template', \
        default=False, action='store_true', \
        help="Overlay the template of each pulsar (if available). " \
            "(Default: Don't bother with the template.)")
    parser.add_option('-i', '--info-text', dest='info_text', \
        default="(DM=%(dm).2f pc$\,$cm$^{-3}$, P=%(pms).2f ms)\n" \
                "MJD: %(mjd).2f (%(date:%b %d, %Y)s)\n" \
                "%(rcvr)s\n" \
                "f=%(freq).1f MHz, BW=%(bw)d MHz\n" \
                "T$_{obs}$=%(length)d s\nSNR=%(snr).1f", \
        help="Text to display next to each panel. (Default: " \
            "display MJD, receiver, and observation length.)")
    options, args = parser.parse_args()
    if not options.sortkeys:
        options.sortkeys = ['mjd', 'rcvr', 'name']
    main()
