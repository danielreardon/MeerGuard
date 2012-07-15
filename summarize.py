#!/usr/bin/env python

import sys
import os.path
import datetime

import numpy as np
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

    for arf in arfs:
        ar = arf.get_archive()
        ar.dedisperse()
        ar.remove_baseline()
        ar.fscrunch()
        ar.tscrunch()
        ar.pscrunch()
    return arfs


def plot(arfs, scale_indep=False, numcols=1, show_template=False, \
                    centre_prof=None):
    if centre_prof is None:
        centre_prof = config.cfg.centre_prof

    psrs = set([arf['name'] for arf in arfs])
    dates = set([arf['yyyymmdd'] for arf in arfs])
    rcvrs = set([arf['rcvr'] for arf in arfs])

    numpercol = int(np.ceil(len(arfs)/float(numcols)))

    plot_height = (TOP-BOT)/numpercol
    panel_width = (RIGHT-LEFT)/(numcols) # The "panel" includes plots+text
    if len(psrs) > 1:
        text_width = 0.225
    else:
        text_width = 0.15
    plot_width = panel_width-text_width
    mins = []
    maxs = []
    fig = plt.figure(figsize=(8,10))
    first = True
    oldcol = 0
    for ii, arf in enumerate(arfs):
        col = ii/numpercol
        row = ii % numpercol

        top = TOP - plot_height*row
        left = LEFT + panel_width*col

        if oldcol < col:
            # We've moved onto a new column, label the previous axes
            # Turn on tick labels for the bottom plot, and add a label
            plt.xlabel("Phase", size='small')
            plt.setp(plt.gca().xaxis.get_ticklabels(), visible=True, \
                        size='x-small', rotation=30)
            oldcol = col

        if first:
            ax0 = plt.axes([left, top-plot_height, plot_width, plot_height])
            plt.setp(ax0.xaxis.get_ticklabels(), visible=False)
            plt.setp(ax0.yaxis.get_ticklabels(), visible=False)
            first = False
        else:
            if scale_indep:
                ax = plt.axes([left, top-plot_height, plot_width, plot_height], \
                                sharex=ax0)
            else:
                ax = plt.axes([left, top-plot_height, plot_width, plot_height], \
                                sharex=ax0, sharey=ax0)
            plt.setp(ax.xaxis.get_ticklabels(), visible=False)
            plt.setp(ax.yaxis.get_ticklabels(), visible=False)

        # Get and prep archive
        ar = arf.get_archive()
        if show_template:
            stdfn = toas.get_standard(arf, analytic=False)
            if os.path.exists(stdfn):
                # Standard exists
                stdarf = utils.ArchiveFile(stdfn)
                # Scrunch it fully
                stdar = stdarf.get_archive()
                stdar.pscrunch()
                stdar.fscrunch()
                stdar.tscrunch()
                if centre_prof:
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
                    show_template = False
                else:
                    template = amp*template - offset
            else:
                show_template = False
        if not show_template:
            # This isn't an else-clause of the above because there are
            # cases where we turn template-showing off.
            if centre_prof:
                utils.print_info("Centering profile...", 2)
                ar.centre_max_bin()
            # Get and scale profile
            prof = ar.get_data().squeeze()
            prof -= np.median(prof)
            prof /= np.median(np.abs(prof))

        # plot
        phases = np.linspace(0, 1.0, len(prof), endpoint=False)
        plt.plot(phases, prof, 'k-')
        if show_template:
            plt.plot(phases, template, 'r-', lw=1.5, alpha=0.5)
        mins.append(prof.min())
        maxs.append(prof.max())

        # Add some text
        if len(psrs) > 1:
            plt.figtext(left+plot_width+0.02, top-0.001, "%(name)s" % arf, \
                        va='top', ha='left', size='x-small')
            plt.figtext(left+plot_width+0.13, top-0.013, "MJD: %(mjd).2f" % arf, \
                        va='top', ha='left', size='xx-small')
            plt.figtext(left+plot_width+0.02, top-0.013, "%(rcvr)s  (%(length)d s)" % arf, \
                        va='top', ha='left', size='xx-small')
        else:
            plt.figtext(left+plot_width+0.02, top-0.001, "MJD: %(mjd).2f" % arf, \
                        va='top', ha='left', size='xx-small')
            plt.figtext(left+plot_width+0.02, top-0.013, "%(rcvr)s  (%(length)d s)" % arf, \
                        va='top', ha='left', size='xx-small')

    # Turn on tick labels for the bottom plot, and add a label
    plt.xlabel("Phase", size='small')
    plt.setp(plt.gca().xaxis.get_ticklabels(), visible=True, \
                    size='x-small', rotation=30)

    plt.xlim(0, 1)
    if not scale_indep:
        plt.ylim(min(mins), max(maxs))
    title = "Summary of "
    if len(psrs) > 1:
        title += "%d Pulsars" % len(psrs)
    else:
        title += "PSR %(name)s" % arfs[0]
    fmt_date = lambda datestr: datetime.datetime.strptime(datestr, "%Y%m%d").strftime("%b %d, %Y")
    if len(dates) > 1:
        title += ", %s to %s" % (fmt_date(min(dates)), fmt_date(max(dates)))
    else:
        title += ", %s" % fmt_date(arfs[0]['yyyymmdd'])
    if len(rcvrs) > 1:
        title += ", %d receivers" % len(rcvrs)
    else:
        title += ", %(rcvr)s receiver" % arfs[0]
    plt.suptitle(title)


def main():
    arfns = args
    if not len(arfns):
        raise errors.InputError("No input archives provided! " \
                                "Here's your summary: NOTHING!")
    print "Making summary plot of %d files" % len(arfns)
    arfs = get_archives(arfns, options.sortkeys)
    plot(arfs, options.scale_indep, numcols=options.numcols, \
                    show_template=options.show_template, \
                    centre_prof=options.centre_prof)
    if options.savefn:
        plt.savefig(options.savefn)
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
        default=1, type='int', \
        help="Number of columns to arrange plots into. (Default: use " \
            "a one-column format.)")
    parser.add_option('-t', '--show-template', dest='show_template', \
        default=False, action='store_true', \
        help="Overlay the template of each pulsar (if available). " \
            "(Default: Don't bother with the template.)")
    options, args = parser.parse_args()
    if not options.sortkeys:
        options.sortkeys = ['mjd', 'rcvr', 'name']
    main()
