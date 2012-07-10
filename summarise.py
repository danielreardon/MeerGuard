#!/usr/bin/env python

import sys

import numpy as np
import matplotlib.pyplot as plt

from coast_guard import config
from coast_guard import utils

TOP = 0.95
BOT = 0.05


def get_archives(arfns, centre_prof=None, \
                    sortkeys=['mjd', 'rcvr', 'name']):
    if centre_prof is None:
        centre_prof = config.cfg.centre_prof

    arfs = [utils.ArchiveFile(arfn) for arfn in arfns]
    for sortkey in sortkeys:
        utils.print_info("Sorting by %s..." % sortkey, 2)
        if utils.header_param_types.get(sortkey) == str:
            arfs.sort(key=lambda x: x[sortkey].lower())
        else:
            arfs.sort(key=lambda x: x[sortkey])

    if centre_prof:
        utils.print_info("Centering profile...", 2)
    for arf in arfs:
        ar = arf.get_archive()
        if centre_prof:
            ar.centre_max_bin()
        ar.dedisperse()
        ar.remove_baseline()
        ar.fscrunch()
        ar.tscrunch()
        ar.pscrunch()
    return arfs


def plot(arfs, scale_indep=False):
    psrs = set([arf['name'] for arf in arfs])

    first = True
    fig = plt.figure(figsize=(8,10))
    plot_height = (TOP-BOT)/len(arfs)
    mins = []
    maxs = []
    for ii, arf in enumerate(arfs):
        if first:
            ax0 = plt.axes([0.05, TOP-plot_height*(ii+1), 0.7, plot_height])
            plt.setp(ax0.xaxis.get_ticklabels(), visible=False)
            plt.setp(ax0.yaxis.get_ticklabels(), visible=False)
            first = False
        else:
            if scale_indep:
                ax = plt.axes([0.05, TOP-plot_height*(ii+1), 0.7, plot_height], \
                                sharex=ax0)
            else:
                ax = plt.axes([0.05, TOP-plot_height*(ii+1), 0.7, plot_height], \
                                sharex=ax0, sharey=ax0)
            plt.setp(ax.xaxis.get_ticklabels(), visible=False)
            plt.setp(ax.yaxis.get_ticklabels(), visible=False)

        # Get and prep archive
        ar = arf.get_archive()
        # Get and scale profile
        prof = ar.get_data().squeeze()
        prof -= np.median(prof)
        prof /= np.median(np.abs(prof))

        # Compute phases
        phases = np.linspace(0, 1.0, len(prof), endpoint=False)

        # plot
        plt.plot(phases, prof, 'k-')
        mins.append(prof.min())
        maxs.append(prof.max())

        # Add some text
        if len(psrs) > 1:
            plt.figtext(0.77, TOP-plot_height*ii-0.017, "%(name)s" % arf, \
                        va='bottom', ha='left', size='x-small')
            plt.figtext(0.88, TOP-plot_height*ii-0.022, "MJD: %(mjd).2f" % arf, \
                        va='top', ha='left', size='xx-small')
            plt.figtext(0.77, TOP-plot_height*ii-0.022, "%(rcvr)s" % arf, \
                        va='top', ha='left', size='xx-small')
            plt.figtext(0.82, TOP-plot_height*ii-0.022, "(%(length)d s)" % arf, \
                        va='top', ha='left', size='xx-small')
        else:
            plt.figtext(0.77, TOP-plot_height*ii-0.017, "MJD: %(mjd).2f" % arf, \
                        va='bottom', ha='left', size='xx-small')
            plt.figtext(0.77, TOP-plot_height*ii-0.022, "%(rcvr)s" % arf, \
                        va='top', ha='left', size='xx-small')
            plt.figtext(0.82, TOP-plot_height*ii-0.022, "(%(length)d s)" % arf, \
                        va='top', ha='left', size='xx-small')

    # Turn on tick labels for the bottom plot, and add a label
    plt.xlabel("Phase")
    plt.setp(plt.gca().xaxis.get_ticklabels(), visible=True)

    plt.xlim(0, 1)
    if not scale_indep:
        plt.ylim(min(mins), max(maxs))
    if len(psrs) > 1:
        plt.suptitle("%d Pulsars" % len(psrs))
    else:
        plt.suptitle("PSR %(name)s" % arfs[0])


def main():
    arfns = args
    print "Making summary plot of %d files" % len(arfns)
    arfs = get_archives(arfns, options.centre_prof, options.sortkeys)
    plot(arfs, options.scale_indep)
    if options.savefn:
        plt.savefig(savefn)
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
            " and source name.)")
    options, args = parser.parse_args()
    if not options.sortkeys:
        options.sortkeys = ['mjd', 'rcvr', 'name']
    main()
