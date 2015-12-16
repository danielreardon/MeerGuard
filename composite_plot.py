#!/usr/bin/env python
import matplotlib
matplotlib.use('agg') # A non-interactive backend

import utils
import diagnose

def main():
    for arfn in args:
        print "Plotting %s" % arfn,
        arf = utils.ArchiveFile(arfn)
        diagnose.make_composite_summary_plot(arf, options.outpsfn)
        print " Done"


if __name__ == '__main__':
    parser = utils.DefaultOptions()
#    parser.add_option('-D', '--dedisperse', dest='dedisp', \
#        action='callback', callback=parser.set_override_config, \
#        help="Dedisperse archive before producing diagnostics. " \
#             "(Default: %s)" % ((config.cfg.dedisp and "this is the default") or "use DM=0"))
#    parser.add_option('--no-dedisperse', dest='dedisp', \
#        action='callback', callback=parser.unset_override_config, \
#        help="Dedisperse archive to DM=0 before producing diagnostics. " \
#             "(Default: %s)" % ((not config.cfg.dedisp and "this is the default") or "use DM in emphemeris"))
#    parser.add_option('-b', '--remove-baseline', dest='rmbaseline', \
#        action='callback', callback=parser.set_override_config, \
#        help="Remove baselines from all profiles using archive's " \
#                "'remove_baseline()' method. (Default: %s)" % \
#                ((config.cfg.rmbaseline and "this is the default") or "do not remove baselines"))
#    parser.add_option('--no-remove-baseline', dest='rmbaseline', \
#        action='callback', callback=parser.unset_override_config, \
#        help="Do not perform any baseline removal. (Default: %s)" % \
#                ((not config.cfg.rmbaseline and "this is the default") or "remove baselines"))
#    parser.add_option('-r', '--remove-profile', dest='rmprof', \
#        action='callback', callback=parser.set_override_config, \
#        help="Remove profile. (Default: %s)" % \
#                ((config.cfg.rmprof and "this is the default") or "leave profile"))
#    parser.add_option('--no-remove-profile', dest='rmprof', \
#        action='callback', callback=parser.unset_override_config, \
#        help="Do not subtract profile. (Default: %s)" % \
#                ((not config.cfg.rmprof and "this is the default") or "remove profile"))
#    parser.add_option('--centre-profile', dest='centre_prof', \
#        action='callback', callback=parser.set_override_config, \
#        help="Centre profile. (Default: %s)" % \
#                ((config.cfg.centre_prof and "this is the default") or "do not rotate profile"))
#    parser.add_option('--no-centre-profile', dest='centre_prof', \
#        action='callback', callback=parser.unset_override_config, \
#        help="Do not rotate profile. (Default: %s)" % \
#                ((not config.cfg.centre_prof and "this is the default") or "rotate profile"))
    parser.add_option('-o', dest='outpsfn', \
        help="Output postscript file name. (Default: <archive name>.ps", \
        default=None)
    options, args = parser.parse_args()
    main()

