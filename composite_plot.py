#!/usr/bin/env python
import utils
import diagnose

def main():
    for arfn in args:
        print "Plotting %s" % arfn,
        arf = utils.ArchiveFile(arfn)
        diagnose.make_composite_summary_plot(arf, \
                    options.preproc, options.outpsfn)
        print " Done"


if __name__ == '__main__':
    parser = utils.DefaultOptions()
    parser.add_option('-j', dest='preproc', \
        help="'psrplot' recognized preprocessing commands. (Default: 'C,D')", \
        default='C,D')
    parser.add_option('-o', dest='outpsfn', \
        help="Output postscript file name. (Default: <archive name>.ps", \
        default=None)
    options, args = parser.parse_args()
    main()

