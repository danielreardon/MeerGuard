#!/usr/bin/env python

"""
Given PSRCHIVE archives reduce them so they are ready to
produce TOAs.

Patrick Lazarus, Nov. 22, 2011
"""
import optparse
import os
import tempfile

import numpy as np
import matplotlib.pyplot as plt
import psrchive

import toas
import diagnose
import utils
import clean
import clean_utils
import combine
import config



def reduce_archives(infns, cfg): 
    """Given a list of PSRCHIVE file names group them into sub-bands
        then remove the edges of each sub-band to remove the artifacts
        caused by aliasing. Finally, combine the sub-bands into a single 
        output file.

        The combined sub-band files are not saved.

        Inputs:
            infns: A list of input PSRCHIVE archive file names.
            cfg: A CoastGuardConfig object containing configurations.

        Outputs:
            outfn: The final reduced file name.
            toas: TOA strings.
    """
    # Generate a filename
    hdr = utils.get_header_vals(infns[0], ['name', 'mjd'])
    basenm = "%s_MJD%.2f" % (hdr['name'], float(hdr['mjd']))

    if len(infns) > 1:
        combinefn = basenm + ".cmb.tmp"
        combine.combine_all(infns, combinefn, cfg.nchan_to_trim) 
    else:
        combinefn = infns[0]
    
    # Create diagnostic plots for pre-cleaned data
    if config.verbosity:
        print "Creating diagnostics for %s" % combinefn
    ar = psrchive.Archive_load(combinefn)
    ar.pscrunch()
    ar.remove_baseline()
    ar.dedisperse()
    data = ar.get_data().squeeze()
    template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
    data = clean_utils.remove_profile(data, ar.get_nsubint(), ar.get_nchan(), \
                                        template)
    data = clean_utils.apply_weights(data, ar.get_weights())
    for func_key in cfg.funcs_to_plot:
        diagnose.DiagnosticFigure(ar, data, func_key)
        plt.savefig("%s.%s.png" % (ar.get_filename(), func_key), dpi=600)

    # Clean the data
    if config.verbosity:
        print "Cleaning %s" % combinefn
    cleanfn = basenm + ".clean"
    ar = psrchive.Archive_load(combinefn)
    clean.deep_clean(ar, cleanfn, cfg.clean_chanthresh, \
                        cfg.clean_subintthresh, cfg.clean_binthresh)
    
    if not config.debug.INTERMEDIATE:
        os.remove(combinefn)
    # Re-create diagnostic plots for clean data
    if config.verbosity:
        print "Creating diagnostics for %s" % cleanfn
    ar = psrchive.Archive_load(cleanfn)
    ar.pscrunch()
    ar.remove_baseline()
    ar.dedisperse()
    data = ar.get_data().squeeze()
    template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
    data = clean_utils.remove_profile(data, ar.get_nsubint(), ar.get_nchan(), \
                                        template)
    data = clean_utils.apply_weights(data, ar.get_weights())
    for func_key in cfg.funcs_to_plot:
        diagnose.DiagnosticFigure(ar, data, func_key)
        plt.savefig("%s.%s.png" % (ar.get_filename(), func_key), dpi=600)

    # Make TOAs
    if config.verbosity:
        print "Generating TOAs"
    stdfn = toas.get_standard(cleanfn, cfg.base_standards_dir)
    if config.verbosity > 1:
        print "Standard profile: %s" % stdfn
    toastrs = toas.get_toas(cleanfn, stdfn, cfg.ntoa_time, cfg.ntoa_freq)
    return cleanfn, toastrs


def main():
    file_list = args + options.from_glob
    to_exclude = options.excluded_files + options.excluded_by_glob
    to_reduce = utils.exclude_files(file_list, to_exclude)
    
    print ""
    print "        reduce.py"
    print "     Patrick  Lazarus"
    print ""
    print "Number of input files: %d" % len(to_reduce)
    
    cfg = config.CoastGuardConfigs()
    cfg.get_default_configs()
    cfg.get_configs_for_archive(to_reduce[0])
   
    outfn, toastrs = reduce_archives(to_reduce, cfg)
    print "Output file name: %s" % outfn
    print "TOAs:"
    print "\n".join(toastrs)


if __name__=="__main__":
    parser = utils.DefaultOptions(usage="%prog [OPTIONS] FILES ...", \
                        description="Given a list of PSRCHIVE file names " \
                                    "reduce them so they are ready to " \
                                    "generate TOAs. A single output file " \
                                    "is produced.")
    parser.add_option('-g', '--glob', dest='from_glob', action='callback', \
                        callback=utils.get_files_from_glob, default=[], \
                        type='string', \
                        help="Glob expression of input files. Glob expression " \
                            "should be properly quoted to not be expanded by " \
                            "the shell prematurely. (Default: no glob " \
                            "expression is used.)") 
    parser.add_option('-x', '--exclude-file', dest='excluded_files', \
                        type='string', action='append', default=[], \
                        help="Exclude a single file. Multiple -x/--exclude-file " \
                            "options can be provided. (Default: don't exclude " \
                            "any files.)")
    parser.add_option('--exclude-glob', dest='excluded_by_glob', action='callback', \
                        callback=utils.get_files_from_glob, default=[], \
                        type='string', \
                        help="Glob expression of files to exclude as input. Glob " \
                            "expression should be properly quoted to not be " \
                            "expanded by the shell prematurely. (Default: " \
                            "exclude any files.)")
    options, args = parser.parse_args()
    main()
