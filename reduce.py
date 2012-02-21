#!/usr/bin/env python

"""
Given PSRCHIVE archives reduce them so they are ready to
produce TOAs.

Patrick Lazarus, Nov. 22, 2011
"""
import optparse
import os
import tempfile

import utils
import clean
import clean_utils
import combine
import config

def reduce_archives(infns, outfn, cfg): 
    """Given a list of PSRCHIVE file names group them into sub-bands
        then remove the edges of each sub-band to remove the artifacts
        caused by aliasing. Finally, combine the sub-bands into a single 
        output file.

        The combined sub-band files are not saved.

        Inputs:
            infns: A list of input PSRCHIVE archive file names.
            outfn: The output file's name.

        Outputs:
            None
    """
    # Combine files from the same sub-band in the time direction
    tmp_combined_subbands = []
    for ctr_freq, to_combine in utils.group_by_ctr_freq(preprocessed).iteritems():
        # Create a temporary output file
        tmphandle, tmpfn = tempfile.mkstemp(suffix=".%dMHz.tmp" % ctr_freq, \
                                            prefix="combined", dir=os.getcwd())
        os.close(tmphandle)

        # Combine sub-integrations for this sub-band
        combine.combine_subints(to_combine, tmpfn)
        tmp_combined_subbands.append(tmpfn)
   
    if cfg.nchan_to_trim > 0:
        print "Will trim subband edges (# Chans trimmed at each edge: %d)" % \
                cfg.nchan_to_trim
        clean.trim_edge_channels(num_to_trim=cfg.nchan_to_trim)

    # Combine the temporary sub-bands together in the frequency direction
    combine.combine_subbands(tmp_combined_subbands, outfn)

    # Remove the temporary combined files
    for to_remove in tmp_combined_subbands:
        os.remove(to_remove)
    
    # Create diagnostic plots for pre-cleaned data
    ar = psrchive.Archive_load(outfn)
    ar.pscrunch()
    ar.remove_baseline()
    ar.dedisperse()
    data = ar.get_data().squeeze()
    template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
    data = clean_utils.remove_profile(data, ar.get_nsubint(), ar.get_nchan(), \
                                        template, options.nthreads)
    data = clean_utils.apply_weights(data, ar.get_weights())
    for func_key in cfg.funcs_to_plot:
        DiagnosticFigure(ar, data, func_key)
        plt.savefig("%s.%s.png" % (ar.get_filename(), func_key), dpi=600)

    # Clean the data
    cleanfn = os.path.splitext(outfn)[-1]+".clean"
    ar = psrchive.Archive_load(outfn)
    clean.deep_clean(ar, cleanfn, cfg.clean_chanthresh, \
                        cfg.clean_subintthresh, cfg.clean_binthresh)
    
    # Re-create diagnostic plots for clean data
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
        DiagnosticFigure(ar, data, func_key)
        plt.savefig("%s.%s.png" % (ar.get_filename(), func_key), dpi=600)

    # Make TOAs
    stdout, stderr = utils.execute("pat -F -T -s %s -A %s -f %s -K " \
                                   "%s.toa/PNG -t %s" % \
            (cfg.standard_profile, cfg.toa_method, cfg.toa_method, \
                cleanfn, cleanfn)) 
    print stdout


def main():
    file_list = args + options.from_glob
    to_exclude = options.excluded_files + options.excluded_by_glob
    to_reduce = utils.exclude_files(file_list, to_exclude)
    
    print ""
    print "        reduce.py"
    print "     Patrick  Lazarus"
    print ""
    print "Number of input files: %d" % len(to_reduce)
    print "Output file name: %s" % options.outfn
    
    cfg = config.CoastGuardConfigs()
    cfg.get_default_configs()
    cfg.get_configs_for_archive(to_reduce[0])
   
    reduce_archives(to_reduce, options.outfn, cfg)


if __name__=="__main__":
    parser = utils.DefaultOptions(usage="%prog [OPTIONS] FILES ...", \
                        description="Given a list of PSRCHIVE file names " \
                                    "reduce them so they are ready to " \
                                    "generate TOAs. A single output file " \
                                    "is produced.")
    parser.add_option('-o', '--outname', dest='outfn', type='string', \
                        help="The output (reduced) file's name. " \
                            "(Default: 'reduce.out.ar')", \
                        default="reduce.out.ar")
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
