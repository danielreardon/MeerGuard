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
import combine

def reduce_archives(infns, outfn, 
                    preprocess=[], preargs=[], prekwargs=[], \
                    interprocess=[], interargs=[], interkwargs=[], \
                    postprocess=[], postargs=[], postkwargs=[]):
    """Given a list of PSRCHIVE file names group them into sub-bands
        then remove the edges of each sub-band to remove the artifacts
        caused by aliasing. Finally, combine the sub-bands into a single 
        output file.

        The combined sub-band files are not saved.

        Inputs:
            infns: A list of input PSRCHIVE archive file names.
            outfn: The output file's name.
            preprocess: A list of functions to apply to each of the
                input archives. The functions are applied in order.
            preargs: A list where each entry is a list of positional 
                arguments to be passed to the preprocess function with
                the same index.
            prekwargs: A list where each entry is a dictionary of keyword
                arguments to be passed to the proprocess function with
                the same index.
            interprocess: A same as 'preprocess', but applied to combined
                subbands.
            interargs: Same as 'preargs', but for 'interprocess' functions.
            interkwargs: Same as 'prekwargs', but for 'interprocess' functions.
            postprocess: A same as 'preprocess', but applied to the fully
                combined archive.
            postargs: Same as 'preargs', but for 'postprocess' functions.
            postkwargs: Same as 'prekwargs', but for 'postprocess' functions.

        Outputs:
            None

        NOTE: All pre/inter/post-processing functions will be called in the
            following way (for example, using a preprocessing function):
                
            <outfn> = preprocess[ii](<infn>, *preargs[ii], **prekwargs[ii])
    """
    if len(preprocess):
        # Pre-process files
        preprocessed = utils.apply_to_archives(infns, preprocess, \
                                                    preargs, prekwargs)
    else:
        preprocessed = infns

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
   
    if len(interprocess):
        # Apply intermediate processing to combined subbands
        interprocessed = utils.apply_to_archives(tmp_combined_subbands, \
                                        interprocess, interargs, interkwargs)
    else:
        interprocessed = tmp_combined_subbands

    # Combine the temporary sub-bands together in the frequency direction
    combine.combine_subbands(interprocessed, outfn)

    if len(postprocess):
        # Post-process output file
        postprocessed = utils.apply_to_archives([outfn], postprocess, \
                                            postargs, postkwargs)[0]
        os.rename(postprocessed, outfn)

    # Remove the temporary combined files
    for to_remove in tmp_combined_subbands:
        os.remove(to_remove)


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
    interprocess = []
    interargs = []
    interkwargs = []

    if options.num_chans_to_trim > 0:
        print "Will trim subband edges (# Chans trimmed at each edge: %d)" % \
                options.num_chans_to_trim
        interprocess.append(clean.trim_edge_channels)
        interargs.append([])
        interkwargs.append({'num_to_trim':options.num_chans_to_trim})
    
    reduce_archives(to_reduce, options.outfn, 
                    interprocess=interprocess, \
                    interargs=interargs, \
                    interkwargs=interkwargs
                    )


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
    parser.add_option('--trim-edge-channels', dest='num_chans_to_trim', \
                        help="Trim the edges of each input file to remove " \
                            "band-pass roll-off and the effect of aliasing. " \
                            "(Default: 0, don't trim edges.)", \
                        default=0, type='int')
    options, args = parser.parse_args()
    main()
