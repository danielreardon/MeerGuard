#!/usr/bin/env python

"""
Given PSRCHIVE archives reduce them so they are ready to
produce TOAs.

Patrick Lazarus, Nov. 22, 2011
"""
import optparse
import datetime
import os.path
import os
import tempfile
import sys
import traceback

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
import errors

class ReductionLog(object):
    """An object to log reduction of timing data.
    """
    def __init__(self, infns, fn):
        # calculate MD5 checksum of all input file
        self.infile_md5s = {}
        for infn in infns:
            self.infile_md5s[infn.fn] = utils.get_md5sum(infn.fn)
        
        self.fn = fn

        # Find the git hash of the code
        self.githash = utils.get_githash()
        self.dirtyrepo = utils.is_gitrepo_dirty()

        # Report the pwd
        self.workdir = os.getcwd()

        # Report the command line used 
        self.cmdline = " ".join(sys.argv)

    def start(self):
        self.starttime = datetime.datetime.now()

    def finish(self):
        self.endtime = datetime.datetime.now()
        self.time_elapsed = self.endtime - self.starttime

    def failure(self, exctype, excval, exctb):
        # Make sure we don't get colour tags in our log's traceback,
        # they're distracting
        tmp = config.colour
        config.colour = False
        self.epilog = "".join(traceback.format_exception(exctype, excval, exctb))
        config.colour = tmp

    def success(self, outfns, toastrs):
        self.outfns = outfns
        self.toastrs = toastrs

        self.epilog = "Output %d data files:" % len(outfns)
        for outfn in outfns:
            self.epilog += "\n    %s (MD5: %s)" % \
                            (outfn.fn, utils.get_md5sum(outfn.fn))

        self.epilog += "\nGenerated %d TOAs:" % len(toastrs)
        for toastr in toastrs:
            self.epilog += "\n    %s" % toastr

    def to_file(self):
        f = open(self.fn, 'w')
        f.write("Starting data reduction: %s\n" % str(self.starttime))
        f.write("Current Coast Guard git hash: %s" % self.githash)
        if self.dirtyrepo:
            f.write(" (dirty)\n")
        else:
            f.write("\n")
        f.write("Current working directory: %s\n" % self.workdir)
        f.write("Complete command line: %s\n" % self.cmdline)
        f.write("Reduced %d files:\n" % len(self.infile_md5s))
        for key in sorted(self.infile_md5s.keys()):
            f.write("    %s (MD5: %s)\n" % (key, self.infile_md5s[key]))
        f.write("Reduction finished: %s (Time elapsed: %s)\n" % \
                (str(self.endtime), str(self.time_elapsed)))
        f.write(self.epilog+"\n")
        f.close()


class ReductionJob(object):
    """An object to represent the reduction of an observation.
    """
    def __init__(self, infns, outfn, is_asterix=False):
        """Given a list of PSRCHIVE file names create a
            ReductionJob object.

            Input:
                infns: A list of input PSRCHIVE archive file names.
                outfn: The name of the reduced archive output.
                is_asterix: If the data is from Effelsberg's Asterix backend.
                    If True, the header information will be corrected.
                    (Default: False)

            Output:
                job: The Reduction job object.
        """
        self.infns = infns
        self.outfn = outfn
        self.basenm = os.path.splitext(self.outfn)[0]

        self.is_asterix = is_asterix # Hopefully only temporary

        logfn = utils.get_outfn(self.basenm+'.log', infns[0])
        self.log = ReductionLog(infns, logfn)
        
    def run(self):
        """Call method to reduce archives, and take care of logging.

            Inputs:
                None

            Outputs:
                None
        """
        
        self.log.start()
        try:
            cleanfns, toastrs = self.reduce_archives()
        except Exception:
            self.log.failure(*sys.exc_info())
            sys.stderr.write("".join(traceback.format_exception(*sys.exc_info())))
            raise errors.DataReductionFailed("Data reduction failed! " \
                        "Check log file: %s" % (self.log.fn))
        else:
            self.log.success(cleanfns, toastrs)
        finally:
            self.log.finish()
            self.log.to_file()
        return cleanfns, toastrs

    def reduce_archives(self): 
        """Group input files into sub-bands then remove the edges of each 
            sub-band to remove the artifacts caused by aliasing. Finally, 
            combine the sub-bands into a single output file.
 
            The combined sub-band files are not saved.
 
            Inputs:
                None
                
            Outputs:
                outfn: The final reduced file name.
                toas: TOA strings.
        """
        if len(self.infns) > 1:
            combinearfs = combine.combine_all(self.infns, self.basenm+".cmb")
        else:
            combinearfs = self.infns
        
        cleanarfs = []
        toastrs = []
        for combinearf in combinearfs:
            if self.is_asterix:
                # Correct the file header
                combinearf = utils.correct_asterix_header(combinearf)
            # Reload configurations
            config.cfg.load_configs_for_archive(combinearf)
            # Create diagnostic plots for pre-cleaned data
            utils.print_info("Creating diagnostics for %s" % combinearf.fn, 1)
            for func_key in config.cfg.funcs_to_plot:
                diagnose.make_diagnostic_figure(combinearf, func_key, \
                                            rmprof=True)
                plt.savefig("%s_diag_noprof_%s.png" % (combinearf.fn, func_key), dpi=600)
                diagnose.make_diagnostic_figure(combinearf, func_key, \
                                            rmprof=False)
                plt.savefig("%s_diag_%s.png" % (combinearf.fn, func_key), dpi=600)
 
            # Clean the data
            utils.print_info("Cleaning %s" % combinearf.fn, 1)
            cleanarf = clean.clean_archive(combinearf, self.outfn)
            
            # Re-create diagnostic plots for clean data
            utils.print_info("Creating diagnostics for %s" % cleanarf.fn, 1)
            for func_key in config.cfg.funcs_to_plot:
                diagnose.make_diagnostic_figure(cleanarf, func_key, \
                                                rmprof=True)
                plt.savefig("%s_diag_noprof_%s.png" % (cleanarf.fn, func_key), dpi=600)
                diagnose.make_diagnostic_figure(cleanarf, func_key, \
                                                rmprof=False)
                plt.savefig("%s_diag_%s.png" % (cleanarf.fn, func_key), dpi=600)

            cleanarfs.append(cleanarf)
            
            # Make TOAs
            utils.print_info("Generating TOAs", 1)
            stdfn = toas.get_standard(cleanarf)
            if not os.path.isfile(stdfn):
                raise errors.NoStandardProfileError("The standard profile (%s) " \
                                                "cannot be found!" % stdfn)
            utils.print_info("Standard profile: %s" % stdfn, 2)
            toastrs.extend(toas.get_toas(cleanarf, stdfn))
        return cleanarfs, toastrs


def main():
    print ""
    print "        reduce.py"
    print "     Patrick  Lazarus"
    print ""
    file_list = args + options.from_glob
    to_exclude = options.excluded_files + options.excluded_by_glob
    to_reduce = utils.exclude_files(file_list, to_exclude)
    print "Number of input files: %d" % len(to_reduce)
    
    to_reduce = [utils.ArchiveFile(fn) for fn in to_reduce]
    
    # Read configurations
    config.cfg.load_configs_for_archive(to_reduce[0])
  
    job = ReductionJob(to_reduce, options.outfn)
    outfns, toastrs = job.run()

    print "Output file names:"
    for outfn in outfns:
        print "    %s" % outfn.fn

    print "TOAs:"
    print "\n".join(toastrs)


if __name__=="__main__":
    parser = utils.DefaultOptions(usage="%prog [OPTIONS] FILES ...", \
                        description="Given a list of PSRCHIVE file names " \
                                    "reduce them so they are ready to " \
                                    "generate TOAs. A single output file " \
                                    "is produced.")
    parser.add_option('-o', '--outname', dest='outfn', type='string', \
                        help="The output (reduced) file's name. " \
                            "(Default: '%(name)s_%(yyyymmdd)s_%(secs)05d_reduced.ar')", \
                        default="%(name)s_%(yyyymmdd)s_%(secs)05d_reduced.ar")
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
    parser.add_option('--getafix', dest='is_asterix', action='store_true', \
                        default=False, \
                        help="If the data are from Effelsberg's Asterix backend " \
                            "guess the receiver used and correct the output " \
                            "archive's header. (Default: False)")
    options, args = parser.parse_args()
    main()
