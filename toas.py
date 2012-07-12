#!/usr/bin/env python
import os
import os.path

import utils
import config
import errors

def get_standard(arf, base_standards_dir=None, analytic=None):
    """Given an archive file name return the name of the 
        standard profile to use for TOA fitting.

        Input:
            arf: An ArchiveFile object for which we want to get a standard.
            base_stddir: The base directory containing standard profiles.
                (Default: Use value from configuration file.)
            analytic: True if an analytic profile should be returned.
                (Default: Use value from configuration file.)

        Output:
            std: The name of the standard profile.
    """
    if base_standards_dir is None:
        base_standards_dir = config.cfg.base_standards_dir
    if analytic is None:
        analytic = config.cfg.analytic

    if analytic:
        fn = utils.get_outfn("%(name)s_%(telescop)s_%(rcvr)s_%(backend)s.m", arf)
    else:
        fn = utils.get_outfn("%(name)s_%(telescop)s_%(rcvr)s_%(backend)s.std", arf)
    fn = fn.capitalize() # J/B should be capitalized, all the rest lower case
    path = os.path.join(base_standards_dir, arf['telescop'].lower(), \
                            arf['rcvr'].lower(), arf['backend'].lower())
    fn = os.path.join(path, fn)

    return fn


def get_toas(arf, stdfn, nsubint=None, nchan=None, makediag=True, \
                method=None, fmt=None):
    """Get TOAs for the given archive file by running 'pat'.
        If no standard profile is given the location of the 
        stardard will be guessed based on header parameters 
        in the archive.

        Inputs:
            arf: The ArchiveFile object to produce TOAs for.
            stdfn: The name of the standard profile to use.
            nsubint: Scrunch archive to this many subints, and 
                produce a TOA for each subint.
            nchan: Scrunch archive to this many channels, and
                produce a TOA for each channel.
            makediag: A boolean value. If True, make diagnostic
                plots by calling 'pat' with the '-t' flag.
            method: The method to be used by 'pat'.
            fmt: The output format of TOAs.

        Output:
            toas: A list of TOA strings.
    """
    if nsubint is None:
        nsubint = config.cfg.ntoa_time
    if nchan is None:
        nchan = config.cfg.ntoa_freq
    if method is None:
        method = config.cfg.toa_method
    if fmt is None:
        fmt = config.cfg.toa_format

    # Prepare most of call to 'pat'
    if stdfn.endswith(".std"):
        patcmd = "pat -s %s -A %s -f %s " % (stdfn, method, fmt)
    elif stdfn.endswith(".m"):
        patcmd = "pat -m %s -A %s -f %s " % (stdfn, method, fmt)
    else:
        raise errors.StandardProfileError("Only standards with filename " \
                            "extensions of '.std' and '.m' are recognized. " \
                            "(Standard provided: %s)" % stdfn)

    if makediag:
        patcmd += "-t "

    basefn = os.path.splitext(arf.fn)[0]
    if nsubint*nchan > 1:
        # If we want to partially scrunch the data call 'pam'
        scrunchedfn = basefn + '.scrn.tmp'
        utils.execute("pam --setnsub %d --setnchn %d -e scrn.tmp %s" % \
                        (nsubint, nchan, arf.fn))
        stdout, stderr = utils.execute(patcmd+"-K %s.toa.png/PNG %s" % \
                                        (basefn, scrunchedfn))
        if not config.debug.INTERMEDIATE:
            os.remove(scrunchedfn)
    else:
        stdout, stderr = utils.execute(patcmd+"-T -F -K %s.toa.png/PNG %s" % \
                                        (basefn, arf.fn))
    
    # Parse output
    outlines = [line.strip() for line in stdout.split('\n') if line.strip()]
    if makediag:
        # Remove line that says plots are being made
        toastrs = outlines[1:]
    else:
        toastrs = outlines

    # Check that we have the right number of TOAs
    if len(toastrs) != nsubint*nchan:
        raise errors.ToaError("Wrong number of TOAs parsed from 'pat' output. " \
                            "Expecting %d. Got %d." % \
                            (nsubint*nchan, len(toastrs)))
    return toastrs


def main():
    print ""
    print "          toas.py"
    print "     Patrick  Lazarus"
    print ""
    file_list = args + options.from_glob
    to_exclude = options.excluded_files + options.excluded_by_glob
    to_time = utils.exclude_files(file_list, to_exclude)
    print "Number of input files: %d" % len(to_time)
    
    to_time = [utils.ArchiveFile(fn) for fn in to_time]
    
    # Read configurations
    for arf in to_time:
        config.cfg.load_configs_for_archive(arf)
        stdfn = get_standard(arf)
        if not os.path.isfile(stdfn):
            raise errors.StandardProfileError("The standard profile (%s) " \
                                            "cannot be found!" % stdfn)
        toastrs = get_toas(arf, stdfn)
        for toastr in toastrs:
            flagstrs = [utils.get_outfn(flag, arf) for flag in config.cfg.flags]
            if flagstrs:
                toastr = toastr + " " + " ".join(flagstrs) 
            print toastr


def purge_flags_callback(option, opt_str, value, parser):
    config.cfg.flags[:] = []


if __name__=="__main__":
    parser = utils.DefaultOptions(usage="%prog [OPTIONS] FILES ...", \
                        description="Given a list of PSRCHIVE file names " \
                                    "compute TOAs for each one and print them " \
                                    "to the terminal.")
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
    parser.add_option('--format', dest='toa_format', action='callback', \
                        callback=parser.override_config, type='string', \
                        help="The pat-recognized TOA format to use. (Default: %s)" % \
                             config.cfg.toa_format)
    parser.add_option('--method', dest='toa_method', action='callback', \
                        callback=parser.override_config, type='string', \
                        help="The pat-recognized TOA method to use. (Default: %s)" % \
                             config.cfg.toa_method)
    parser.add_option('--num-subband', dest='ntoa_freq', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="Number of subbands to generate TOAs for. " \
                             "(Default: %d)" % config.cfg.ntoa_freq)
    parser.add_option('--num-subint', dest='ntoa_time', action='callback', \
                        callback=parser.override_config, type='int', \
                        help="Number of subints to generate TOAs for. " \
                             "(Default: %d)" % config.cfg.ntoa_time)
    parser.add_option('--base-std-dir', dest='base_standards_dir', action='callback', \
                        callback=parser.override_config, type='string', \
                        help="The base directory containing standard profiles. " \
                             "(Default: %s)" % config.cfg.base_standards_dir)
    parser.add_option('-t', '--template', dest='template', \
                        help="The template to use. This may be " \
                            "a standard profile (*.std), or an analytic " \
                            "template (*.m). No other filename extensions " \
                            "are recognized. (Default: automatically grab " \
                            "template for this pulsar, telescope, receiver, " \
                            "and backend combination.)")
    parser.add_option('-m', '--use-analytic', dest="analytic", action='callback', \
                        callback=parser.set_override_config, \
                        help="Use an analytic template (*.m). NOTE: This only " \
                            "applies if the template is automatically " \
                            "fetched. (Default: %s)" % \
                            ((config.cfg.analytic and "Use analytic") or \
                                    "Use standard profile"))
    parser.add_option('-s', '--use-standard', dest="analytic", action='callback', \
                        callback=parser.unset_override_config, \
                        help="Use a stardard profile (*.std). NOTE: This only " \
                            "applies if the template is automatically " \
                            "fetched. (Default: %s)" % \
                            ((config.cfg.analytic and "Use analytic") or \
                                    "Use standard profile"))
    parser.add_option('-f', '--flag', dest='flags', \
                        action='append', default=config.cfg.flags, \
                        help="Add the following flag to each TOA line. " \
                            "Be sure to include both the flag name and value. " \
                            "Also, make sure you properly quote your flag+value. " \
                            "(Default: '%s')" % "', '".join(config.cfg.flags))
    parser.add_option('--burn-flags', dest='flags', action='callback', \
                        callback=purge_flags_callback, \
                        help="Remove all flags (including those previously " \
                                "added on the command line).")
    options, args = parser.parse_args()
    main()
