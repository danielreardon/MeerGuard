import os
import os.path

import utils
import config
import errors

def get_standard(arf, base_standards_dir=None):
    """Given an archive file name return the name of the 
        standard profile to use for TOA fitting.

        Input:
            arf: An ArchiveFile object for which we want to get a standard.
            base_stddir: The base directory containing standard profiles.
                (Default: The current working directory)

        Output:
            std: The name of the standard profile.
    """
    if base_standards_dir is None:
        base_standards_dir = config.cfg.base_standards_dir

    stdfn = utils.get_outfn("%(name)s_%(telescop)s_%(rcvr)s_%(backend)s.std", arf)
    stdfn = stdfn.capitalize() # J/B should be capitalized, all the rest lower case
    stdpath = os.path.join(base_standards_dir, arf['telescop'].lower(), \
                            arf['rcvr'].lower(), arf['backend'].lower())
    stdfn = os.path.join(stdpath, stdfn)

    return stdfn


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
    patcmd = "pat -s %s -A %s -f %s " % (stdfn, method, fmt)
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
            raise errors.NoStandardProfileError("The standard profile (%s) " \
                                            "cannot be found!" % stdfn)
        toastrs = get_toas(arf.fn, stdfn)
        for toastr in toastrs:
            print toastr


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
    options, args = parser.parse_args()
    main()
