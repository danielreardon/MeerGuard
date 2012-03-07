import os
import os.path

import utils
import config
import errors

def get_standard(fn, base_stddir='.'):
    """Given an archive file name return the name of the 
        standard profile to use for TOA fitting.

        Input:
            fn: The name of the archive file for which we want a standard.
            base_stddir: The base directory containing standard profiles.
                (Default: The current working directory)

        Output:
            std: The name of the standard profile.
    """
    hdr = utils.get_header_vals(fn, ['name', 'freq', 'telescop', 'backend', 'rcvr'])
    stdfn = "%s_%s_%s_%s.std" % (hdr['name'].upper(), hdr['telescop'].lower(), \
                            hdr['rcvr'].lower(), hdr['backend'].lower())
    stdpath = os.path.join(base_stddir, hdr['telescop'].lower(), \
                            hdr['rcvr'].lower(), hdr['backend'].lower())
    stdfn = os.path.join(stdpath, stdfn)

    if not os.path.isfile(stdfn):
        raise errors.NoStandardProfileError("The standard profile (%s) " \
                                            "cannot be found!" % stdfn)
    return stdfn


def get_toas(fn, stdfn, nsubint=1, nchan=1, makediag=True, \
                method='PGS', fmt='princeton'):
    """Get TOAs for the given archive file by running 'pat'.
        If no standard profile is given the location of the 
        stardard will be guessed based on header parameters 
        in the archive.

        Inputs:
            fn: The name of the archive file to produce TOAs for.
            stdfn: The name of the standard profile to use.
            nsubint: Scrunch archive to this many subints, and 
                produce a TOA for each subint. (Default: 1).
            nchan: Scrunch archive to this many channels, and
                produce a TOA for each channel. (Default: 1).
            makediag: A boolean value. If True, make diagnostic
                plots by calling 'pat' with the '-t' flag.
            method: The method to be used by 'pat'.
                (Default: PGS)
            fmt: The output format of TOAs.
                (Default: princeton)

        Output:
            toas: A list of TOA strings.
    """
    # Prepare most of call to 'pat'
    patcmd = "pat -s %s -A %s -f %s " % (stdfn, method, fmt)
    if makediag:
        patcmd += "-t "

    basefn = os.path.splitext(fn)[0]
    if nsubint*nchan > 1:
        # If we want to partially scrunch the data call 'pam'
        scrunchedfn = basefn + '.scrn.tmp'
        utils.execute("pam --setnsub %d --setnchn %d -e scrn.tmp %s" % \
                        (nsubint, nchan, fn))
        stdout, stderr = utils.execute(patcmd+"-K %s.toa.png/PNG %s" % \
                                        (basefn, scrunchedfn))
        if not config.debug.INTERMEDIATE:
            os.remove(scrunchedfn)
    else:
        stdout, stderr = utils.execute(patcmd+"-T -F -K %s.toa.png/PNG %s" % \
                                        (basefn, fn))
    
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
    cfg = config.CoastGuardConfigs()
    cfg.get_default_configs()
    for arf in to_time:
        cfg.get_configs_for_archive(arf)
        stdfn = get_standard(arf.fn, cfg.base_standards_dir)
        toastrs = get_toas(arf.fn, stdfn, cfg.ntoa_time, cfg.ntoa_freq, \
                            method=cfg.toa_method, fmt=cfg.toa_format)
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
    options, args = parser.parse_args()
    main()
