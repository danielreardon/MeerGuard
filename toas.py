import utils
import config
import errors
import debug

def get_standard(fn):
    """Given an archive file name return the name of the 
        standard profile to use for TOA fitting.

        Input:
            fn: The name of the archive file for which we want a standard.

        Output:
            std: The name of the standard profile.
    """
    hdr = utils.get_header_vals(fn, ['name', 'freq', 'telescop', 'backend', 'receiver'])
    stdfn = "%(name)s_%(telescop)s_%(receiver)s_%(backend)s.std" % hdr)
    stdpath = os.path.join(config.base_standards_dir, hdr['telescop'], \
                            ['receiver'], ['backend'])
    stdfn = os.path.join(stdpath, stdfn)

    if not os.path.isfile(stdfn):
        raise errors.NoStandardProfileError("The standard profile (%s) " \
                                            "cannot be found!" % stdfn)
    return stdfn


def get_toas(fn, nsubint=1, nchan=1, stdfn=None, makediag=True):
    """Get TOAs for the given archive file by running 'pat'.
        If no standard profile is given the location of the 
        stardard will be guessed based on header parameters 
        in the archive.

        Inputs:
            fn: The name of the archive file to produce TOAs for.
            nsubint: Scrunch archive to this many subints, and 
                produce a TOA for each subint. (Default: 1).
            nchan: Scrunch archive to this many channels, and
                produce a TOA for each channel. (Default: 1).
            stdfn: The name of the standard profile to use.
                (Default: Guess location of standard profile.)
            makediag: A boolean value. If True, make diagnostic
                plots by calling 'pat' with the '-t' flag.

        Output:
            toas: A list of TOA strings.
    """
    if stdfn is None:
        stdfn = get_standard(fn)
    basefn = os.path.splitext(fn)[0]
    # If we want to partially scrunch the data call 'pam'
    if nsubint*nchan > 1:
        srunchedfn = basefn + '.scrn.tmp'
        utils.execute("pam --setnsub %d --setnchn %d -e scrn.tmp %s" % \
                        (nsubint, nchan, fn))
        stdout, stderr = utils.execute("pat -s %s -A %s -f %s -K " \
                                        "%s.toa/PNG -t %s" % \
                (stdfn, cfg.toa_method, cfg.toa_format, basefn, srunchedfn))
        if not debug.INTERMEDIATE:
            os.remove(scrunchedfn)
    else:
        stdout, stderr = utils.execute("pat -T -F -s %s -A %s -f %s -K " \
                                        "%s.toa/PNG -t %s" % \
                (stdfn, cfg.toa_method, cfg.toa_format, basefn, fn))
    return stdout
