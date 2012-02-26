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
