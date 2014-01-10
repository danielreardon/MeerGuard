#!/usr/bin/env python

import os.path

import utils
import errors

def calibrate(infn, caldbpath, nchans=None):
    """Calibrate a pulsar scan using the calibrator database provided.

        Inputs:
            infn: The name of the archive to calibrate.
            caldbpath: The path to a calibrator database to use.
            nchans: Scrunch the input file to this many
                channels before calibrating. 
                (Default: don't scrunch)

        Outputs:
            polcalfn: The name of the polarization calibrator used.
    """
    if not os.path.isfile(caldbpath):
        raise errors.DataReductionFailed("Calibrator database " \
                        "file not found (%s)." % caldbpath)
    if nchans is not None:
        preproc = ['-j', 'F %d' % nchans]
    else:
        preproc = []
    # Now calibrate, scrunching to the appropriate 
    # number of channels
    stdout, stderr = utils.execute(['pac', '-d', caldbpath, \
                    infn] + preproc)
    
    # Get name of calibrator used
    calfn = None
    lines = stdout.split("\n")
    for ii, line in enumerate(lines):
        if line.strip() == "pac: PolnCalibrator constructed from:":
            calfn = lines[ii+1].strip()
            # Insert log message
            utils.log_message("Polarization calibrator used:" \
                                "\n    %s" % calfn, 'info')
            break
    return calfn
