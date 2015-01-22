#!/usr/bin/env python
import urllib2
import datetime
import sys
import os.path
import warnings

import numpy as np
from scipy import interpolate

from coast_guard import utils
from coast_guard import errors

from pyriseset import utils as rsutils

BASE_MASER_URL = "http://effwww.mpifr-bonn.mpg.de/maser/"


def get_maser_lines(day):
    """Get maser file from Effelsberg for a specific day.
       An error is raised if no file can be found for the given day.

        Inputs:
            day: A Date object, Datetime object, date string, or MJD.

        Outputs:
            lines: The lines from the maser file.
    """
    if (type(day) is int) or (type(day) is float):
        # Assume day is an MJD
        dt = rsutils.mjd_to_datetime(day)
    elif (type(day) is datetime.datetime) or (type(day) is datetime.date):
        pass
    elif (type(day) is str) or (type(day) is unicode):
        try:
            # Assume mjd-string
            mjd = float(day)
            dt = rsutils.mjd_to_datetime(mjd)
        except ValueError:
            # Date-string
            dt = rsutils.parse_datestr(day)
    else:
        raise ValueError("Unrecognized type for day. "
                         "Expected int or float (MJD), "
                         "string (YYYY-MM-DD format), " 
                         "datetime.date object, or "
                         "datetime.datetime object. "
                         "Got %s object." % type(day))

    yyyymmdd = dt.strftime("%Y%m%d")

    data = None
    # First assume data file is in base directory
    urls = [BASE_MASER_URL+"gps%s.txt" % yyyymmdd,
            BASE_MASER_URL+"Year_%s/gps%s.txt" % (dt.strftime("%Y"), yyyymmdd),
           ]
    for url in urls:
        try:
            data = urllib2.urlopen(url)
            break
        except urllib2.HTTPError, e:
            if e.getcode() != 404:
                raise
    else:
        raise errors.FatalCoastGuardError("Cannot find Effelsberg maser " 
                                          "correction file for '%s'. "
                                          "Checked the following URLs:\n"
                                          "    %s" % 
                                          (day, "\n    ".join(urls)))
    lines = data.readlines()
    return lines
    

def parse_maser_file(lines):
    maser = []
    for line in lines:
        split = line.split()
        try:
            dt = datetime.datetime.strptime(" ".join(split[:5]), "%d %m %Y %H %M")
        except ValueError:
            warnings.warn("Skipping badly formatted line:\n    %s" % line,
                          errors.CoastGuardWarning)
        else:
            mjd = rsutils.datetime_to_mjd(dt)
            try:
                correction = float(split[5])
            except ValueError:
                warnings.warn("Skipping badly formatted correction: %s\n"
                              "(Full line: %s)" % (split[5], line.strip()), 
                              errors.CoastGuardWarning)
            else:
                maser.append((mjd, float(split[5])))
    return np.array(maser)


def get_maser_data(day):
    return parse_maser_file(get_maser_lines(day))


def _get_monthly_mjds(start, end, interval, num_per_day):
    mjd = int(start)
    mjds = []
    dt = rsutils.mjd_to_datetime(mjd)
    while mjd <= end:
        mjds.append(mjd)
        month = dt.month+1
        year = dt.year
        if month > 12:
            year += 1
            month -= 12
        dt = dt.replace(year=year, month=month)
        mjd = rsutils.datetime_to_mjd(dt)
    return np.array(mjds)


def get_mjds(start, end, interval, num_per_day):
    if os.path.isfile(interval):
        # Read MJDs from file
        mjds = np.loadtxt(interval, unpack=True, usecols=(0,), comments='#')
    else:
        if interval.lower() == 'monthly':
            imjds = _get_monthly_mjds(start, end, interval, num_per_day)
        else:
            if interval.lower() == 'weekly':
                nn = 7
            elif interval.lower() == 'daily':
                nn = 1
            else:
                nn = int(interval)
            imjds = np.arange(start, end+1, nn)
        fmjd = np.arange(1.0/(2*num_per_day),1,1.0/num_per_day)
        mjds = (imjds[:,np.newaxis] + fmjd).flatten()
    mjds.sort()
    return mjds


def main():
    if args.outfn is not None:
        outfile = open(args.outfn, 'w')
    else:
        outfile = sys.stdout

    mjds = get_mjds(args.start_mjd, args.end_mjd, 
                    args.interval, args.num_per_day)
    curr = None    
    for mjd in mjds:
        imjd = int(mjd)
        if curr != imjd:
            utils.print_info("Getting maser corrections for MJD %05d" % imjd, 1)
            # Get corrections
            data = get_maser_data(imjd)
            get_correction = interpolate.interp1d(data[:,0], data[:,1], 
                                                  kind='nearest')
            curr = imjd
        correction = get_correction(mjd)
        if correction > 0.5:
            correction -= 1
        outfile.write("%.6f\t%.5e\n" % (mjd, correction))

    if args.outfn is not None:
        outfile.close()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Write a clock correction " 
                                                "file for Effelsberg.")
    parser.add_argument("-o", dest='outfn', default=None,
                        help="Output file. (Default: stdout)")
    parser.add_argument("-s", "--start-mjd", dest='start_mjd', 
                        type=int, default=49718,
                        help="MJD for start of clock correction file. "
                             "(Default: 49718 - i.e. Jan. 1, 1995)")
    parser.add_argument("-e", "--end-mjd", dest='end_mjd',
                        type=int, default=None,
                        help="MJD for end of clock correction file. "
                             "(Default: today)")
    parser.add_argument("-D", "--day-interval", dest='interval',
                        type=str, default='monthly',
                        help="Interval between days on which to include " 
                             "clock corrections. Value can be: 'monthly', " 
                             "'weekly', 'daily', an integer, or the name "
                             "of a file containing MJDs. (Default: Monthly)")
    parser.add_argument("-n", "--num-per-day", dest='num_per_day',
                        type=int, default=3,
                        help="Number of corrections to include for each day. "
                             "NOTE: This is ignored if the '-d/--day-interval' "
                             "argument is provided with a file containing a "
                             "list of MJDs. (Default: 3 corrections per day)")
    args = parser.parse_args()
    main()
