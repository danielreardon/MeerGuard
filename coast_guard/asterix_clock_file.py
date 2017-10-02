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

# Asterix/PSRix clock offsets
# Determined from fitting JUMPs
#CLOCK_OFFSETS = [(-np.inf, 56230, 0.0), # This serves as the reference 
#                                     # time/phase for the other offsets.
#                 (56230, 56500, -0.0972851),
#                 (56500, 56720, -0.4092691),
#                 (56720, 56981, -0.000000612),
#                 (56981, np.inf, -0.000000127),
#                ]
# Determined from LEAP by Kuo (see email Apr 6, 2015)
CLOCK_OFFSETS = [(-np.inf, 56230, 0.0), # This serves as the reference 
                                        # time/phase for the other offsets.
                 #(56230, 56500, -0.097284254),
                 #(56500, 56720, -0.409268005),
                 (56230, 56490, -0.097284254),
                 (56490, 56720, -0.409268005),
                 (56720, 56981, -0.000000921),
                 (56981, np.inf, -0.000000304),
                ]

class NoMaserFileFound(Exception):
    pass

class NoMaserData(Exception):
    pass


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
        raise NoMaserFileFound("Cannot find Effelsberg maser " 
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
    data = parse_maser_file(get_maser_lines(day))
    if not len(data):
        raise NoMaserData("No data parsed from maser file for %s" % day)
    return data


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


def get_mjds(start, end, interval, num_per_day, additional=None):
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
        if additional:
            mjds = np.concatenate([mjds, additional])
    mjds.sort()
    return mjds


def maser_gps_fit_factory(allparams):
    def get_correction(mjd):
        corr = None
        for params in allparams:
            if (mjd >= params[0]) and (mjd < params[1]):
                mjdscale, mjdoff, clkscale, clkoff = params[2:6]
                coeffs = params[6:][::-1]
                val = np.polyval(coeffs, (mjd-mjdoff)/mjdscale)
                corr = val*clkscale + clkoff
                return corr
        return corr
    return get_correction


def main():
    if args.outfn is not None:
        outfile = open(args.outfn, 'w')
    else:
        outfile = sys.stdout

    if args.interp_method not in ("linear", "nearest", "quadratic", "cubic", "median"):
        # Assume a file of paramters is provided
        fitfn = args.interp_method
        if os.path.isfile(fitfn):
            args.interp_method = "file"
            fitparams = np.loadtxt(fitfn, unpack=False)
            get_correction = maser_gps_fit_factory(fitparams)
        else:
            raise ValueError("Interpolation method (%s) is not recognized "
                             "nor is it a file of parameters!" % args.interp_method)

    if args.include_clock_offsets:
        clock_offsets = CLOCK_OFFSETS
        clock_mjds = [float(clk[0]) for clk in CLOCK_OFFSETS if np.isfinite(clk[0])] + \
                     [float(clk[1]) for clk in CLOCK_OFFSETS if np.isfinite(clk[1])]
        clock_mjds = sorted(set(clock_mjds))
    else:
        clock_offsets = []
        clock_mjds = []

    end_mjd = args.end_mjd
    if end_mjd is None:
        end_mjd = rsutils.mjdnow()
    mjds = get_mjds(args.start_mjd, end_mjd, 
                    args.interval, args.num_per_day, 
                    additional=clock_mjds)
    curr = None
    if args.include_clock_offsets:
        outfile.write("# UTC(EFFIX) UTC(GPS)\n")
        outfile.write("# Effelsberg Asterix/PSRix clock correction file\n")
    else:
        outfile.write("# UTC(EFF) UTC(GPS)\n")
        outfile.write("# Effelsberg clock correction file\n")
    outfile.write("# Generated on %s with %s (by P. Lazarus) \n" % 
                  (datetime.datetime.now().strftime("%B %d, %Y"), __file__))
    outfile.write("# The following clock offsets are included:\n")
    if clock_offsets:
        for start_mjd, end_mjd, clkoff in clock_offsets:
            outfile.write("#    MJD: %5s to %5s; offset=%g s\n" % (start_mjd, end_mjd, clkoff))
    else:
        outfile.write("#    None\n")
    outfile.write("#\n")
   
    if args.include:
        with open(args.include, 'r') as inclff:
            for line in inclff:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                mjd = float(line.split()[0])
                if mjd < mjds[0]:
                    outfile.write(line+"\n")

    # Include clock offsets
    clkoff = 0
           
    for mjd in mjds:
        imjd = int(mjd)
        try:
            if args.interp_method == "file":
                # File containing parameters
                # get_correction is defined above when the fit-file is read
                pass
            else:
                if curr != imjd:
                    utils.print_info("Getting maser corrections for MJD %05d" % imjd, 1)
                    # Get corrections
                    data = get_maser_data(imjd)
                    if args.interp_method == "median":
                        get_correction = lambda mjd: np.median(data[:,1])
                    elif args.interp_method in ("linear", "nearest", "quadratic", "cubic"):
                        get_correction = interpolate.interp1d(data[:,0], data[:,1], 
                                                              kind=args.interp_method)
                curr = imjd
            correction = get_correction(mjd)
            if correction > 0.5:
                correction -= 1
            elif correction < -0.5:
                correction += 1
                correction = -correction
            
            if float(mjd) in clock_mjds:
                if clkoff:
                    outfile.write("%.6f\t%.12e # Clock offset: %g s\n" % (mjd, correction+clkoff, clkoff))
                else:
                    outfile.write("%.6f\t%.12e\n" % (mjd, correction))
            
            for start_mjd, end_mjd, offset in clock_offsets:
                if start_mjd <= mjd < end_mjd:
                    clkoff = offset
                    break
            if clkoff:
                outfile.write("%.6f\t%.12e # Clock offset: %g s\n" % (mjd, correction+clkoff, clkoff))
            else:
                outfile.write("%.6f\t%.12e\n" % (mjd, correction))
        except NoMaserFileFound:
            outfile.write("# Cannot determine clock correction for MJD %g: " \
                          "No maser file found for MJD %d\n" % (mjd, imjd))
        except NoMaserData:
            outfile.write("# Cannot determine clock correction for MJD %g: " \
                          "No maser data parsed from file for MJD %d\n" % (mjd, imjd))
        except ValueError, exc:
            outfile.write("# Cannot determine clock correction for MJD %g: " \
                          "%s\n" % (mjd, str(exc)))

    # Write clock correction for far in future (to allow for extrapolation?)
    outfile.write("60000.00000 0.00000e+00\n")
    if args.outfn is not None:
        outfile.close()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Write a clock correction " 
                                                "file for Effelsberg.")
    parser.add_argument("-o", dest='outfn', default=None,
                        help="Output file. (Default: stdout)")
    parser.add_argument("-s", "--start-mjd", dest='start_mjd', 
                        type=int, default=55562,
                        help="MJD for start of clock correction file. "
                             "(Default: 55562 - i.e. Jan. 1, 2011, "
                             "the year Asterix was installed)")
    parser.add_argument("-e", "--end-mjd", dest='end_mjd',
                        type=int, default=None,
                        help="MJD for end of clock correction file. "
                             "(Default: today)")
    parser.add_argument("--no-clock-offsets", dest="include_clock_offsets",
                        action='store_false',
                        help="Do not include the Asterix clock offsets in "
                             "the clock corrections. (Default: include "
                             "Asterix clock offsets.)")
    parser.add_argument("--include", dest='include', 
                        type=str, default=None,
                        help="An existing clock file to include in the "
                             "newly generate clock file. (Default: don't "
                             "include any corrections from another file)")
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
    parser.add_argument("-I", "--interp-method", dest='interp_method',
                        type=str, default="nearest",
                        help="Method to use when interpolating MASER-to-GPS difference data. "
                             "Recognized values are: "
                             "'median' - Median of the daily values; "
                             "'linear' - Linear interpolation; "
                             "'nearest' - Use the nearest data point; "
                             "'quadratic' - 2nd order spline; "
                             "'cubic' - 3rd order spline; "
                             "otherwise, assume name of file containing "
                             "polynomial coefficients that provide the "
                             "difference between the maser and GPS. "
                             "(Default: 'nearest')")
    args = parser.parse_args()
    main()
