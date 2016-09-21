#!/usr/bin/env python

from coast_guard import calibrate
from coast_guard import database
from coast_guard import utils

def main():
    db = database.Database()
    caldbfn = calibrate.update_caldb(db, args.sourcename, force=True)
    print "Updated %s" % caldbfn


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Forcefully update " \
                        "calibrator database for a given source.")
    parser.add_argument("-n", "--sourcename", dest='sourcename', type=str, \
                        help="Name of source for which to update calibrator database.")
    args = parser.parse_args()
    main()
