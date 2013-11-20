#!/usr/bin/env python

import reduce_data
import database
import utils

def main():
    db = database.Database()
    reduce_data.update_caldb(db, args.sourcename, force=True)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Forcefully update " \
                        "calibrator database for a given source.")
    parser.add_argument("-n", "--sourcename", dest='sourcename', type=str, \
                        help="Name of source for which to update calibrator database.")
    args = parser.parse_args()
    main()
