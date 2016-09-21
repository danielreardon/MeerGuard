#!/usr/bin/env python

import datetime
import os

import utils
import database


def main():
    cutoff_date = datetime.date.today() - \
                  datetime.timedelta(days=args.days_ago)
    datestr = cutoff_date.strftime('%Y-%m-%d')
    print "Will delete database rows (and referenced files) " \
          "added on, or after, %s (YYYY-MM-DD)" % datestr

    db = database.Database()
    with db.transaction() as conn:
        select = db.select([db.files.c.filepath,
                            db.files.c.filename,
                            db.files.c.file_id]).\
                    where(db.files.c.added > datestr).\
                    order_by(db.files.c.added.desc())
        results = conn.execute(select)
        filerows = results.fetchall()
        results.close()

        select = db.select ([db.directories.c.path]).\
                    where(db.directories.c.added > datestr).\
                    order_by(db.directories.c.added.desc())
        results = conn.execute(select)
        dirrows = results.fetchall()
        results.close()

        select = db.select ([db.obs.c.obs_id]).\
                        where(db.obs.c.added > datestr).\
                        order_by(db.obs.c.added.desc())
        results = conn.execute(select)
        obsrows = results.fetchall()
        results.close()

        select = db.select ([db.logs.c.log_id]).\
                        where(db.logs.c.added > datestr).\
                        order_by(db.logs.c.added.desc())
        results = conn.execute(select)
        logsrows = results.fetchall()
        results.close()

        print "There are %d entires to be removed from files table" % \
            len(filerows)
        print "There are %d entires to be removed from directories table" % \
            len(dirrows)
        print "There are %d entires to be removed from obs table" % \
            len(obsrows)
        print "There are %d entires to be removed from logs table" % \
            len(logsrows)
        if not args.dryrun:
            for row in utils.show_progress(filerows, width=50, tot=len(filerows)):
                ff = os.path.join(row['filepath'], row['filename'])
                try:
                    os.remove(ff)
                except:
                    pass
                # Remove file entries
                delete = db.files.delete().\
                            where(db.files.c.file_id == row['file_id'])
                results = conn.execute(delete)
                results.close()
            # Remove log entries
            delete = db.logs.delete().\
                        where(db.logs.c.added > datestr)
            results = conn.execute(delete)
            results.close()
            # Remove obs entries
            delete = db.obs.delete().\
                        where(db.obs.c.added > datestr)
            results = conn.execute(delete)
            results.close()
            # Remove directory entries
            delete = db.directories.delete().\
                        where(db.directories.c.added > datestr)
            results = conn.execute(delete)
            results.close()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Delete recent files.")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", \
                        help="Show some information and do not delete "
                             "files or database rows. " \
                             "(Default: delete files/rows)")
    parser.add_argument("-D", "--days-ago", dest="days_ago", type=int, \
                        required=True,
                        help="Number of days to go back to find entries to delete.")
    args = parser.parse_args()
    main()


