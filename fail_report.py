#!/usr/bin/env python
import os.path

import database
import reduce_data


def get_failed_files(db):
    """Get a list of files with status = 'failed'

        Input:
            db: A Database object.

        Output:
            rows: A list of file rows with status='failed'
    """
    with db.transaction() as conn:
        select = db.select([db.files]).\
                    where(db.files.c.status=='failed')
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()
    return rows


def main():
    db = database.Database()
    for filerow in get_failed_files(db):
        logrow = reduce_data.get_log(db, filerow['group_id'])
        print "File ID: %d - Group ID: %d (%s)" % \
                    (filerow['file_id'], filerow['group_id'], filerow['stage'])
        print "    %s" % os.path.join(filerow['filepath'], filerow['filename'])
        print "    log: %s" % os.path.join(logrow['logpath'], logrow['logname'])
        print ""

if __name__ == '__main__':
    main()
