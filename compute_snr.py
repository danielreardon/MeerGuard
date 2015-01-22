#!/usr/bin/env python

import os
import sys

from coast_guard import utils
from coast_guard import database


def main():
    db = database.Database()
    with db.transaction() as conn:
        select = db.select([db.files]).\
                    where((db.files.c.snr == None) &
                          (db.files.c.is_deleted == False) &
                          (db.files.c.stage != 'grouped')).\
                    order_by(db.files.c.added.desc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
        for row in utils.show_progress(rows, width=50, tot=len(rows)):
            fn = os.path.join(row['filepath'], row['filename'])
            try:
                snr = utils.get_archive_snr(fn)
            except Exception, e:
                sys.stderr.write("Error when computing SNR of %s."
                                 "%s" % (fn, str(e)))
            else:
                update = db.files.update().\
                            values(snr=snr).\
                            where(db.files.c.file_id == row['file_id'])
                result = conn.execute(update)
                result.close()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Add SNR to files where"
                                                "it is missing.")
    args = parser.parse_args()
    main()