#!/usr/bin/env python

import os

from coast_guard import utils
from coast_guard import database


def main():
    psrname = utils.get_prefname(args.psr)
    print "Will delete database rows (and referenced files) " \
          "for source name %s" % psrname

    db = database.Database()
    with db.transaction() as conn:
        select = db.select([db.obs.c.obs_id]).\
                        where(db.obs.c.sourcename == psrname)
        results = conn.execute(select)
        obsrows = results.fetchall()
        results.close()

        select = db.select([db.files.c.file_id,
                            db.files.c.filepath,
                            db.files.c.filename],
                           from_obj=[db.files.\
                                outerjoin(db.obs,
                                    onclause=db.files.c.obs_id ==
                                          db.obs.c.obs_id)]).\
                    where(db.obs.c.sourcename == psrname).\
                    order_by(db.files.c.file_id.desc())
        results = conn.execute(select)
        filerows = results.fetchall()
        results.close()

        select = db.select([db.logs.c.log_id,
                            db.logs.c.logpath,
                            db.logs.c.logname],
                           from_obj=[db.logs.\
                                outerjoin(db.obs,
                                    onclause=db.logs.c.obs_id ==
                                          db.obs.c.obs_id)]).\
                    where(db.obs.c.sourcename == psrname)
        results = conn.execute(select)
        logsrows = results.fetchall()
        results.close()

        select = db.select([db.diagnostics.c.diagnostic_id,
                            db.diagnostics.c.diagnosticpath,
                            db.diagnostics.c.diagnosticname],
                           from_obj=[db.diagnostics.\
                               outerjoin(db.files,
                                    onclause=db.diagnostics.c.file_id ==
                                        db.files.c.file_id).\
                               outerjoin(db.obs,
                                    onclause=db.files.c.obs_id ==
                                        db.obs.c.obs_id)]).\
                    where(db.obs.c.sourcename == psrname)
        results = conn.execute(select)
        diagrows = results.fetchall()
        results.close()

        select = db.select([db.obs.c.dir_id]).\
                    where(db.obs.c.sourcename == psrname).\
                    distinct(db.obs.c.dir_id)
        results = conn.execute(select)
        dirrows = results.fetchall()
        results.close()

        print "There are %d entires to be removed from files table" % \
            len(filerows)
        print "There are %d entires to be removed from obs table" % \
            len(obsrows)
        print "There are %d entries to be removed from directories table" % \
            len(dirrows)
        print "There are %d entires to be removed from logs table" % \
            len(logsrows)
        print "There are %d entries to be remove from diagnostics table" % \
            len(diagrows)

        if not args.dryrun:
            # Remove diagnostics entries
            print "Removing diagnostic rows"
            for row in utils.show_progress(diagrows, width=50, tot=len(diagrows)):
                diagnostic_id = row['diagnostic_id']
                ff = os.path.join(row['diagnosticpath'], row['diagnosticname'])
                try:
                    os.remove(ff)
                except:
                    pass
                delete = db.diagnostics.delete().\
                        where(db.diagnostics.c.diagnostic_id == diagnostic_id)
                conn.execute(delete)
                results.close()

            # Remove files entries
            print "Removing file rows"
            for row in utils.show_progress(filerows, width=50, tot=len(filerows)):
                file_id = row['file_id']
                ff = os.path.join(row['filepath'], row['filename'])
                try:
                    os.remove(ff)
                except:
                    pass
                delete = db.files.delete().\
                        where(db.files.c.file_id == file_id)
                conn.execute(delete)
                results.close()

            # Remove logs entries
            print "Removing log rows"
            for row in utils.show_progress(logsrows, width=50, tot=len(logsrows)):
                log_id = row['log_id']
                ff = os.path.join(row['logpath'], row['logname'])
                try:
                    os.remove(ff)
                except:
                    pass
                delete = db.logs.delete().\
                        where(db.logs.c.log_id == log_id)
                conn.execute(delete)
                results.close()

            # Remove obs entries
            print "Removing obs rows"
            for row in utils.show_progress(obsrows, width=50, tot=len(obsrows)):
                obs_id = row['obs_id']
                delete = db.obs.delete().\
                        where(db.obs.c.obs_id == obs_id)
                conn.execute(delete)
                results.close()

            # Remove directories entries
            print "Removing directories rows"
            for row in utils.show_progress(dirrows, width=50, tot=len(dirrows)):
                dir_id = row['dir_id']
                delete = db.directories.delete().\
                        where(db.directories.c.dir_id == dir_id)
                conn.execute(delete)
                results.close()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Delete recent files.")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true",
                        help="Show some information and do not delete "
                             "files or database rows. "
                             "(Default: delete files/rows)")
    parser.add_argument("-p", "--psr", dest="psr",
                        required=True,
                        help="Pulsar for which to find entries to delete.")
    args = parser.parse_args()
    main()


