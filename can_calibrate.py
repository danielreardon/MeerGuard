#!/usr/bin/env python
import sys
import datetime

from coast_guard import utils
from coast_guard import database
from coast_guard import reduce_data
from coast_guard import calibrate

def get_files(psrnames, retry=False):
    """Get a list of data base rows containing
        file and obs information for the given pulsar.

        Inputs:
            psrnames: The names of the pulsar to match.
            retry: Only get files to retry calibration on.
                (Default: get all file matching psrnames)

        Outputs:
            rows: A list of rows containing file and obs
                information for each matching file.
    """
    db = database.Database()

    # Select psrs to whereclause
    psrname = utils.get_prefname(psrnames[0])
    whereclause = (db.obs.c.sourcename.like(psrname))
    for psrname in psrnames[1:]:
        psrname = utils.get_prefname(psrname)
        whereclause |= (db.obs.c.sourcename.like(psrname))

    if retry:
        whereclause &= (db.files.c.stage=='cleaned') & \
                       (db.files.c.status.in_(['calfail', 'done'])) & \
                       (db.files.c.qcpassed) & \
                       (db.obs.c.obstype=='pulsar') & \
                       (db.files.c.cal_file_id == None)

    with db.transaction() as conn:
        select = db.select([db.files, 
                            db.obs.c.dir_id,
                            db.obs.c.sourcename,
                            db.obs.c.obstype,
                            db.obs.c.start_mjd,
                            db.obs.c.rcvr],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=(db.files.c.obs_id ==
                                        db.obs.c.obs_id))]).\
                    where(whereclause).\
                    order_by(db.files.c.added.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
   
    # Only keep most recently added file for each
    # observation. Rows are sorted in the query above.
    obs_ids = []
    for ii in reversed(range(len(rows))):
        if rows[ii]['obs_id'] in obs_ids:
            rows.pop(ii)
        else:
            obs_ids.append(rows[ii]['obs_id'])
    return rows


def retry(db, file_id):
    with db.transaction() as conn:
        update = db.files.update().\
                    where(db.files.c.file_id == file_id).\
                    values(status='new',
                           last_modified=datetime.datetime.now())
        conn.execute(update)


def main():
    rows = get_files(args.psrnames, retry=args.retry)
    info = {}
   
    psrnameset = set([row['sourcename'] for row in rows])
    utils.sort_by_keys(rows, args.sortkeys)
    db = database.Database()
    with db.transaction() as conn:
        for row in rows:
            if row['obstype'] == 'pulsar':
                calscans = reduce_data.get_potential_polcal_scans(db, row['obs_id'])
                cancal = bool(calscans)
            sys.stdout.write(args.fmt.decode('string-escape') % row)
            if row['obstype'] == 'pulsar':
                sys.stdout.write("\t%s\n" % cancal)
                utils.print_info("Number of potential calibrator scans: %d" % 
                                 len(calscans), 1)
                msg = "    %s" % "\n    ".join(["Obs ID: %d; File ID: %d; %s" % 
                                                (calrow['obs_id'], calrow['file_id'],
                                                 calrow['filename']) 
                                                for calrow in calscans
                                                if type(calrow) is not str])
                utils.print_info(msg, 2)
            else:
                sys.stdout.write("\n")
            if args.retry:
                for desc in reduce_data.get_all_descendents(row['file_id'], db):
                    if (desc['status'] == 'failed') and (desc['stage'] == 'calibrated'):
                        # File has been calibrated, but it failed. Do not retry.
                        cancal = False
                        utils.print_info("Calibration of file %d has previously failed. Will _not_ retry." % row['file_id'], 1)
                if (cancal and (row['status'] != 'failed')) or (not cancal and (row['status'] == 'calfail')):
                    retry(db, row['file_id'])
                    utils.print_info("Will retry calibration of file %d" % row['file_id'], 1)
        if args.retry:
            for name in psrnameset:
                try:
                    reduce_data.reattempt_calibration(db, name)
                    calibrate.update_caldb(db, name, force=True)
                except:
                    pass


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="For each matching file print "
                                                "if it can be polarization "
                                                "calibrated (i.e. sufficient "
                                                "calibration scans are registered "
                                                "in the database)")
    parser.add_argument('-p', '--psr', dest='psrnames',
                        type=str, action='append',
                        help="The pulsar to grab files for. "
                             "NOTE: Multiple '-p'/'--psr' options may be given")
    parser.add_argument('--sort', dest='sortkeys', metavar='SORTKEY', \
                        action='append', default=['added'], \
                        help="DB column to sort raw data files by. Multiple " \
                            "--sort options can be provided. Options " \
                            "provided later will take precedent " \
                            "over previous options. (Default: Sort " \
                            "by 'added'.)")
    parser.add_argument("-r", "--retry-uncal", dest='retry',
                        action='store_true',
                        help="Cleaned files that passed quality control "
                             "that can be calibrated, but have not been "
                             "should be marked as 'calfail' instead of "
                             "'new' so calibration will be reattempted.")
    parser.add_argument("--fmt", dest='fmt', default='%(filename)s',
                        help="Write custom format for each matching file.")
    args = parser.parse_args()
    main()
