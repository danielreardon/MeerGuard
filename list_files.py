#!/usr/bin/env python

import os.path

from coast_guard import database
from coast_guard import utils


def get_loaded_whereclause():
    db = database.Database()
    whereclause = (db.files.c.status == 'done')
    return whereclause


def get_all_whereclause():
    db = database.Database()
    whereclause = True
    return whereclause


def get_calibrated_whereclause():
    db = database.Database()
    whereclause = (db.files.c.stage == 'calibrated') & \
                  (db.files.c.qcpassed == True)
    return whereclause


def get_cleaned_whereclause():
    db = database.Database()
    whereclause = (db.files.c.stage == 'cleaned') & \
                  (db.files.c.qcpassed == True)
    return whereclause


def get_corrected_whereclause():
    db = database.Database()
    whereclause = (db.files.c.stage == 'corrected')
    return whereclause


FILETYPE_TO_WHERE = {'loaded': get_loaded_whereclause,
                     'recent': get_all_whereclause,
                     'corrected': get_corrected_whereclause,
                     'calibrated': get_calibrated_whereclause,
                     'cleaned': get_cleaned_whereclause}


def get_current_files(psrnames, rcvr=None):
    """Get a list of data base rows containing
        file and obs information for the given pulsar,
        filetype and receiver.

        Inputs:
            psrnames: The names of the pulsar to match.
            rcvr: The name of the receiver to match.
                (Default: Match all)

        Outputs:
            rows: A list of rows containing file and obs
                information for each matching file.
    """
    db = database.Database()

    # Select psrs to whereclause
    psrname = utils.get_prefname(psrnames[0])
    tmp = (db.obs.c.sourcename == psrname)
    for psrname in psrnames[1:]:
        psrname = utils.get_prefname(psrname)
        tmp |= (db.obs.c.sourcename == psrname)
    
    whereclause = tmp
    if rcvr is not None:
        whereclause &= (db.obs.c.rcvr == rcvr)

    with db.transaction() as conn:
        select = db.select([db.files, 
                            db.obs.c.dir_id,
                            db.obs.c.sourcename,
                            db.obs.c.obstype,
                            db.obs.c.start_mjd,
                            db.obs.c.length,
                            db.obs.c.bw,
                            db.obs.c.freq,
                            db.obs.c.nsubints,
                            db.obs.c.nsubbands,
                            db.obs.c.obsband,
                            db.obs.c.rcvr],
                    from_obj=[db.obs.\
                        outerjoin(db.files,
                            onclause=(db.files.c.file_id ==
                                        db.obs.c.current_file_id))]).\
                    where(whereclause).\
                    order_by(db.files.c.added.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    return rows


def get_files(psrnames, filetype='current', rcvr=None):
    if filetype in ('all', 'current'):
        rows = get_current_files(psrnames, rcvr)
    else:
        rows = get_files_by_type(psrnames, filetype, rcvr)
    return rows


def get_files_by_type(psrnames, filetype, rcvr=None):
    """Get a list of data base rows containing
        file and obs information for the given pulsar,
        filetype and receiver.

        Inputs:
            psrnames: The names of the pulsar to match.
            filetype: The type of files to match.
            rcvr: The name of the receiver to match.
                (Default: Match all)

        Outputs:
            rows: A list of rows containing file and obs
                information for each matching file.
    """
    db = database.Database()

    # Select psrs to whereclause
    psrname = utils.get_prefname(psrnames[0])
    tmp = (db.obs.c.sourcename == psrname)
    for psrname in psrnames[1:]:
        psrname = utils.get_prefname(psrname)
        tmp |= (db.obs.c.sourcename == psrname)
    
    getwhere = FILETYPE_TO_WHERE[filetype]
    whereclause = tmp & getwhere()

    if rcvr is not None:
        whereclause &= (db.obs.c.rcvr == rcvr)

    with db.transaction() as conn:
        select = db.select([db.files, 
                            db.obs.c.dir_id,
                            db.obs.c.sourcename,
                            db.obs.c.obstype,
                            db.obs.c.start_mjd,
                            db.obs.c.length,
                            db.obs.c.bw,
                            db.obs.c.freq,
                            db.obs.c.nsubints,
                            db.obs.c.nsubbands,
                            db.obs.c.obsband,
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


def get_files_by_id(file_ids):
    """Get a list of data base rows containing
        file and obs information for the given,
        file IDs.

        Inputs:
            file_ids: A list of file IDs to match

        Outputs:
            rows: A list of rows containing file and obs
                information for each matching file.
    """
    db = database.Database()

    with db.transaction() as conn:
        select = db.select([db.files, 
                            db.obs.c.dir_id,
                            db.obs.c.sourcename,
                            db.obs.c.obstype,
                            db.obs.c.start_mjd,
                            db.obs.c.length,
                            db.obs.c.bw,
                            db.obs.c.freq,
                            db.obs.c.nsubints,
                            db.obs.c.nsubbands,
                            db.obs.c.obsband,
                            db.obs.c.rcvr],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=(db.files.c.obs_id ==
                                        db.obs.c.obs_id))]).\
                    where(db.files.c.file_id.in_(args.file_ids)).\
                    order_by(db.files.c.added.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
   
    return rows


def main():
    if args.file_ids:
        rows = get_files_by_id(args.file_ids)
    else:
        rows = get_files(args.psrnames, args.type)
    info = {}
    
    utils.sort_by_keys(rows, args.sortkeys)
    for row in rows:
        if args.fmt is not None:
            print args.fmt.decode('string-escape') % row
        else:
            print os.path.join(row['filepath'], row['filename'])
            utils.print_info("    File ID: %(file_id)d; "
                             "Obs ID: %(obs_id)d; "
                             "Status: %(status)s; "
                             "Stage: %(stage)s; "
                             "QC passed: %(qcpassed)s" % row, 2)
        info['Total'] = info.get('Total', 0)+1
        info['QC Passed'] = info.get('QC Passed', 0)+int(bool(row['qcpassed']))
        info['Status %s' % row['status']] = \
            info.get('Status %s' % row['status'], 0)+1
        info['Stage %s' % row['stage']] = \
            info.get('Stage %s' % row['stage'], 0)+1
    utils.print_info("Summary:\n    %s" %
                     "\n    ".join(["%s: %d" % xx for xx in info.iteritems()]), 1)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="List files generated by "
                                                "the automated pipeline.")
    parser.add_argument('-p', '--psr', dest='psrnames',
                        type=str, action='append',
                        help="The pulsar to grab files for. "
                             "NOTE: Multiple '-p'/'--psr' options may be given")
    parser.add_argument('--type', dest='type', type=str,
                        help='Type of files to list. Options are:'
                             '%s' % sorted(set(FILETYPE_TO_WHERE.keys())))
    parser.add_argument('-F', '--file-id', action='append', dest='file_ids',
                        default=[],
                        help="File ID to match. Multiple -F/--file-id options "
                             "may be provided.")
    parser.add_argument('--sort', dest='sortkeys', metavar='SORTKEY', \
                        action='append', default=['added'], \
                        help="DB column to sort raw data files by. Multiple " \
                            "--sort options can be provided. Options " \
                            "provided later will take precedent " \
                            "over previous options. (Default: Sort " \
                            "by 'added'.)")
    parser.add_argument("--fmt", dest='fmt', default=None,
                        help="Write custom format for each matching file.")
    args = parser.parse_args()
    main()
