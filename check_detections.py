#!/usr/bin/env python

import matplotlib.pyplot as plt

from coast_guard import database
from coast_guard import utils


def get_files(psrname):
    """Get a list of database rows the given pulsar.

        Inputs:
            psrname: The name of the pulsar to match.

        Outputs:
            rows: A list of rows containing file and obs
                information for each matching file.
    """
    db = database.Database()
    psrname = utils.get_prefname(psrname)

    whereclause = (db.obs.c.sourcename == psrname) & \
                  (db.files.c.stage == 'cleaned') & \
                  (((db.files.c.status == 'failed') &
                    (db.files.c.qcpassed == False)) |
                   ((db.files.c.status.in_(['new', 'processed', 'done'])) &
                    (db.files.c.qcpassed == True))) & \
                  (db.obs.c.obstype == 'pulsar')
    with db.transaction() as conn:
        select = db.select([db.files,
                            db.obs.c.sourcename,
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
    return rows


def main():
    rows = get_files(args.psrname)
    data = {'detect': ([], []),
            'RFI': ([], []),
            'non-detect': ([], [])}
    for row in rows:
        if row['qcpassed']:
            if row['snr']:
                data['detect'][0].append(row['start_mjd'])
                data['detect'][1].append(row['snr'])
        else:
            if 'RFI' in row['note']:
                data['RFI'][0].append(row['start_mjd'])
                data['RFI'][1].append(0)
            elif 'non-detection' in row['note']:
                data['non-detect'][0].append(row['start_mjd'])
                data['non-detect'][1].append(0)
    plt.figure()
    plt.scatter(data['detect'][0], data['detect'][1], marker='o', c='k')
    print data['RFI'][0], data['RFI'][1]
    plt.scatter(data['RFI'][0], data['RFI'][1], marker='x', c='r')
    plt.scatter(data['non-detect'][0], data['non-detect'][1], marker='o', facecolors=None)
    plt.show()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Check detections for a pulsar.")
    parser.add_argument('-p', '--psr', dest='psrname', type=str,
                        required=True,
                        help='Name of the pulsar to check.')
    args = parser.parse_args()
    main()