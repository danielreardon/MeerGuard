#!/usr/bin/env python


import utils
import database

import matplotlib.pyplot as plt


def get_obs_mjds(db, psrs):
    """Get a list of MJDs for the observations of the
        given pulsars.

        Inputs:
            db: A database connection object.
            psrs: A list of pulsar names.

        Outputs:
            obs_mjds: A dictionary of lists of observation MJDs.
    """
    prefnames = [utils.get_prefname(name) for name in psrs]
    with db.transaction() as conn:
        select = db.select([db.obs.c.sourcename,
                            db.obs.c.start_mjd]).\
                    where(db.obs.c.sourcename.in_(prefnames))
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()
    obs_mjds = {}
    for row in rows:
        mjdlist = obs_mjds.setdefault(row['sourcename'], [])
        mjdlist.append(row['start_mjd'])
    return obs_mjds


def main():
    db = database.Database()
    obs_mjds = get_obs_mjds(db, args.pulsars)
    mjds = []
    ipsr = []
    psrnames = sorted(obs_mjds.keys())
    for ii, psrname in enumerate(psrnames):
        print psrname, len(obs_mjds[psrname])
        ipsr.extend([ii]*len(obs_mjds[psrname]))
        mjds.extend(sorted(obs_mjds[psrname]))
    plt.scatter(mjds, ipsr)
    plt.yticks(range(len(psrnames)), psrnames)
    plt.xlabel('MJD')
    plt.show()

if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Plot cadence of pulsar "
                                                "observations.")
    parser.add_argument('pulsars', type=str, nargs='+',
                        help='Pulsar to plot cadence for.')
    args = parser.parse_args()
    main()
