#!/usr/bin/env python
import datetime
import os.path

from coast_guard import config
from coast_guard import utils
from coast_guard import errors
from coast_guard import database


def get_caldb(db, sourcename):
    """Given a sourcename return the corresponding entry in the
        caldb table.

        Inputs:
            db: A Database object.
            sourcename: The name of the source to match.
                (NOTE: '_R' will be removed from the sourcename, if present)

        Output:
            caldbrow: The caldb's DB row, or None if no caldb entry exists.
    """
    name = utils.get_prefname(sourcename)
    if name.endswith('_R'):
        name = name[:-2]

    with db.transaction() as conn:
        select = db.select([db.caldbs]).\
                    where(db.caldbs.c.sourcename == name)
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()

    if len(rows) == 1:
        return rows[0]
    elif len(rows) == 0:
        return None
    else:
        raise errors.DatabaseError("Bad number of caldb rows (%d) "
                                   "with sourcename='%s'!" %
                                   (len(rows), name))


def update_caldb(db, sourcename, force=False):
    """Check for new calibrator scans. If found update the calibrator database.

        Inputs:
            db: A Database object.
            sourcename: The name of the source to match.
                (NOTE: '_R' will be removed from the sourcename, if present)
            force: Forcefully update the caldb
        
        Outputs:
            caldb: The path to the updated caldb.
    """
    name = utils.get_prefname(sourcename)
    if name.endswith('_R'):
        name = name[:-2]

    # Get the caldb
    caldb = get_caldb(db, name)
    if caldb is None:
        lastupdated = datetime.datetime.min
        outdir = os.path.join(config.output_location, 'caldbs')
        try:
            os.makedirs(outdir)
        except OSError:
            # Directory already exists
            pass
        outfn = '%s.caldb.txt' % name.upper()
        outpath = os.path.join(outdir, outfn)
        insert_new = True
        values = {'sourcename': name,
                  'caldbpath': outdir,
                  'caldbname': outfn}
    else:
        lastupdated = caldb['last_modified']
        outpath = os.path.join(caldb['caldbpath'], caldb['caldbname'])
        insert_new = False
        values = {}

    with db.transaction() as conn:
        if not insert_new:
            # Mark update of caldb as in-progress
            update = db.caldbs.update().\
                        values(status='updating',
                                last_modified=datetime.datetime.now()).\
                        where(db.caldbs.c.caldb_id == caldb['caldb_id'])
            conn.execute(update)

        select = db.select([db.files],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=db.files.c.obs_id ==
                                    db.obs.c.obs_id)]).\
                    where((db.files.c.status.in_(['new', 'done'])) &
                            (db.files.c.stage == 'calibrated') &
                            (db.obs.c.obstype == 'cal') & 
                            (db.obs.c.sourcename == ('%s_R' % name)))
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()

        numnew = 0
        for row in rows:
            if row['added'] > lastupdated:
                numnew += 1

        utils.print_info("Found %d suitable calibrators for %s. "
                         "%d are new." %
                         (len(rows), name, numnew), 2)

        values['numentries'] = len(rows)

        try:
            if numnew or force:
                # Create an updated version of the calibrator database 
                basecaldir = os.path.join(config.output_location,
                                            name.upper()+"_R")
                utils.execute(['pac', '-w', '-u', '.pcal.T', '-k', outpath],
                                dir=basecaldir)
        except:
            #raise
            values['status'] = 'failed'
            if insert_new:
                action = db.caldbs.insert()
            else:
                action = db.caldbs.update().\
                            values(note='%d new entries added' % numnew,
                                    last_modifed=datetime.datetime.now()).\
                            where(db.caldbs.c.caldb_id == caldb['caldb_id'])
            conn.execute(action, values)
        else:
            if insert_new:
                action = db.caldbs.insert()
            else:
                action = db.caldbs.update().\
                            values(status='ready',
                                    note='%d new entries added' % numnew,
                                    last_modified=datetime.datetime.now()).\
                            where(db.caldbs.c.caldb_id == caldb['caldb_id'])
            conn.execute(action, values)
    return outpath


def calibrate(infn, caldbpath, nchans=None):
    """Calibrate a pulsar scan using the calibrator database provided.

        Inputs:
            infn: The name of the archive to calibrate.
            caldbpath: The path to a calibrator database to use.
            nchans: Scrunch the input file to this many
                channels before calibrating. 
                (Default: don't scrunch)

        Outputs:
            polcalfn: The name of the polarization calibrator used.
    """
    if not os.path.isfile(caldbpath):
        raise errors.DataReductionFailed("Calibrator database "
                                         "file not found (%s)." % caldbpath)
    if nchans is not None:
        preproc = ['-j', 'F %d' % nchans]
    else:
        preproc = []
    # Now calibrate, scrunching to the appropriate 
    # number of channels
    stdout, stderr = utils.execute(['pac', '-d', caldbpath,
                                    infn] + preproc)
    
    # Get name of calibrator used
    calfn = None
    lines = stdout.split("\n")
    for ii, line in enumerate(lines):
        if line.strip() == "pac: PolnCalibrator constructed from:":
            calfn = lines[ii+1].strip()
            # Insert log message
            utils.log_message("Polarization calibrator used:"
                              "\n    %s" % calfn, 'info')
            break
    return calfn


def main():
    print ""
    print "        calibrate.py"
    print "     Patrick  Lazarus"
    print ""
    
    if len(args.files):
        print "Number of input files: %d" % len(args.files)
    else:
        raise errors.InputError("No files to calibrate!")

    if args.caldb is None:
        # Prepare to fetch caldb info from the pipeline database
        db = database.Database()
    else:
        caldb = args.caldb

    for fn in args.files:
        if args.caldb is None: 
            arf = utils.ArchiveFile(fn)
            caldb = update_caldb(db, arf['name'], force=True)
        calfn = calibrate(fn, caldb)
        #print "    Output calibrated file: %s" % calfn


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Calibrate Asterix " \
                                    "data files.")
    parser.add_argument('files', nargs='*', help="Files to calibrate.")
    parser.add_argument('--caldb', dest='caldb', type=str, \
                        help="Calibrator database to use. " \
                             "(Default: use the database for this " \
                             "pulsar from the pipeline)", \
                        default=None)
    args = parser.parse_args()
    main()
