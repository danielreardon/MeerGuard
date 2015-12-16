#!/usr/bin/env python

import sys
import os.path
import datetime
import shutil
import operator

from coast_guard import config
from coast_guard import utils
from coast_guard import database


def dump_db_entries(db, obs_id, log_ids=None, file_ids=None, diag_ids=None):
    dumps = []
    stdout, stderr = utils.execute(["mysqldump", "--port=%d" % db.engine.url.port,
                                    "--password=%s" % db.engine.url.password,
                                    "--user=%s" % db.engine.url.username,
                                    "--host=%s" % db.engine.url.host,
                                    db.engine.url.database, "obs",
                                    "--where", "obs_id=%d" % obs_id])
    dumps.append(stdout)
       
    if log_ids:
        stdout, stderr = utils.execute(["mysqldump", "--port=%d" % db.engine.url.port,
                                        "--password=%s" % db.engine.url.password,
                                        "--user=%s" % db.engine.url.username,
                                        "--host=%s" % db.engine.url.host,
                                        db.engine.url.database, "logs",
                                        "--where", "log_id IN (%s)" % 
                                        ",".join(["%d" % xx for xx in log_ids])])
        dumps.append(stdout)

    if file_ids:
        stdout, stderr = utils.execute(["mysqldump", "--port=%d" % db.engine.url.port,
                                        "--password=%s" % db.engine.url.password,
                                        "--user=%s" % db.engine.url.username,
                                        "--host=%s" % db.engine.url.host,
                                        db.engine.url.database, "files",
                                        "--where", "file_id IN (%s)" % 
                                        ",".join(["%d" % xx for xx in file_ids])])
        dumps.append(stdout)

    if diag_ids:
        stdout, stderr = utils.execute(["mysqldump", "--port=%d" % db.engine.url.port,
                                        "--password=%s" % db.engine.url.password,
                                        "--user=%s" % db.engine.url.username,
                                        "--host=%s" % db.engine.url.host,
                                        db.engine.url.database, "diagnostics",
                                        "--where", "diagnostic_id IN (%s)" % 
                                        ",".join(["%d" %xx for xx in diag_ids])])
        dumps.append(stdout)
    return "\n".join(dumps)


def get_obsinfo(db, obs_id):
    # Get info for this obs ID
    with db.transaction() as conn:
        select = db.select([db.obs]).\
                    where(db.obs.c.obs_id==obs_id)
        result = conn.execute(select)
        row = result.fetchone()
        result.close()
    return row


def get_loginfo(db, obs_id):
    # Get log IDs for this obs
    with db.transaction() as conn:
        select = db.select([db.logs]).\
                    where(db.logs.c.obs_id==obs_id).\
                    order_by(db.logs.c.log_id.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()

    log_ids = [row['log_id'] for row in rows]
    logfns = [os.path.join(row['logpath'], row['logname']) for row in rows]
    return log_ids, logfns


def get_diaginfo(db, file_ids):
    # Get diagnostic IDs for this file
    with db.transaction() as conn:
        select = db.select([db.diagnostics]).\
                    where(db.diagnostics.c.file_id.in_(file_ids)).\
                    order_by(db.diagnostics.c.diagnostic_id.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()

    diag_ids = [row['diagnostic_id'] for row in rows]
    diagfns = [os.path.join(row['diagnosticpath'], row['diagnosticname']) for row in rows]
    return diag_ids, diagfns


def get_reattinfo(db, file_ids):
    # Get reattempt IDs for this file
    with db.transaction() as conn:
        select = db.select([db.reattempts]).\
                    where(db.reattempts.c.file_id.in_(file_ids)).\
                    order_by(db.reattempts.c.reatt_id.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
        reatt_ids = [row['reatt_id'] for row in rows]
    return reatt_ids


def get_qcinfo(db, file_ids):
    # Get qc IDs for this file
    with db.transaction() as conn:
        select = db.select([db.qctrl]).\
                    where(db.qctrl.c.file_id.in_(file_ids)).\
                    order_by(db.qctrl.c.qctrl_id.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
        qctrl_ids = [row['qctrl_id'] for row in rows]
    return qctrl_ids


def get_fileinfo(db, obs_id):
    # Get file IDs for this obs
    # Make sure files are sorted such that the newest file is first
    # This is important because of foreign key constraints (ie parent_file_id)
    with db.transaction() as conn:
        select = db.select([db.files]).\
                    where(db.files.c.obs_id==obs_id).\
                    order_by(db.files.c.file_id.desc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    return rows

            
def main():
    db = database.Database()

    obs_id = args.obs_id
    obsinfo = get_obsinfo(db, obs_id)
    datestr = utils.mjd_to_datetime(obsinfo['start_mjd']).strftime("%Y%m%d")
    subdirs = [datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S"), datestr, obsinfo['sourcename']]
    subdirs.reverse()
    backupdir = os.path.join(config.output_location, "removed", *subdirs)
    print "Will remove database entries for obs ID %d" % obs_id
    print "Back-ups of existing files will be copied to %s" % backupdir
    
    log_ids, logfns = get_loginfo(db, obs_id)
    assert len(log_ids) == len(logfns)
    print "Will also remove %d logs" % len(log_ids)
    tmp = "\n".join(["Log ID: %d; %s" % xx for xx in zip(log_ids, logfns)])
    utils.print_info(tmp, 1)

    rows = get_fileinfo(db, obs_id)
    file_ids = [row['file_id'] for row in rows
                if not ((row['stage'] == 'grouped') or 
                ((row['stage'] == 'combined') and (not row['is_deleted'])))]
    file_ids_left = [row['file_id'] for row in rows if row['file_id'] not in file_ids]
    fns = [os.path.join(row['filepath'], row['filename'])
           for row in rows if row['file_id'] in file_ids]
    print "Will also remove %d files" % len(rows)
    tmp = "\n".join(["File ID: %d; %s" % xx for xx in zip(file_ids, fns)])
    utils.print_info(tmp, 1)

    diag_ids, diagfns = get_diaginfo(db, file_ids)
    assert len(diag_ids) == len(diagfns)
    print "Will also remove %d diagnostics" % len(diag_ids)
    tmp = "\n".join(["Diagnostic ID: %d; %s" % xx for xx in zip(diag_ids, diagfns)])
    utils.print_info(tmp, 1)
    
    qctrl_ids = get_qcinfo(db, file_ids)
    print "Will also remove %d quality control entries" % len(qctrl_ids)
    tmp = "\n".join(["QC ID: %d" % xx for xx in qctrl_ids])
    utils.print_info(tmp, 1)
    
    reatt_ids = get_reattinfo(db, file_ids)
    print "Will also remove %d re-attempt entries" % len(reatt_ids)
    tmp = "\n".join(["Re-attempt ID: %d" % xx for xx in reatt_ids])
    utils.print_info(tmp, 1)
    
    mysqldumpstr = dump_db_entries(db, obs_id, log_ids, file_ids, diag_ids)
    utils.print_info("MySQL dump:\n%s" % mysqldumpstr, 2)
    
    if not args.dryrun:
        try:
            # Make back-up directory
            oldumask = os.umask(0007)
            os.makedirs(backupdir)
            os.umask(oldumask)
            # Write mysql dump
            with open(os.path.join(backupdir, "db_entries.sql"), 'w') as ff:
                ff.write(mysqldumpstr)
            # Move files
            for src in fns+logfns+diagfns:
                fn = os.path.basename(src)
                dest = os.path.join(backupdir, fn)
                if os.path.isfile(src):
                    # Make sure file exists (it may have already been deleted)
                    shutil.move(src, dest)
            # Remove entries from the database
            with db.transaction() as conn:
                # Remove diagnostic entries
                delete = db.diagnostics.delete().\
                            where(db.diagnostics.c.diagnostic_id.in_(diag_ids))
                results = conn.execute(delete)
                results.close()
                # Remove any quality control entries in the database
                delete = db.qctrl.delete().\
                            where(db.qctrl.c.qctrl_id.in_(qctrl_ids))
                results = conn.execute(delete)
                results.close()
                # Remove obs' 'current_file_id' entry
                update = db.obs.update().\
                            where(db.obs.c.obs_id == row['obs_id']).\
                            values(current_file_id=None)
                results = conn.execute(update)
                results.close()
                # Remove file entries 
                # (newest first because of foreign key constraints - parent_file_id column)
                for row in rows:
                    if (row['stage'] == 'grouped') or \
                            ((row['stage'] == 'combined') and (not row['is_deleted'])):
                        # Leave grouped files and undeleted combined files
                        pass
                    else:
                        delete = db.files.delete().\
                                    where(db.files.c.file_id == row['file_id'])
                        results = conn.execute(delete)
                        results.close()
                #
                # Do not delete log entries from the database even though log file was moved
                #
                # Update newest file left to have status new
                update = db.files.update().\
                            where(db.files.c.file_id == max(file_ids_left)).\
                            values(status='new',
                                   note='Data are being reprocessed.',
                                   last_modified=datetime.datetime.now())
                conn.execute(update)

        except:
            print "Error encountered! Will attempt to un-move files."
            # Try to unmove files
            for src in fns+logfns+diagfns:
                fn = os.path.basename(src)
                dest = os.path.join(backupdir, fn)
                if os.path.isfile(dest) and not os.path.isfile(src):
                    shutil.move(dest, src)
            if os.path.isdir(backupdir):
                try:
                    os.remove(os.path.join(backupdir, "db_entries.sql"))
                    os.rmdir(backupdir)
                except:
                    print "Could not remove back-up dir %s" % backupdir
            raise
        else:
            print "Successfully reseted obs ID: %d" % obs_id


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Reset observation to be reprocessed.")
   
    parser.add_argument("--obs-id", dest='obs_id', type=int,
                          help="ID of observation to set for reprocessing.")
    parser.add_argument("-n", "--dry-run", action='store_true', dest='dryrun',
                        help="Don't actually remove database entries or "
                             "move files. (Default: remove and move)")
    args = parser.parse_args()
    main()
