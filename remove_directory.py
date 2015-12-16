#!/usr/bin/env python

import sys
import os.path
import datetime
import shutil

from coast_guard import config
from coast_guard import utils
from coast_guard import database


def dump_db_entries(db, dir_id, obs_ids=None, log_ids=None, file_ids=None, diag_ids=None):
    dumps = []
    stdout, stderr = utils.execute(["mysqldump", "--port=%d" % db.engine.url.port,
                                    "--password=%s" % db.engine.url.password,
                                    "--user=%s" % db.engine.url.username,
                                    "--host=%s" % db.engine.url.host,
                                    db.engine.url.database, "directories",
                                    "--where", "dir_id=%d" % dir_id])
    dumps.append(stdout)
    if obs_ids:
        stdout, stderr = utils.execute(["mysqldump", "--port=%d" % db.engine.url.port,
                                        "--password=%s" % db.engine.url.password,
                                        "--user=%s" % db.engine.url.username,
                                        "--host=%s" % db.engine.url.host,
                                        db.engine.url.database, "obs",
                                        "--where", "obs_id IN (%s)" % 
                                        ",".join(["%d" % xx for xx in obs_ids])])
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


def get_obsinfo(db, dir_id):
    # Get obs IDs for this directory
    with db.transaction() as conn:
        select = db.select([db.obs]).\
                    where(db.obs.c.dir_id==dir_id).\
                    order_by(db.obs.c.obs_id.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()

    obs_ids = [row['obs_id'] for row in rows]
    return obs_ids


def get_loginfo(db, obs_ids):
    # Get log IDs for this obs
    with db.transaction() as conn:
        select = db.select([db.logs]).\
                    where(db.logs.c.obs_id.in_(obs_ids)).\
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


def get_fileinfo(db, obs_ids):
    # Get file IDs for this obs
    with db.transaction() as conn:
        select = db.select([db.files]).\
                    where(db.files.c.obs_id.in_(obs_ids)).\
                    order_by(db.files.c.file_id.asc())
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    file_ids = [row['file_id'] for row in rows]
    fns = [os.path.join(row['filepath'], row['filename']) for row in rows]
    return file_ids, fns


def get_id_from_dir(path):
    path = os.path.abspath(path)
    db = database.Database()
    with db.transaction() as conn:
        select = db.select([db.directories]).\
                    where(db.directories.c.path==path)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    if len(rows) != 1:
        raise ValueError("Bad number (%d) of rows for Directory = %s!" % 
                         (len(rows), path))
    return rows[0]['dir_id']

            
def get_dir_from_id(dir_id):
    db = database.Database()
    with db.transaction() as conn:
        select = db.select([db.directories]).\
                    where(db.directories.c.dir_id==dir_id)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    if len(rows) != 1:
        raise ValueError("Bad number (%d) of rows for Directory ID = %d!" % 
                         (len(rows), dir_id))
    return rows[0]['path']

            
def main():
    if args.dir_id is not None:
        # Get directory path from database
        dir_toremove = get_dir_from_id(args.dir_id)
        dir_id = arg.dir_id
    else:
        dir_toremove = os.path.join(config.base_rawdata_dir, args.dir)
        dir_id = get_id_from_dir(args.dir)
    if not dir_toremove.startswith(config.base_rawdata_dir):
        raise ValueError("Directory to remove (%s) is not in the raw "
                         "data directory (%s)" % 
                         (dir_toremove, config.base_rawdata_dir))

    subdirs = [datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S")]
    tmp = dir_toremove
    while tmp and (os.path.abspath(config.base_rawdata_dir) != os.path.abspath(tmp)):
        tmp, tmp2 = os.path.split(tmp)
        subdirs.append(tmp2)
    subdirs.reverse()
    backupdir = os.path.join(config.output_location, "removed", *subdirs)
    print "Will remove database entries for data in %s" % dir_toremove
    print "Back-ups of existing files will be copied to %s" % backupdir
    
    db = database.Database()

    obs_ids = get_obsinfo(db, dir_id)
    print "Will also remove %d observations" % len(obs_ids)
    tmp = ", ".join(["%d" % xx for xx in obs_ids])
    utils.print_info("Obs IDs: %s" % tmp, 1)

    log_ids, logfns = get_loginfo(db, obs_ids)
    assert len(log_ids) == len(logfns)
    print "Will also remove %d logs" % len(log_ids)
    tmp = "\n".join(["Log ID: %d; %s" % xx for xx in zip(log_ids, logfns)])
    utils.print_info(tmp, 1)

    file_ids, fns = get_fileinfo(db, obs_ids)
    assert len(file_ids) == len(fns)
    print "Will also remove %d files" % len(file_ids)
    tmp = "\n".join(["File ID: %d; %s" % xx for xx in zip(file_ids, fns)])
    utils.print_info(tmp, 1)
    
    diag_ids, diagfns = get_diaginfo(db, file_ids)
    assert len(diag_ids) == len(diagfns)
    print "Will also remove %d diagnostics" % len(diag_ids)
    tmp = "\n".join(["Diagnostic ID: %d; %s" % xx for xx in zip(diag_ids, diagfns)])
    utils.print_info(tmp, 1)
    
    mysqldumpstr = dump_db_entries(db, dir_id, obs_ids, log_ids, file_ids, diag_ids)
    utils.print_info("MySQL dump:\n%s" % mysqldumpstr, 2)
    
    if not args.dryrun:
        try:
            # Make back-up directory
            os.makedirs(backupdir)
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
                # Remove file entries 
                # (newest first because of foreign key constraints - parent_file_id column)
                for file_id in reversed(sorted(file_ids)):
                    delete = db.files.delete().\
                                where(db.files.c.file_id == file_id)
                    results = conn.execute(delete)
                    results.close()
                # logs
                delete = db.logs.delete().\
                            where(db.logs.c.log_id.in_(log_ids))
                results = conn.execute(delete)
                results.close()
                # obs
                delete = db.obs.delete().\
                            where(db.obs.c.obs_id.in_(obs_ids))
                results = conn.execute(delete)
                results.close()
                # directory
                delete = db.directories.delete().\
                            where(db.directories.c.dir_id == dir_id)
                results = conn.execute(delete)
                results.close()
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
                    os.rmdir(backupdir)
                except:
                    print "Could not remove back-up dir %s" % backupdir
            raise
        else:
            print "Successfully scrubbed %s (ID: %d)" % (dir_toremove, dir_id)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Remove directory, processed "
                                                "files and database entries.")
   
    dirgroup = parser.add_mutually_exclusive_group()
    dirgroup.add_argument("--dir-id", dest='dir_id', type=int,
                          help="ID of directory to remove's database entry.")
    dirgroup.add_argument("--dir", dest="dir", type=str,
                          help="Raw data directory whose database entries "
                               "should be removed. Note: the raw data "
                               "nor its directory will _not_ be removed.")
    parser.add_argument("-n", "--dry-run", action='store_true', dest='dryrun',
                        help="Don't actually remove database entries or "
                             "move files. (Default: remove and move)")
    args = parser.parse_args()
    main()
