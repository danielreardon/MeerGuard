#!/usr/bin/env python

import multiprocessing
import traceback
import warnings
import tempfile
import datetime
import shutil
import time
import glob
import sys
import os

import config
import utils
import diagnose
import cleaners
import combine
import database
import errors
import debug
import log
import correct
import calibrate

import pyriseset as rs

import toaster.config
import toaster.debug
import toaster.errors
from toaster.toolkit.rawfiles import load_rawfile

# A lock for each calibrator database file
# The multiprocessing.Lock objects are created on demand
CALDB_LOCKS = {}

STAGE_TO_EXT = {'combined': '.cmb',
                'grouped': '.list.txt',
                'cleaned': '.clean',
                'corrected': '.corr'}

TWO_HRS_IN_DAYS = 2.0/24.0

SOURCELISTS = {'epta': ['J0030+0451', 'J0218+4232', 'J0613-0200', 
                        'J0621+1002', 'J0751+1807', 'J1012+5307', 
                        'J1022+1001', 'J1024-0719', 'J1600-3053', 
                        'J1640+2224', 'J1643-1224', 'J1713+0747', 
                        'J1730-2304', 'J1741+1351', 'J1744-1134', 
                        'J1853+1303', 'J1857+0943', 'J1911+1347', 
                        'J1918-0642', 'J1939+2134', 'J2010-1323', 
                        'J2145-0750', 'J2229+2643', 'J2317+1439', 
                        'J2322+2057', 'J0340+4129', 'J2017+0603', 
                        'J2043+1711', 'J2234+0944', 'J0023+0923'],
          'priority1': ['J0613-0200', 'J1012+5307', 'J1022+1001', 
                        'J1024-0719', 'J1600-3053', 'J1640+2224', 
                        'J1643-1224', 'J1713+0747', 'J1730-2304', 
                        'J1744-1134', 'J1853+1303', 'J1857+0943', 
                        'J1911+1347', 'J1918-0642', 'J1939+2134', 
                        'J2145-0750', 'J2317+1439'], 
                'mou': [#'J1946+3414', 'J1832-0836', 'J2205+6015', 
                        'J1125+7819', 'J0742+6620', 'J1710+4923', 
                        'J0636+5128', 'J2234+0611', 'J0931-1902']}

PARFILES = {'J0737-3039A': '/media/part1/plazarus/timing/asterix/'
                           'testing/parfiles/to_install/0737-3039A.par'}


def load_directories(db, *args, **kwargs):
    """Search for directories containing asterix data.
        For each newly found entry, insert a row in the
        database.

        Input:
            db: Database object to use.
            ** Additional arguments are passed on to 'get_rawdata_dirs' **

        Output:
            ninserts: Number of new directories inserted.
    """
    # Get add-time of most recently added directory DB entry
    with db.transaction() as conn:
        select = db.select([db.directories.c.added]).\
                    order_by(db.directories.c.added.desc()).\
                    limit(1)
        results = conn.execute(select)
        row = results.fetchone()
        results.close()
    if row is None:
        most_recent_addtime = 0
    else:
        most_recent_addtime = time.mktime(row['added'].timetuple())

    ninserts = 0
    dirs = get_rawdata_dirs(*args, **kwargs)
    nn = len(dirs)
    for ii, path in utils.show_progress(enumerate(dirs), tot=nn, width=50):
        if os.path.getmtime(path) > most_recent_addtime:
            # Only try to add new entries
            try:
                with db.transaction() as conn:
                    insert = db.directories.insert().\
                            values(path=path)
                    # 'directories.path' is constrained to be unique, so
                    # trying to insert a directory that already exists
                    # will result in an error, which will be automatically
                    # rolled back by the context manager (i.e. no new
                    # database entry will be inserted)
                    conn.execute(insert)
            except:
                pass
            else:
                # The following line is only reached if the execution
                # above doesn't raise an exception
                ninserts += 1
    return ninserts


def load_groups(dirrow):
    """Given a row from the DB's directories table create a group 
        listing from the asterix data stored in the directories 
        and load it into the database.

        Inputs:
            dirrow: A row from the directories table.

        Outputs:
            ninserts: The number of group rows inserted.
    """
    tmplogfile, tmplogfn = tempfile.mkstemp(suffix='.log',
                                            dir=config.tmp_directory)
    os.close(tmplogfile)
    log.setup_logger(tmplogfn)

    db = database.Database()
    path = dirrow['path']
    dir_id = dirrow['dir_id']
    # Mark as running
    with db.transaction() as conn:
        update = db.directories.update().\
                    where(db.directories.c.dir_id == dir_id).\
                    values(status='running',
                            last_modified=datetime.datetime.now())
        conn.execute(update)
    if dirrow['status'] != 'new':
        return errors.BadStatusError("Groupings can only be "
                                     "generated for 'directory' entries "
                                     "with status 'new'. (The status of "
                                     "Dir ID %d is '%s'.)" %
                                     (dir_id, dirrow['status']))
    try:
        ninserts = 0
        values = []
        obsinfo = []
        logfns = []
        for dirs, fns in zip(*make_groups(path)):
            fns.sort()
            arf = utils.ArchiveFile(os.path.join(dirs[0], fns[0]))
            listoutdir = os.path.join(config.output_location, 'groups', arf['name'])
            try:
                os.makedirs(listoutdir)
            except OSError:
                # Directory already exists
                pass
            logoutdir = os.path.join(config.output_location, 'logs', arf['name'])

            try:
                os.makedirs(logoutdir)
            except OSError:
                # Directory already exists
                pass
            baseoutname = "%s_%s_%s_%05d_%dsubints" % (arf['name'],
                                                       arf['band'],
                                                       arf['yyyymmdd'],
                                                       arf['secs'],
                                                       len(fns))
            listfn = os.path.join(listoutdir, baseoutname+'.txt')
            logfn = os.path.join(logoutdir, baseoutname+'.log')
            logfns.append(logfn)
            combine.write_listing(dirs, fns, listfn)
            listpath, listname = os.path.split(listfn)
            if arf['name'].endswith("_R"):
                obstype = 'cal'
            else:
                obstype = 'pulsar'
            obsinfo.append({'sourcename': arf['name'],
                            'start_mjd': arf['mjd'],
                            'obstype': obstype})

            values.append({'filepath': listpath,
                           'filename': listname,
                           'stage': 'grouped',
                           'md5sum': utils.get_md5sum(listfn),
                           'filesize': os.path.getsize(listfn)})
    except Exception as exc:
        utils.print_info("Exception caught while working on Dir ID %d" %
                            dir_id, 0)
        shutil.copy(tmplogfn, os.path.join(config.output_location, 'logs',
                                           "dir%d.log" % dir_id))
        # Add ID number to exception arguments
        exc.args = (exc.args[0] + "\n(Dir ID: %d)" % dir_id,)
        if isinstance(exc, (errors.CoastGuardError,
                            errors.FatalCoastGuardError)):
            msg = exc.get_message()
        else:
            msg = str(exc)
            utils.log_message(traceback.format_exc(), 'error')
        with db.transaction() as conn:
            update = db.directories.update().\
                        where(db.directories.c.dir_id == dir_id).\
                        values(status='failed',
                                note='Grouping failed! %s: %s' %
                                     (type(exc).__name__, msg),
                                last_modified=datetime.datetime.now())
            conn.execute(update)
        raise
    else:
        with db.transaction() as conn:
            version_id = utils.get_version_id(db)
            for obs, vals, logfn in zip(obsinfo, values, logfns):
                # Insert obs
                insert = db.obs.insert().\
                            values(dir_id=dir_id)
                result = conn.execute(insert, obs)
                obs_id = result.inserted_primary_key[0]
                # Insert file
                insert = db.files.insert().\
                            values(obs_id=obs_id)
                result = conn.execute(insert, vals)
                file_id = result.inserted_primary_key[0]
                # Insert log
                shutil.copy(tmplogfn, logfn)
                insert = db.logs.insert().\
                            values(obs_id=obs_id,
                                   logpath=os.path.dirname(logfn),
                                   logname=os.path.basename(logfn))
                conn.execute(insert)
            update = db.directories.update().\
                        where(db.directories.c.dir_id == dir_id).\
                        values(status='processed',
                                last_modified=datetime.datetime.now())
            conn.execute(update)
        ninserts += len(values)
    finally:
        if os.path.isfile(tmplogfn):
            os.remove(tmplogfn)
    return ninserts


def load_combined_file(filerow):
    """Given a row from the DB's files table create a combined
        archive and load it into the database.

        Input:
            filerow: A row from the files table.

        Outputs:
            file_id: The ID of newly loaded 'combined' file.
    """
    db = database.Database()
    parent_file_id = filerow['file_id']
    obs_id = filerow['obs_id']

    logrow = get_log(db, obs_id)
    log_id = logrow['log_id']
    logfn = os.path.join(logrow['logpath'], logrow['logname'])
    log.setup_logger(logfn)

    # Mark as running
    with db.transaction() as conn:
        update = db.files.update().\
                    where(db.files.c.file_id == parent_file_id).\
                    values(status='running',
                            last_modified=datetime.datetime.now())
        conn.execute(update)
    if filerow['status'] != 'new':
        return errors.BadStatusError("Combined files can only be "
                                     "generated from 'files' entries "
                                     "with status 'new'. (The status of "
                                     "File ID %d is '%s'.)" %
                                     (parent_file_id, filerow['status']))
    fn = os.path.join(filerow['filepath'], filerow['filename'])
    try:
        subdirs, subints = combine.read_listing(fn)
        arf = utils.ArchiveFile(os.path.join(subdirs[0], subints[0]))
        # Combine the now-prepped subints
        cmbdir = os.path.join(config.output_location, arf['name'], 'combined')
        try:
            os.makedirs(cmbdir)
        except OSError:
            # Directory already exists
            pass
        if arf['name'] in PARFILES:
            parfn = PARFILES[arf['name']]
        else:
            parfn = None
        cmbfn = make_combined_file(subdirs, subints, outdir=cmbdir, parfn=parfn)

        # Pre-compute values to insert because some might be
        # slow to generate
        arf = utils.ArchiveFile(cmbfn)
        if arf['nchan'] > 512:
            note = "Scrunched from %d to 512 channels" % arf['nchan']
            utils.print_info("Reducing %s from %d to 512 channels" %
                             (cmbfn, arf['nchan']), 2)
            # Scrunch to 512 channels
            utils.execute(['pam', '-m', '--setnchn', '512', cmbfn])
        else:
            note = None
        values = {'filepath': cmbdir,
                  'filename': os.path.basename(cmbfn),
                  'stage': 'combined',
                  'md5sum': utils.get_md5sum(cmbfn),
                  'filesize': os.path.getsize(cmbfn),
                  'parent_file_id': parent_file_id,
                  'note': note,
                  'snr': arf['snr']}
    except Exception as exc:
        utils.print_info("Exception caught while working on File ID %d" %
                         parent_file_id, 0)
        if isinstance(exc, (errors.CoastGuardError,
                            errors.FatalCoastGuardError)):
            # Get error message without colours mark-up
            msg = exc.get_message()
        else:
            msg = str(exc)
            utils.log_message(traceback.format_exc(), 'error')
        with db.transaction() as conn:
            update = db.files.update(). \
                        where(db.files.c.file_id==parent_file_id).\
                        values(status='failed', \
                                note='Combining failed! %s: %s' % \
                                            (type(exc).__name__, msg), \
                                last_modified=datetime.datetime.now())
            conn.execute(update)
        raise
    else:
        with db.transaction() as conn:
            version_id = utils.get_version_id(db)
            # Insert new entry
            insert = db.files.insert().\
                    values(version_id=version_id, \
                            obs_id=obs_id)
            result = conn.execute(insert, values)
            new_file_id = result.inserted_primary_key[0]
            # Update status of parent file's entry
            update = db.files.update(). \
                        where(db.files.c.file_id==parent_file_id).\
                        values(status='processed', \
                                last_modified=datetime.datetime.now())
            conn.execute(update)
    return new_file_id


def load_corrected_file(filerow):
    """Given a row from the DB's files table referring to a
        status='new', stage='combined' file, process the file
        by correcting its header and load the new file into
        the database.

        Inputs:
            filerow: A row from the files table.

        Output:
            file_id: The ID of the newly loaded 'corrected' file.
    """
    db = database.Database()
    parent_file_id = filerow['file_id']
    obs_id = filerow['obs_id']

    logrow = get_log(db, obs_id)
    log_id = logrow['log_id']
    logfn = os.path.join(logrow['logpath'], logrow['logname'])
    log.setup_logger(logfn)

    # Mark as running
    with db.transaction() as conn:
        update = db.files.update().\
                    where(db.files.c.file_id==parent_file_id).\
                    values(status='running', \
                            last_modified=datetime.datetime.now())
        conn.execute(update)
    if (filerow['status'] != 'new') or (filerow['stage'] != 'combined'):
        return errors.BadStatusError("Corrected files can only be " \
                        "generated from 'file' entries with " \
                        "status='new' and stage='combined'. " \
                        "(For File ID %d: status='%s', stage='%s'.)" % \
                        (parent_file_id, filerow['status'], filerow['stage']))
    infn = os.path.join(filerow['filepath'], filerow['filename'])
    try:
        corrfn, corrstr, note = correct.correct_header(infn)

        arf = utils.ArchiveFile(corrfn)

        # Move file to archive directory
        archivedir = os.path.join(config.output_location, \
                                config.output_layout) % arf
        archivefn = (config.outfn_template+".corr") % arf
        try:
            os.makedirs(archivedir)
        except OSError:
            # Directory already exists
            pass
        shutil.move(corrfn, os.path.join(archivedir, archivefn))
        # Update 'corrfn' so it still refers to the file
        corrfn = os.path.join(archivedir, archivefn)
        arf.fn = corrfn

        # Make diagnostic plots
        fullresfn, lowresfn = make_summary_plots(arf)

        # Pre-compute values to insert because some might be
        # slow to generate
        arf = utils.ArchiveFile(corrfn)
        values = {'filepath': archivedir,
                  'filename': archivefn,
                  'stage': 'corrected',
                  'note': note,
                  'md5sum': utils.get_md5sum(corrfn),
                  'filesize': os.path.getsize(corrfn),
                  'parent_file_id': parent_file_id,
                  'snr': arf['snr']}
        diagvals = [{'diagnosticpath': os.path.dirname(fullresfn),
                     'diagnosticname': os.path.basename(fullresfn)},
                    {'diagnosticpath': os.path.dirname(lowresfn),
                     'diagnosticname': os.path.basename(lowresfn)}
                   ]
    except Exception as exc:
        utils.print_info("Exception caught while working on File ID %d" %
                         parent_file_id, 0)
        # Add ID number to exception arguments
        exc.args = (exc.args[0] + "\n(File ID: %d)" % parent_file_id,)
        if isinstance(exc, (errors.CoastGuardError,
                            errors.FatalCoastGuardError)):
            msg = exc.get_message()
        else:
            msg = str(exc)
            utils.log_message(traceback.format_exc(), 'error')
        with db.transaction() as conn:
            update = db.files.update(). \
                        where(db.files.c.file_id == parent_file_id).\
                        values(status='failed',
                                note='Correction failed! %s: %s' %
                                            (type(exc).__name__, msg),
                                last_modified=datetime.datetime.now())
            conn.execute(update)
        raise
    else:
        # Success!
        with db.transaction() as conn:
            version_id = utils.get_version_id(db)
            # Insert new entry
            insert = db.files.insert().\
                    values(version_id = version_id,
                           obs_id=obs_id)
            result = conn.execute(insert, values)
            file_id = result.inserted_primary_key[0]
            # Insert diagnostic entries
            insert = db.diagnostics.insert().\
                    values(file_id=file_id)
            result = conn.execute(insert, diagvals)
            # Update observation to include correct receiver
            update = db.obs.update().\
                        where(db.obs.c.obs_id == obs_id).\
                        values(rcvr=arf['rcvr'],
                               last_modified=datetime.datetime.now())
            conn.execute(update)
            # Update parent file
            update = db.files.update().\
                        where(db.files.c.file_id == parent_file_id).\
                        values(status='processed',
                               last_modified=datetime.datetime.now())
            conn.execute(update)

        rows = get_files(db, obs_id)
        for row in get_files(db, obs_id):
            ext = STAGE_TO_EXT[row['stage']]
            move_file(db, row['file_id'], archivedir,
                    (config.outfn_template+ext) % arf)
        move_log(db, log_id, archivedir,
                    (config.outfn_template+".log") % arf)
    return file_id


def load_cleaned_file(filerow):
    """Given a row from the DB's files table referring to a
        status='new', stage='combined' file, process the file
        by cleaning it and load the new file into the database.

        Inputs:
            filerow: A row from the files table.

        Ouput:
            file_id: The ID of the newly loaded 'cleaned' file.
    """
    db = database.Database()
    parent_file_id = filerow['file_id']
    obs_id = filerow['obs_id']

    logrow = get_log(db, obs_id)
    log_id = logrow['log_id']
    logfn = os.path.join(logrow['logpath'], logrow['logname'])
    log.setup_logger(logfn)

    with db.transaction() as conn:
        update = db.files.update().\
                    where(db.files.c.file_id == parent_file_id).\
                    values(status='running',
                            last_modified=datetime.datetime.now())
        conn.execute(update)
    if (filerow['status'] != 'new') or (filerow['stage'] != 'corrected'):
        return errors.BadStatusError("Cleaned files can only be "
                        "generated from 'file' entries with "
                        "status='new' and stage='corrected'. "
                        "(For File ID %d: status='%s', stage='%s'.)" %
                        (parent_file_id, filerow['status'], filerow['stage']))
    infn = os.path.join(filerow['filepath'], filerow['filename'])
    try:
        arf = utils.ArchiveFile(infn)
        # Clean the data file
        config.cfg.load_configs_for_archive(arf)
        cleaner_queue = [cleaners.load_cleaner('rcvrstd'),
                         cleaners.load_cleaner('surgical')]

        for cleaner in cleaner_queue:
            cleaner.run(arf.get_archive())

        # Write out the cleaned data file
        archivedir = os.path.join(config.output_location,
                                config.output_layout) % arf
        archivefn = (config.outfn_template+".clean") % arf
        cleanfn = os.path.join(archivedir, archivefn)

        # Make sure output directory exists
        try:
            os.makedirs(archivedir)
        except OSError:
            # Directory already exists:
            pass
        arf.get_archive().unload(cleanfn)
        arf = utils.ArchiveFile(cleanfn)

        # Make diagnostic plots
        fullresfn, lowresfn = make_summary_plots(arf)

        # Pre-compute values to insert because some might be
        # slow to generate
        values = {'filepath': archivedir,
                  'filename': archivefn,
                  'stage': 'cleaned',
                  'md5sum': utils.get_md5sum(cleanfn),
                  'filesize': os.path.getsize(cleanfn),
                  'parent_file_id': parent_file_id,
                  'snr': arf['snr']}
        diagvals = [{'diagnosticpath': os.path.dirname(fullresfn),
                     'diagnosticname': os.path.basename(fullresfn)},
                    {'diagnosticpath': os.path.dirname(lowresfn),
                     'diagnosticname': os.path.basename(lowresfn)}
                   ]
    except Exception as exc:
        utils.print_info("Exception caught while working on File ID %d" %
                         parent_file_id, 0)
        # Add ID number to exception arguments
        exc.args = (exc.args[0] + "\n(File ID: %d)" % parent_file_id,)
        if isinstance(exc, (errors.CoastGuardError,
                            errors.FatalCoastGuardError)):
            msg = exc.get_message()
        else:
            msg = str(exc)
            utils.log_message(traceback.format_exc(), 'error')
        with db.transaction() as conn:
            update = db.files.update(). \
                        where(db.files.c.file_id == parent_file_id).\
                        values(status='failed',
                                note='Cleaning failed! %s: %s' % \
                                            (type(exc).__name__, msg),
                                last_modified=datetime.datetime.now())
            conn.execute(update)
        raise
    else:
        with db.transaction() as conn:
            version_id = utils.get_version_id(db)
            # Insert new entry
            insert = db.files.insert().\
                    values(version_id=version_id,
                            obs_id=obs_id)
            result = conn.execute(insert, values)
            file_id = result.inserted_primary_key[0]
            # Insert diagnostic entries
            insert = db.diagnostics.insert().\
                    values(file_id=file_id)
            result = conn.execute(insert, diagvals)
            # Update parent file
            update = db.files.update(). \
                        where(db.files.c.file_id == parent_file_id).\
                        values(status='processed',
                                last_modified=datetime.datetime.now())
            conn.execute(update)
    return file_id


def load_calibrated_file(filerow, lock):
    """Given a row from the DB's files table referring to a
        status='new' file, process the file
        by calibrating it and load the new file into the database.

        In the case of a 'pulsar' obs this requires an associated
        'cal' scan.

        In the case of a 'cal' scan this function will prepare
        and load the obs.

        Inputs:
            filerow: A row from the files table.
            lock: Lock for calibrator database file

        Ouput:
            file_id: The ID of the newly loaded 'calibrated' file.
    """
    name = utils.get_prefname(filerow['sourcename'])
    if name.endswith('_R'):
        name = name[:-2]

    db = database.Database()
    parent_file_id = filerow['file_id']
    obs_id = filerow['obs_id']

    logrow = get_log(db, obs_id)
    log_id = logrow['log_id']
    logfn = os.path.join(logrow['logpath'], logrow['logname'])
    log.setup_logger(logfn)

    with db.transaction() as conn:
        update = db.files.update().\
                    where(db.files.c.file_id == parent_file_id).\
                    values(status='running',
                            last_modified=datetime.datetime.now())
        conn.execute(update)
    if (filerow['status'] != 'new') or (filerow['stage'] != 'cleaned') or \
                (not filerow['qcpassed']):
        raise errors.BadStatusError("Calibrated files can only be "
                        "generated from 'file' entries with "
                        "status='new' and stage='cleaned' and "
                        "That have successfully passed quality control "
                        "- i.e. qcpassed=True."
                        "(For File ID %d: status='%s', stage='%s', "
                        "qcpassed=%s)" %
                        (parent_file_id, filerow['status'],
                            filerow['stage'], filerow['qcpassed']))
    infn = os.path.join(filerow['filepath'], filerow['filename'])
    try:
        arf = utils.ArchiveFile(infn)
        # Reduce data to the equivalent of 128 channels over 200 MHz
        # That is f_chan = 1.5625 MHz
        nchans = arf['bw']/1.5625
        values = {'sourcename': name,
                  'stage': 'calibrated',
                  'parent_file_id': parent_file_id}
        if nchans != arf['nchan']:
            values['note'] = "Scrunched to %d channels " \
                                "(1.5625 MHz each)" % nchans

        if filerow['obstype'] == 'cal':
            # Calibrator scan
            # Prepare the data file for being used to calibrate pulsar scans

            utils.execute(['pam', '--setnchn', '%d' % nchans, '-T',
                           '-e', 'pcal.T', infn])
            outpath = os.path.splitext(infn)[0]+'.pcal.T'
            arf = utils.ArchiveFile(outpath)
            plotfn = make_stokes_plot(arf)
            diagvals = [{'diagnosticpath': os.path.dirname(plotfn),
                         'diagnosticname': os.path.basename(plotfn)}]
            values['status'] = 'done'
        else:
            # Pulsar scan. Calibrate it.
            caldbrow = get_caldb(db, name)
            if caldbrow is None:
                raise errors.DataReductionFailed("No matching calibrator "
                                                 "database row for %s." % name)
            caldbpath = os.path.join(caldbrow['caldbpath'],
                                        caldbrow['caldbname'])
            try:
                lock.acquire()
                calfn = calibrate.calibrate(infn, caldbpath, nchans=nchans)
            finally:
                lock.release()

            if calfn is not None:
                calpath, calname = os.path.split(calfn)
                # Get file_id number for calibrator scan
                with db.transaction() as conn:
                    select = db.select([db.files]).\
                                where((db.files.c.filepath == calpath) &
                                    (db.files.c.filename == calname))
                    results = conn.execute(select)
                    rows = results.fetchall()
                    results.close()

                if len(rows) == 1:
                    values['cal_file_id'] = rows[0]['file_id']
                else:
                    raise errors.DatabaseError("Bad number of file "
                                               "rows (%d) with path='%s' "
                                               "and name='%s'!" %
                                               (len(rows), calpath,
                                                calname))

            outpath = os.path.splitext(infn)[0]+'.calibP'
            # Make diagnostic plots
            arf = utils.ArchiveFile(outpath)
            fullresfn, lowresfn = make_summary_plots(arf)
            pp_fullresfn, pp_lowresfn = make_polprofile_plots(arf)

            diagvals = [{'diagnosticpath': os.path.dirname(fullresfn),
                         'diagnosticname': os.path.basename(fullresfn)},
                        {'diagnosticpath': os.path.dirname(lowresfn),
                         'diagnosticname': os.path.basename(lowresfn)},
                        {'diagnosticpath': os.path.dirname(pp_fullresfn),
                         'diagnosticname': os.path.basename(pp_fullresfn)},
                        {'diagnosticpath': os.path.dirname(pp_lowresfn),
                         'diagnosticname': os.path.basename(pp_lowresfn)}]
            values['snr'] = arf['snr']
        if not os.path.isfile(outpath):
            raise ValueError("Cannot find output file (%s)!" % outpath)

        # Add other file-related values to insert into the DB
        values['filepath'], values['filename'] = os.path.split(outpath)
        values['md5sum'] = utils.get_md5sum(outpath)
        values['filesize'] = os.path.getsize(outpath)
    except Exception as exc:
        utils.print_info("Exception caught while working on File ID %d" %
                         parent_file_id, 0)
        # Add ID number to exception arguments
        exc.args = (exc.args[0] + "\n(File ID: %d)" % parent_file_id,)
        if isinstance(exc, (errors.CoastGuardError,
                            errors.FatalCoastGuardError)):
            msg = exc.get_message()
        else:
            msg = str(exc)
            utils.log_message(traceback.format_exc(), 'error')
        if filerow['obstype'] == 'cal':
            status = 'failed'
            note = 'Calibration failed! %s: %s' % (type(exc).__name__, msg)
        elif can_calibrate(db, obs_id):
            # Calibration of this file will be reattempted when 
            # the calibration database is updated
            status = 'calfail'
            note = 'Calibration failed! %s: %s' % (type(exc).__name__, msg)
        else:
            status = 'toload'
            note = 'File cannot be calibrated'
        with db.transaction() as conn:
            update = db.files.update(). \
                        where(db.files.c.file_id == parent_file_id).\
                        values(status=status,
                                note=note,
                                last_modified=datetime.datetime.now())
            conn.execute(update)
        raise
    else:
        with db.transaction() as conn:
            version_id = utils.get_version_id(db)
            # Insert new entry
            insert = db.files.insert().\
                    values(version_id=version_id,
                            obs_id=obs_id)
            result = conn.execute(insert, values)
            file_id = result.inserted_primary_key[0]
            if diagvals:
                # Insert diagnostic entries
                insert = db.diagnostics.insert().\
                        values(file_id=file_id)
                result = conn.execute(insert, diagvals)
            # Update parent file
            update = db.files.update(). \
                        where(db.files.c.file_id == parent_file_id).\
                        values(status='processed',
                                last_modified=datetime.datetime.now())
            conn.execute(update)
        if filerow['obstype'] == 'cal':
            # Update the calibrator database
            try:
                lock.acquire()
                update_caldb(db, arf['name'], force=True)
            finally:
                lock.release()
    return file_id


def load_to_toaster(filerow):
    """Load the row to TOASTER database.

        Input:
            filerow: The DB of the entry to be loaded.

        Outputs:
            None
    """
    db = database.Database()
    file_id = filerow['file_id']
    fn = os.path.join(filerow['filepath'], filerow['filename'])
    try:
        rawfile_id = load_rawfile.load_rawfile(fn)
    except Exception as exc:
        utils.print_info("Exception caught while working on File ID %d" %
                         file_id, 0)
        # Add ID number to exception arguments
        exc.args = (exc.args[0] + "\n(File ID: %d)" % file_id,)
        msg = str(exc)
        utils.log_message(traceback.format_exc(), 'error')
        with db.transaction() as conn:
            update = db.files.update().\
                        where(db.files.c.file_id == file_id).\
                        values(status='failed',
                                note='Could not be loaded into TOASTER.',
                                last_modified=datetime.datetime.now())
            conn.execute(update)
        raise
    else:
        with db.transaction() as conn:
            # Update file
            update = db.files.update().\
                        where(db.files.c.file_id == file_id).\
                        values(status='done',
                                note="Loaded into TOASTER DB (rawfile ID: %d)" %
                                     rawfile_id,
                                last_modified=datetime.datetime.now())
            conn.execute(update)


def can_calibrate(db, obs_id):
    """Return True is observation can be calibrated.
        NOTE: It is still possible the observation cannot
            be calibrated _now_ even if this function returns
            True. This might be the case if the calibration
            observation hasn't been reduced yet.

        Inputs:
            db: A database object.
            obs_id: The ID number of an entry in the database.

        Outputs:
            can_cal: True if the observation can be calibrated.
    """
    obsrow = get_obs(db, obs_id)
    if obsrow['obstype'] != 'pulsar':
        raise errors.InputError("Only observations of type 'pulsar' "
                                "can be calibrated. Obstype for obs_id %d: %s" %
                                (obs_id, obsrow['obstype']))
    mjdnow = rs.utils.mjdnow()
    if (mjdnow - obsrow['start_mjd']) < 7:
        # Observation is less than 1 week old.
        # Let's hold out hope that it can be calibrated.
        return True
    mjdrange = (obsrow['start_mjd']-TWO_HRS_IN_DAYS,
                obsrow['start_mjd']+TWO_HRS_IN_DAYS)
    # Now try to find a compatible calibrator scan
    with db.transaction() as conn:
        select = db.select([db.files],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=(db.files.c.obs_id ==
                                        db.obs.c.obs_id))]).\
                    where((db.obs.c.obstype == 'cal') &
                            (db.obs.c.sourcename == "%s_R" %
                                    obsrow['sourcename']) &
                            ((db.obs.c.rcvr == obsrow['rcvr']) |
                                (db.obs.c.rcvr.is_(None))) &
                            db.obs.c.start_mjd.between(*mjdrange))
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()
    obs = {}
    utils.sort_by_keys(rows, ['file_id'])
    for row in rows:
        can_cal = obs.setdefault(row['obs_id'], True)
        obs[row['obs_id']] &= (not (row['qcpassed'] == False))
    can_cal = obs.values()
    utils.print_info("Found %d potential calibrators for obs ID %d" %
                    (sum(can_cal), obs_id), 2)
    return any(can_cal)


def get_obs(db, obs_id):
    """Given a observation ID return the corresponding entry
        in the obs table.

        Inputs:
            db: A Database object.
            obs_id: A observation ID.

        Outputs:
            obsrow: The corresponding obs entry.
    """
    with db.transaction() as conn:
        select = db.select([db.obs]).\
                    where(db.obs.c.obs_id == obs_id)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    if len(rows) == 1:
        return rows[0]
    elif len(rows) == 0:
        return None
    else:
        raise errors.DatabaseError("Bad number of obs rows (%d) "
                                   "with obs_id=%d!" %
                                   (len(rows), obs_id))
    return rows


def get_files(db, obs_id):
    """Given a observation ID return the corresponding entries
        in the files table.

        Inputs:
            db: A Database object.
            obs_id: A observation ID.

        Outputs:
            filerows: The corresponding file entries.
    """
    with db.transaction() as conn:
        select = db.select([db.files,
                            db.obs.c.dir_id,
                            db.obs.c.sourcename,
                            db.obs.c.obstype,
                            db.obs.c.start_mjd],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=db.files.c.obs_id ==
                                    db.obs.c.obs_id)]).\
                    where(db.files.c.obs_id == obs_id)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    return rows


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
                    where((db.files.c.status == 'new') &
                            (db.files.c.stage == 'calibrated') &
                            (db.obs.c.obstype == 'cal'))
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
    reattempt_calibration(db, name)
    return outpath


def reattempt_calibration(db, sourcename):
    """Mark files that have failed calibration to be reattempted.

        Inputs:
            db: A Database object.
            sourcename: The name of the source to match.
                (NOTE: '_R' will be removed from the sourcename, if present)
            
        Outputs:
            None
    """
    name = utils.get_prefname(sourcename)
    if name.endswith('_R'):
        name = name[:-2]

    db = database.Database()
    with db.transaction() as conn:
        # Get rows that need to be updated
        # The update is a two-part process because
        # a join is required. (Can updates include joins?)
        select = db.select([db.files],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=db.files.c.obs_id ==
                                    db.obs.c.obs_id)]).\
                    where((db.files.c.status == 'calfail') &
                            (db.files.c.stage == 'cleaned') &
                            (db.files.c.qcpassed == True) &
                            (db.obs.c.sourcename == name))
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
        # Now update rows
        for row in rows:
            update = db.files.update().\
                    where(db.files.c.file_id == row['file_id']).\
                    values(status='new',
                            note='Reattempting calibration',
                            last_modified=datetime.datetime.now())
            conn.execute(update)
        utils.print_info("Resetting status to 'new' (from 'calfail') "
                         "for %d files with sourcename='%s'" %
                         (len(rows), name), 2)


def get_log(db, obs_id):
    """Given a obs_id retrive the corresponding entry
        in the logs table.

        Inputs:
            db: A Database object.
            obs_id: The ID of the group to get the log for.

        Output:
            logrow: The log's DB row.
    """
    with db.transaction() as conn:
        select = db.select([db.logs]).\
                    where(db.logs.c.obs_id == obs_id)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
        if len(rows) != 1:
            raise errors.DatabaseError("Bad number of rows (%d) "
                                       "with obs_id=%d!" %
                                       (len(rows), obs_id))
        return rows[0]


def move_log(db, log_id, destdir, destfn=None):
    """Given a group ID move the associated listing.

        Inputs:
            db: Database object to use.
            log_id: The ID of a row in the logs table.
            destdir: The destination directory.
            destfn: The destination file name.
                (Default: Keep old file name).

        Outputs:
            None
    """
    with db.transaction() as conn:
        select = db.select([db.logs]).\
                    where(db.logs.c.log_id == log_id)
        result = conn.execute(select)
        rows = result.fetchall()
        if len(rows) != 1:
            raise errors.DatabaseError("Bad number of rows (%d) "
                                       "with log_id=%d!" %
                                       (len(rows), log_id))
        lg = rows[0]
        if destfn is None:
            destfn = lg['logname']
        # Copy file
        src = os.path.join(lg['logpath'], lg['logname'])
        dest = os.path.join(destdir, destfn)
        try:
            os.makedirs(destdir)
        except OSError:
            # Directory already exists
            pass
        shutil.copy(src, dest)
        # Update database
        update = db.logs.update().\
                    where(db.logs.c.log_id == log_id).\
                    values(logpath=destdir,
                            logname=destfn,
                            last_modified=datetime.datetime.now())
        conn.execute(update)
        # Remove original
        os.remove(src)
        utils.print_info("Moved log from %s to %s. The database "
                         "has been updated accordingly." % (src, dest))


def delete_file(db, file_id):
    """Given a file ID remove the associated archive.

        Inputs:
            db: Database object to use.
            file_id: The ID of a row in the files table.

        Outputs:
            None
    """
    with db.transaction() as conn:
        select = db.select([db.files]).\
                    where(db.files.c.file_id == file_id)
        result = conn.execute(select)
        rows = result.fetchall()
        if len(rows) != 1:
            raise errors.DatabaseError("Bad number of rows (%d) "
                                       "with file_id=%d!" %
                                       (len(rows), file_id))
        ff = rows[0]
        # Copy file
        fn = os.path.join(ff['filepath'], ff['filename'])
        utils.print_info("Deleting archive file (%s)." % fn, 2)
        # Update database
        update = db.files.update().\
                    where(db.files.c.file_id == file_id).\
                    values(is_deleted=True,
                            last_modified=datetime.datetime.now())
        conn.execute(update)
        # Remove original
        try:
            os.remove(fn)
        except:
            pass


def move_file(db, file_id, destdir, destfn=None):
    """Given a file ID move the associated archive.

        Inputs:
            db: Database object to use.
            file_id: The ID of a row in the files table.
            destdir: The destination directory.
            destfn: The destination file name.
                (Default: Keep old file name).

        Outputs:
            None
    """
    with db.transaction() as conn:
        select = db.select([db.files]).\
                    where(db.files.c.file_id == file_id)
        result = conn.execute(select)
        rows = result.fetchall()
        if len(rows) != 1:
            raise errors.DatabaseError("Bad number of rows (%d) "
                                       "with file_id=%d!" %
                                       (len(rows), file_id))
        ff = rows[0]
        if destfn is None:
            destfn = ff['filename']
        # Copy file
        src = os.path.join(ff['filepath'], ff['filename'])
        dest = os.path.join(destdir, destfn)
        utils.print_info("Moving archive file from %s to %s." % (src, dest), 2)
        if src == dest:
            utils.print_info("File is already at its destination (%s). "
                             "No need to move." % dest, 2)
        else:
            try:
                os.makedirs(destdir)
            except OSError:
                # Directory already exists
                pass
            shutil.copy(src, dest)
            # Update database
            update = db.files.update().\
                        where(db.files.c.file_id == file_id).\
                        values(filepath=destdir,
                                filename=destfn,
                                last_modified=datetime.datetime.now())
            conn.execute(update)
            # Remove original
            os.remove(src)
            utils.print_info("Moved archive file from %s to %s. The database "
                             "has been updated accordingly." % (src, dest), 2)


def get_rawdata_dirs(basedir=None, priority=[]):
    """Get a list of directories likely to contain asterix data.
        Directories 2 levels deep with a name "YYYYMMDD" are returned.

        Input:
            basedir: Root of the directory tree to search.
            priority: List of directories to prioritize.
                (Default: No priorities)

        Output:
            outdirs: List of likely raw data directories.
    """
    if basedir is None:
        basedir = config.base_rawdata_dir
    outdirs = []
    if priority:
        indirs = []
        for name in priority:
            indirs.extend(glob.glob(os.path.join(basedir, name)))
    else:
        indirs = glob.glob(os.path.join(basedir, "*"))
    for path in indirs:
        subdirs = glob.glob(os.path.join(path, "*"))
        for subdir in subdirs:
            if os.path.isdir(subdir):
                try:
                    datetime.datetime.strptime(os.path.basename(subdir), "%Y%m%d")
                except:
                    pass
                else:
                    # Is a directory whose name has the required format
                    outdirs.append(subdir)
    return outdirs


def make_groups(path):
    """Given a directory containing asterix subint files
        return a list of subint groups.

        Input:
            path: A directory containing frequency sub-band 
                directories.

        Output:
            usedirs_list: List of lists of directories to use when combining.
                (NOTE: This may be different than the input
                    'subdirs' because some directories may have
                    too few subints to be worth combining. This
                    depends on the input value of 'tossfrac'.)
            groups_list: List of lists of groups of files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed appears in each of 'usedirs'.)
    """
    usedirs_list = []
    groups_list = []

    # Try L-band and S-band
    for band, subdir_pattern in \
                    zip(['Lband', 'Sband'], ['1'+'[0-9]'*3, '2'+'[0-9]'*3]):
        subdirs = glob.glob(os.path.join(path, subdir_pattern))
        if subdirs:
            utils.print_info("Found %d freq sub-band dirs for %s in %s. "
                             "Will group sub-ints contained" %
                             (len(subdirs), band, path), 2)
            usedirs, groups = combine.group_subband_dirs(subdirs)
            # Keep track of the groups and directories used
            for grp in groups:
                groups_list.append(grp)
                usedirs_list.append(usedirs)
    return usedirs_list, groups_list


def make_combined_file(subdirs, subints, outdir, parfn=None):
    """Given lists of directories and subints combine them.

        Inputs:
            subdirs: List of sub-band directories containing 
                sub-ints to combine
            subints: List of subint files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed should appear in each of the
                    subdirs.)
            outdir: Directory to copy combined file to.
            parfn: Parfile to install when creating combined file
                (Default: don't install a new ephemeris)

        Outputs:
            outfn: The name of the combined archive.
    """
    # Work in a temporary directory
    tmpdir = tempfile.mkdtemp(suffix="_combine",
                              dir=config.tmp_directory)
    try:
        # Prepare subints
        preppeddirs = prepare_subints(subdirs, subints,
                                      baseoutdir=os.path.join(tmpdir, 'data'))
        cmbfn = combine.combine_subints(preppeddirs, subints,
                                        parfn=parfn, outdir=outdir)
    except:
        raise # Re-raise the exception
    finally:
        if debug.is_on('reduce'):
            warnings.warn("Not cleaning up temporary directory (%s)" % tmpdir)
        else:
            utils.print_info("Removing temporary directory (%s)" % tmpdir, 2)
            shutil.rmtree(tmpdir)
    return cmbfn


def prepare_subints(subdirs, subints, baseoutdir):
    """Prepare subints by
           - Moving them to the temporary working directory
           - De-weighting 6.25% from each sub-band edge
           - Converting archive format to PSRFITS

        Inputs:
            subdirs: List of sub-band directories containing 
                sub-ints to combine
            subints: List of subint files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed should appear in each of the
                    subdirs.)
            baseoutdir: Directory containing the sub-directories
                of preprared files.

        Outputs:
            prepsubdirs: The sub-directories containing prepared files.
    """
    devnull = open(os.devnull)
    tmpsubdirs = []
    for subdir in utils.show_progress(subdirs, width=50):
        freqdir = os.path.split(os.path.abspath(subdir))[-1]
        freqdir = os.path.join(baseoutdir, freqdir)
        try:
            os.makedirs(freqdir)
        except OSError:
            # Directory already exists
            pass
        fns = [os.path.join(subdir, fn) for fn in subints]
        utils.execute(['paz', '-j', 'convert psrfits',
                       '-E', '6.25', '-O', freqdir] + fns,
                      stderr=devnull)
        tmpsubdirs.append(freqdir)
    utils.print_info("Prepared %d subint fragments in %d freq sub-dirs" %
                    (len(subints), len(subdirs)), 3)
    return tmpsubdirs


def make_summary_plots(arf):
    """Make two summary plots. One with the native time/freq/bin resolution
        and nother that is partially scrunched.

        Input:
            arf: An ArchiveFile object.

        Outputs:
            fullresfn: The name of the high-resolution summary plot file.
            lowresfn: The name of the low-resolution summary plot file.
    """
    fullresfn = arf.fn+".png"
    diagnose.make_composite_summary_plot(arf, outfn=fullresfn)

    # 6.25 MHz channels
    nchans = arf['bw']/6.25
    preproc = 'C,D,B 128,F %d' % nchans
    if arf['length'] > 60:
        # one minute subintegrations
        preproc += ",T %d" % (arf['length']/60)
    lowresfn = arf.fn+".scrunched.png"
    diagnose.make_composite_summary_plot(arf, preproc, outfn=lowresfn)

    return fullresfn, lowresfn


def make_polprofile_plots(arf):
    """Make two polarization profile plots. One with the native bin 
        resolution and another that is partially scrunched.

        Input:
            arf: An ArchiveFile object.

        Outputs:
            fullresfn: The name of the high-resolution polarization 
                profile plot file.
            lowresfn: The name of the low-resolution polarization 
                profile plot file.
    """
    fullresfn = arf.fn+".Scyl.png"
    diagnose.make_polprofile_plot(arf, outfn=fullresfn)

    preproc = 'C,D,T,F,B 128'
    lowresfn = arf.fn+".Scyl.scrunched.png"
    diagnose.make_polprofile_plot(arf, preproc, outfn=lowresfn)

    return fullresfn, lowresfn


def make_stokes_plot(arf):
    """Make a stokes profile plot.

        Input:
            arf: An ArchiveFile object.

        Output:
            plotfn: The name of the stokes plot.
    """
    utils.print_info("Creating stokes profile plot for %s" % arf.fn, 3)
    outfn = "%s.stokes.png" % arf.fn
    utils.print_info("Output plot name: %s" % outfn, 2)
    suffix = os.path.splitext(outfn)[-1]
    handle, tmpfn = tempfile.mkstemp(suffix=suffix)

    grdev = "%s/PNG" % tmpfn
    utils.execute(['psrplot', '-p', 'stokes', '-j', 'CDTF',
                  arf.fn, '-D', grdev])
    # Rename tmpfn to requested output filename
    shutil.move(tmpfn, outfn)
    return outfn


def get_togroup(db):
    """Get a list of directories rows that need to be grouped.

        Inputs:
            db: A Database object to use.
        
        Outputs:
            dirrows: A list of directory rows.
    """
    with db.transaction() as conn:
        select = db.select([db.directories]).\
                    where(db.directories.c.status == 'new')
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()
    utils.print_info("Got %d rows to be grouped" % len(rows), 2)
    return rows


def get_toload(db):
    """Get a list of rows to load into the TOASTER DB.

        Inputs:
            db: A Database object to use.

        Output:
            rows: A list database rows to be reduced.
    """
    with db.transaction() as conn:
        select = db.select([db.files,
                            db.obs.c.dir_id,
                            db.obs.c.sourcename,
                            db.obs.c.obstype,
                            db.obs.c.start_mjd],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=db.files.c.obs_id ==
                                    db.obs.c.obs_id)]).\
                            where(db.files.c.status == 'toload')
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()
    utils.print_info("Got %d rows to load to TOASTER" % len(rows), 2)
    return rows


def get_todo(db, action, priorities=None):
    """Get a list of rows to reduce.
        
        Inputs:
            db: A Database object to use.
            action: The action to perform.
            priorities: A list of source names to reduce.
                NOTE: sources not listed in priorities will never be reduced
                (Default: Reduce all sources).

        Outputs:
            rows: A list database rows to be reduced.
    """
    if action not in ACTIONS:
        raise errors.UnrecognizedValueError("The file action '%s' is not "
                                            "recognized. Valid file actions "
                                            "are '%s'." %
                                            "', '".join(ACTIONS.keys()))

    target_stages, qcpassed_only, withlock, actfunc = ACTIONS[action]
    whereclause = db.files.c.status == 'new'
    if target_stages is not None:
        whereclause &= db.files.c.stage.in_(target_stages)
    if qcpassed_only:
        whereclause &= db.files.c.qcpassed == True
    if priorities:
        prioritizer, cfgstr = priorities[0]
        tmp = prioritizer(db, cfgstr)
        for prioritizer, cfgstr in priorities[1:]:
            tmp |= prioritizer(db, cfgstr)
        whereclause &= tmp
    with db.transaction() as conn:
        select = db.select([db.files,
                            db.obs.c.dir_id,
                            db.obs.c.sourcename,
                            db.obs.c.obstype,
                            db.obs.c.start_mjd],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=db.files.c.obs_id ==
                                    db.obs.c.obs_id)]).\
                            where(whereclause)
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()
    utils.print_info("Got %d rows for '%s' action (priority: %s)" %
                        (len(rows), action, priorities), 2)
    return rows


def launch_task(db, action, row):
    """Launch a single task acting on the relevant file.

        Inputs:
            db: A Database object to use.
            action: The action to perform.
            row: A single row representing a taks to launch

        Outputs:
            proc: The started multiprocessing.Process object
    """
    if action not in ACTIONS:
        raise errors.UnrecognizedValueError("The file action '%s' is not "
                                            "recognized. Valid file actions "
                                            "are '%s'." %
                                            "', '".join(ACTIONS.keys()))

    target_stages, qcpassed_only, withlock, actfunc = ACTIONS[action]
    results = []
    with db.transaction() as conn:
        update = db.files.update().\
                    where(db.files.c.file_id == row['file_id']).\
                    values(status='submitted',
                            last_modified=datetime.datetime.now())
        conn.execute(update)
    if withlock:
        lock = get_caldb_lock(row['sourcename'])
        args = (row,lock)
    else:
        args = (row,)
    name = "%s.file_id:%d" % (action, row['file_id'])
    proc = multiprocessing.Process(group=None, target=actfunc,
                                   name=name, args=args)
    proc.start()
    return proc


def get_caldb_lock(sourcename):
    """Return the lock used to access the calibrator database
        file for the given source.

        Input:
            sourcename: The name of the source to match.
                (NOTE: '_R' will be removed from the sourcename, if present)

        Output:
            lock: The corresponding lock.
    """
    name = utils.get_prefname(sourcename)
    if name.endswith('_R'):
        name = name[:-2]
    lock = CALDB_LOCKS.setdefault(name, multiprocessing.Lock())
    return lock


def prioritize_pulsar(db, psrname):
    """Return a sqlalchemy query that will prioritize 
        a pulsar.

        Inputs:
            db: A Database object to use.
            psrname: The name of the pulsar to prioritize.

        Outputs:
            sqlquery: A sqlquery object.
    """
    return db.obs.c.sourcename.like(psrname)


def prioritize_mjdrange(db, mjdrangestr):
    """Return a sqlalchemy query that will prioritize
        observations in a specific MJD range.

        Inputs:
            db: A Database object to use.
            mjdrangestr: The range of MJDs.
                format: <start MJD>-<end MJD>

        Output:
            sqlquery: A sqlquery object.
    """
    startmjd, endmjd = [float(xx) for xx in mjdrangestr.split('-')]
    return ((db.obs.c.start_mjd > startmjd) &
                    (db.obs.c.start_mjd < endmjd))


def prioritize_predef_srclist(db, srclist_name):
    """Return a sqlalchemy query that will prioritize a pre-defined
        list of pulars.

        Inputs:
            db: A Database object to use.
            srclist_name: The name of the source list to prioritize.

        Outputs:
            sqlquery: A sqlquery object.
    """
    srclist = [utils.get_prefname(src) for src in SOURCELISTS[srclist_name]]
    srclist += [name+"_R" for name in srclist if not name.endswith("_R")]
    return db.obs.c.sourcename.in_(srclist)


# Actions are defined by a tuple: (target stage, 
#                                  passed quality control,
#                                  with calibrator database lock,
#                                  function to proceed to next step)
ACTIONS = {'combine': (['grouped'], False, False, load_combined_file),
           'correct': (['combined'], False, False, load_corrected_file),
           'clean': (['corrected'], False, False, load_cleaned_file),
           'calibrate': (['cleaned'], True, True, load_calibrated_file),
           'load': ([], True, False, load_to_toaster)}

PRIORITY_FUNC = {'pulsar': prioritize_pulsar,
                 'psr': prioritize_pulsar,
                 #'date': prioritize_daterange,
                 'mjd': prioritize_mjdrange,
                 'srclist': prioritize_predef_srclist}


def parse_priorities(priority_str):
    ruletype, sep, cfgstrs = priority_str.partition('=')
    if ruletype.lower() not in PRIORITY_FUNC:
        raise ValueError("Prioritization rule '%s' is not recognized. "
                         "Valid types are: '%s'" %
                         (ruletype, "', '".join(PRIORITY_FUNC.keys())))
    priority_list = []
    for cfgstr in cfgstrs.split(','):
        priority_list.append((PRIORITY_FUNC[ruletype], cfgstr))
        utils.print_info("Will add %s=%s as a priority" %
                         (ruletype, cfgstr), 1)
    return priority_list


def main():
    # Share verbosity level with TOASTER
    toaster.config.cfg.verbosity = config.verbosity
    # Share debug modes with TOASTER
    for mode in debug.get_on_modes():
        try:
            toaster.debug.set_mode_on(mode)
        except toaster.errors.BadDebugMode:
            pass

    inprogress = []
    try:
        priority_list = []
        for priority_str in args.priority:
            priority_list.extend(parse_priorities(priority_str))
        db = database.Database()

        # Load raw data directories
        print "Loading directories..."
        ndirs = load_directories(db)
        # Group data immediately
        dirrows = get_togroup(db)
        print "Grouping subints..."
        for dirrow in utils.show_progress(dirrows, width=50):
            try:
                load_groups(dirrow)
            except errors.CoastGuardError:
                sys.stderr.write("".join(traceback.format_exception(*sys.exc_info())))

        # Turn off progress counters before we enter the main loop
        config.show_progress = False

        print "Entering main loop..."
        while True:
            nfree = args.numproc - len(inprogress)
            nsubmit = 0
            if nfree:
                # Load files to TOASTER
                toload = get_toload(db)[:nfree]
                for row in toload:
                    proc = launch_task(db, 'load', row)
                    inprogress.append(proc)
                nnew = len(toload)
                nfree -= nnew
                nsubmit += nnew
                if nnew:
                    utils.print_info("Launched %d 'load' tasks" % nnew, 0)

                for action in ('calibrate', 'clean', 'correct', 'combine'):
                    rows = get_todo(db, action,
                                    priorities=priority_list)[:nfree]
                    for row in rows:
                        proc = launch_task(db, action, row)
                        inprogress.append(proc)
                    nnew = len(rows)
                    nfree -= nnew
                    nsubmit += nnew
                    if nnew:
                        utils.print_info("Launched %d '%s' tasks" %
                                         (nnew, action), 0)
            utils.print_info("[%s] - Num running: %d; Num submitted: %d" %
                        (datetime.datetime.now(), len(inprogress), nsubmit), 0)
            # Sleep between iterations
            time.sleep(args.sleep_time)
            # Check for completed tasks
            for ii in xrange(len(inprogress)-1, -1, -1):
                proc = inprogress[ii]
                #print "Checking %s" % proc.name
                #print "Is alive: %s; Exitcode: %s" % \
                #        (proc.is_alive(), proc.exitcode)
                if not proc.is_alive() and proc.exitcode is not None:
                    if proc.exitcode != 0:
                        if proc.exitcode < 0:
                            msg = "With signal %d" % (-proc.exitcode)
                        else:
                            msg = "With error code %d" % proc.exitcode
                        sys.stderr.write("Process failed! %s\n" % msg)
                    inprogress.pop(ii)
    except:
        # Re-raise the error
        raise


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Automated reduction "
                                    "of Asterix data.")
    parser.add_argument("-P", "--num-procs", dest='numproc', type=int,
                        default=1,
                        help="Number of processes to run simultaneously.")
    parser.add_argument("-t", "--sleep-time", dest='sleep_time', type=int,
                        default=300,
                        help="Number of seconds to sleep between iterations "
                             "of the main loop. (Default: 300s)")
    parser.add_argument("--prioritize", action='append',
                        default=[], dest='priority',
                        help="A rule for prioritizing observations.")
    args = parser.parse_args()
    main()
