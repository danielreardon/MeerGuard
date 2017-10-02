#!/usr/bin/env python

import multiprocessing
import traceback
import warnings
import tempfile
import datetime
import hashlib
import shutil
import time
import glob
import sys
import os


import toaster.config
import toaster.debug
import toaster.errors
from toaster.toolkit.rawfiles import load_rawfile

from coast_guard import config
from coast_guard import utils
from coast_guard import diagnose
from coast_guard import cleaners
from coast_guard import combine
from coast_guard import database
from coast_guard import errors
from coast_guard import debug
from coast_guard import log
from coast_guard import correct
from coast_guard import calibrate

import pyriseset as rs

# Set umask so that all group members can access files/directories created
os.umask(0007)

# A lock for each calibrator database file
# The multiprocessing.Lock objects are created on demand
CALDB_LOCKS = {}

STAGE_TO_EXT = {'combined': '.cmb',
                'grouped': '.list.txt',
                'cleaned': '.clean',
                'corrected': '.corr'}

MINUTES_PER_DAY = 60.0*24.0

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
        'wasted_time': ['J0030+0451', 'J0218+4232', 'J0613-0200',
                        'J0621+1002', 'J0751+1807', 'J1012+5307',
                        'J1022+1001', 'J1024-0719', 'J1600-3053',
                        'J1640+2224', 'J1643-1224', 'J1713+0747',
                        'J1730-2304', 'J1738+0333', 'J1744-1134',
                        'J1853+1303', 'J1857+0943', 'J1911+1347',
                        'J1918-0642', 'J1939+2134', 'J2010-1323',
                        'J2145-0750', 'J2229+2643', 'J2317+1439',
                        'J2322+2057', 'J0102+4829', 'J0307+7442',
                        'J0340+4129', 'J0645+5158', 'J1231-1411',
                        'J1312+0051', 'J1741+1351', 'J2017+0603',
                        'J2043+1711', 'J2302+4442', 'J0636+5128',
                        'J0742+6620', 'J1125+7819', 'J1710+4923',
                        'J2234+0611', 'J0348+0432', 'J0407+1607',
                        'J0737-3039A', 'J1518+4904', 'J1753-2240',
                        'J1756-2251', 'J1811-1736', 'J1906+0746',
                        'J0023+0923', 'J1023+0038', 'J1745+1017',
                        'J1810+1744', 'J2214+3000', 'J2234+0944'], 
          'priority1': ['J0613-0200', 'J1012+5307', 'J1022+1001', 
                        'J1024-0719', 'J1600-3053', 'J1640+2224', 
                        'J1643-1224', 'J1713+0747', 'J1730-2304', 
                        'J1744-1134', 'J1853+1303', 'J1857+0943', 
                        'J1911+1347', 'J1918-0642', 'J1939+2134', 
                        'J2145-0750', 'J2317+1439'], 
                'mou': [#'J1946+3414', 'J1832-0836', 'J2205+6015', 
                        'J1125+7819', 'J0742+6620', 'J1710+4923', 
                        'J0636+5128', 'J2234+0611', 'J0931-1902'],
       'asterixpaper': ['J0030+0451', 'J0034-0534', 'J0218+4232', 
                        'J0348+0432', 
                        'J0610-2100', 'J0613-0200', 'J0621+1002', 
                        #'J0737-3039A',
                        'J0751+1807', 'J0900-3144', 'J1012+5307', 
                        'J1022+1001', 'J1024-0719', 'J1455-3330', 
                        'J1518+4904',
                        'J1600-3053', 'J1640+2224', 'J1643-1224', 
                        'J1713+0747', 'J1721-2457', 'J1730-2304', 
                        'J1738+0333', 'J1741+1351', 'J1744-1134', 
                        'J1751-2857', 'J1801-1417', 'J1802-2124',
                        'J1804-2717', 'J1843-1113', 'J1853+1303', 
                        'J1857+0943', 'J1909-3744', 'J1910+1256', 
                        'J1911-1114', 'J1911+1347', 'J1918-0642', 
                        'J1939+2134', 'J1955+2908', 'J2010-1323', 
                        'J2019+2425', 'J2033+1734', 'J2145-0750', 
                        'J2229+2643', 'J2317+1439', 'J2322+2057', 
                        'J0340+4129', 'J2017+0603', 'J2043+1711', 
                        'J2124-3358', 'J2234+0944', 'J0023+0923']}

PARFILES = {
            'J0030+0451': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J0030+0451.par-ML',
            'J0034-0534': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J0034-0534.par-ML',
            'J0218+4232': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J0218+4232.par-ML',
            'J0340+4129': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/to_install/J0340+4129.par',
            'J0610-2100': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J0610-2100.par-ML',
            'J0613-0200': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J0613-0200.par-ML',
            'J0621+1002': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J0621+1002.par-ML',
            'J0737-3039A': '/media/part1/plazarus/timing/asterix/'
                           'testing/parfiles/to_install/0737-3039A.par',
            'J0751+1807': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J0751+1807.par-ML',
            'J0900-3144': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J0900-3144.par-ML',
            'J1012+5307': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1012+5307.par-ML',
            'J1022+1001': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1022+1001.par-ML',
            'J1024-0719': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1024-0719.par-ML',
            'J1455-3330': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1455-3330.par-ML',
            'J1600-3053': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1600-3053.par-ML',
            'J1640+2224': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1640+2224.par-ML',
            'J1643-1224': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1643-1224.par-ML',
            'J1713+0747': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1713+0747.par-ML',
            'J1721-2457': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1721-2457.par-ML',
            'J1730-2304': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1730-2304.par-ML',
            'J1738+0333': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1738+0333.par-ML',
            'J1744-1134': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1744-1134.par-ML',
            'J1751-2857': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1751-2857.par-ML',
            'J1801-1417': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1801-1417.par-ML',
            'J1802-2124': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1802-2124.par-ML',
            'J1804-2717': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1804-2717.par-ML',
            'J1811-1736': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/to_install/J1811-1736.atnf.par',
            'J1843-1113': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1843-1113.par-ML',
            'J1853+1303': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1853+1303.par-ML',
            'J1857+0943': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1857+0943.par-ML',
            'J1909-3744': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1909-3744.par-ML',
            'J1910+1256': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1910+1256.par-ML',
            'J1911-1114': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1911-1114.par-ML',
            'J1911+1347': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1911+1347.par-ML',
            'J1918-0642': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1918-0642.par-ML',
            'J1939+2134': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1939+2134.par-ML',
            'J1955+2908': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J1955+2908.par-ML',
            'J2010-1323': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J2010-1323.par-ML',
            'J2017+0603': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/to_install/J2017+0603.atnf.par',
            'J2019+2425': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J2019+2425.par-ML',
            'J2033+1734': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J2033+1734.par-ML',
            'J2043+1711': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/to_install/J2043+1711.par',
            'J2124-3358': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J2124-3358.par-ML',
            'J2145-0750': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J2145-0750.par-ML',
            'J2229+2643': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J2229+2643.par-ML',
            'J2317+1439': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J2317+1439.par-ML',
            'J2322+2057': '/media/part1/plazarus/timing/asterix/testing/'
                          'parfiles/epta-v2.2-parfiles/J2322+2057.par-ML',
            }


PATH_TO_BACKEND = {'/media/part2/TIMING/Asterix/': 'ASTERIX',
                   '/media/part2/TIMING/Asterix_V2/': 'ASTERIXv2'}


def get_backend_from_dir(path):
    backend = None
    for key in PATH_TO_BACKEND:
        if path.startswith(key):
            backend = PATH_TO_BACKEND[key]
            break
    return backend


def load_directories(db, force=False, *args, **kwargs):
    """Search for directories containing asterix data.
        For each newly found entry, insert a row in the
        database.

        Input:
            db: Database object to use.
            force: Attempt to load all directories regardless
                of modification times. (Default: False)
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
        if force or (os.path.getmtime(path) > most_recent_addtime):
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
            try:
                ephem = utils.extract_parfile(os.path.join(dirs[0], fns[0]))
                ephem_md5sum = hashlib.md5(ephem).hexdigest()
            except errors.InputError, exc:
                warnings.warn(exc.get_message(), errors.CoastGuardWarning)
                ephem_md5sum = None
            obsinfo.append({'sourcename': arf['name'],
                            'start_mjd': arf['intmjd']+arf['fracmjd'],
                            'obstype': obstype,
                            'nsubbands': len(dirs),
                            'nsubints': len(fns), 
                            'obsband': arf['band'],
                            'backend': get_backend_from_dir(path)})

            values.append({'filepath': listpath,
                           'filename': listname,
                           'stage': 'grouped',
                           'md5sum': utils.get_md5sum(listfn),
                           'ephem_md5sum': ephem_md5sum,
                           'coords': arf['coords'],
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
                # Update obs to have current_file_id set
                update = db.obs.update().\
                            where(db.obs.c.obs_id == obs_id).\
                            values(current_file_id=file_id)
                result = conn.execute(update)
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
        cmbfn = make_combined_file(subdirs, subints, outdir=cmbdir, parfn=parfn, 
                                   backend=filerow['backend'])

        # Pre-compute values to insert because some might be
        # slow to generate
        arf = utils.ArchiveFile(cmbfn)
        if (arf['backend'] == 'ASTERIX') and (arf['nchan'] > 512):
            factor = 0.015625*arf['nchan']/len(subdirs)
            new_nchan = arf['nchan']/factor
            note = "Scrunched from %d to %g channels" % (arf['nchan'], new_nchan)
            utils.print_info("Reducing %s from %d to %g channels" %
                             (cmbfn, arf['nchan'], new_nchan), 2)
            # Scrunch channels
            utils.execute(['pam', '-m', '--setnchn', "%d" % new_nchan, cmbfn])
            # Re-load archive file
            arf = utils.ArchiveFile(cmbfn)
        else:
            note = None

        # Make diagnostic plots
        fullresfn, lowresfn = make_summary_plots(arf)

        values = {'filepath': cmbdir,
                  'filename': os.path.basename(cmbfn),
                  'stage': 'combined',
                  'md5sum': utils.get_md5sum(cmbfn),
                  'filesize': os.path.getsize(cmbfn),
                  'parent_file_id': parent_file_id,
                  'note': note,
                  'coords': arf['coords'],
                  'snr': arf['snr']}
        try:
            ephem = utils.extract_parfile(cmbfn)
            values['ephem_md5sum'] = hashlib.md5(ephem).hexdigest()
        except errors.InputError, exc:
            warnings.warn(exc.get_message(), errors.CoastGuardWarning)
        diagvals = [{'diagnosticpath': os.path.dirname(fullresfn),
                     'diagnosticname': os.path.basename(fullresfn)},
                    {'diagnosticpath': os.path.dirname(lowresfn),
                     'diagnosticname': os.path.basename(lowresfn)}
                   ]
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
            # Insert diagnostic entries
            insert = db.diagnostics.insert().\
                    values(file_id=new_file_id)
            result = conn.execute(insert, diagvals)
            # Update status of parent file's entry
            update = db.files.update(). \
                        where(db.files.c.file_id==parent_file_id).\
                        values(status='processed', \
                                last_modified=datetime.datetime.now())
            conn.execute(update)
            # Update observation length
            update = db.obs.update().\
                        where(db.obs.c.obs_id==obs_id).\
                        values(length=arf['length'],
                               bw=arf['bw'],
                               current_file_id=new_file_id,
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
        global mjd_to_receiver
        if (filerow['obsband'] == 'Lband') and (mjd_to_receiver is not None):
            imjd = int(filerow['start_mjd'])
            rcvr = mjd_to_receiver.get(imjd, 'X')
            if rcvr == '?':
                raise errors.HeaderCorrectionError("Using MJD to receiver mapping "
                                                   "but receiver is unknown (%s)" % rcvr)
            elif rcvr == 'X':
                raise errors.HeaderCorrectionError("Using MJD to receiver mapping "
                                                   "but MJD (%d) has no entry" % imjd)
            elif rcvr == '1':
                # Single pixel receiver
                rcvr = "P200-3"
            elif rcvr == '7':
                # 7-beam receiver
                rcvr = "P217-3"
            else:
                raise errors.HeaderCorrectionError("Using MJD to receiver mapping "
                                                   "but receiver is invalid (%s)" % rcvr)
        else:
            rcvr = None # Receiver will be determined automatically
        corrfn, corrstr, note = correct.correct_header(infn, receiver=rcvr)

        arf = utils.ArchiveFile(corrfn)

        # Move file to archive directory
        archivedir = os.path.join(config.output_location, \
                                config.output_layout) % arf
        archivefn = (config.outfn_template+".corr") % arf
        try:
            os.makedirs(archivedir)
            utils.add_group_permissions(archivedir, "rwx")
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
                  'coords': arf['coords'],
                  'snr': arf['snr']}
        try:
            ephem = utils.extract_parfile(corrfn)
            values['ephem_md5sum'] = hashlib.md5(ephem).hexdigest()
        except errors.InputError, exc:
            warnings.warn(exc.get_message(), errors.CoastGuardWarning)
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
                               current_file_id=file_id,
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
            utils.add_group_permissions(archivedir, "rwx")
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
                  'coords': arf['coords'],
                  'snr': arf['snr']}
        try:
            ephem = utils.extract_parfile(cleanfn)
            values['ephem_md5sum'] = hashlib.md5(ephem).hexdigest()
        except errors.InputError, exc:
            warnings.warn(exc.get_message(), errors.CoastGuardWarning)
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
            # Update current file ID for obs
            update = db.obs.update().\
                        where(db.obs.c.obs_id == obs_id).\
                        values(current_file_id=file_id,
                               last_modified=datetime.datetime.now())
            conn.execute(update)
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
        # Check if file has already been calibrated and failed
        cal_already_failed = False
        with db.transaction() as conn:
            family = get_all_obs_files(filerow['file_id'], db)
            for member in family:
                if (member['stage'] == 'calibrated') and (member['qcpassed'] == False):
                    cal_already_failed = True
                    raise errors.CalibrationError("Obs (ID: %d) has previously been calibrated "
                                                  "and failed (file ID: %d). Will not try again." % 
                                                  (filerow['obs_id'], filerow['file_id']))

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
            caldbrow = calibrate.get_caldb(db, name)
            if caldbrow is None:
                raise errors.DataReductionFailed("No matching calibrator "
                                                 "database row for %s." % name)
            caldbpath = os.path.join(caldbrow['caldbpath'],
                                        caldbrow['caldbname'])
            utils.print_debug("Calibration DB: %s" % caldbpath, 'calibrate')
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
        values['coords'] = arf['coords']
        try:
            ephem = utils.extract_parfile(outpath)
            values['ephem_md5sum'] = hashlib.md5(ephem).hexdigest()
        except errors.InputError, exc:
            warnings.warn(exc.get_message(), errors.CoastGuardWarning)
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
        elif (not cal_already_failed) and can_calibrate(db, obs_id):
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
            # Update current file ID for obs
            update = db.obs.update().\
                        where(db.obs.c.obs_id == obs_id).\
                        values(current_file_id=file_id,
                               last_modified=datetime.datetime.now())
            conn.execute(update)
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
                calibrate.update_caldb(db, arf['name'], force=True)
                reattempt_calibration(db, name)
            finally:
                lock.release()
    return file_id


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
                        values(status='done',
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
    """Return True if observation can be calibrated.
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
    return bool(get_potential_polcal_scans(db, obs_id))


def get_potential_polcal_scans(db, obs_id):
    """Return list of potential polarization calibration scans
        for the given observation.

        NOTE: Scans that have not completed processing or 
            quality control are still considered to be 
            potential calibration scans.

        Inputs:
            db: A database object.
            obs_id: The ID number of an entry in the database.

        Outputs:
            cals: List of potential calibrator scans.
    """
    obsrow = get_obs(db, obs_id)
    if obsrow['obstype'] != 'pulsar':
        raise errors.InputError("Only observations of type 'pulsar' "
                                "can be calibrated. Obstype for obs_id %d: %s" %
                                (obs_id, obsrow['obstype']))
    psrchive_cfg = utils.get_psrchive_configs()
    polcal_validity_minutes = float(psrchive_cfg.get("Database::short_time_scale", 120))
    mjdrange = (obsrow['start_mjd']-polcal_validity_minutes/MINUTES_PER_DAY,
                obsrow['start_mjd']+polcal_validity_minutes/MINUTES_PER_DAY)
    # Now try to find a compatible calibrator scan
    with db.transaction() as conn:
        select = db.select([db.files],
                    from_obj=[db.files.\
                        outerjoin(db.obs,
                            onclause=(db.files.c.obs_id ==
                                        db.obs.c.obs_id))]).\
                    where((db.obs.c.obstype == 'cal') &
                            (db.obs.c.sourcename == ("%s_R" %
                                    obsrow['sourcename'])) &
                            ((db.obs.c.rcvr == obsrow['rcvr']) |
                                (db.obs.c.rcvr.is_(None))) &
                            db.obs.c.start_mjd.between(*mjdrange) &
                            (db.obs.c.bw == obsrow['bw']) &
                            (db.obs.c.freq == obsrow['freq'])).\
                    order_by(db.files.c.added.asc())
        results = conn.execute(select)
        rows = results.fetchall()
        results.close()
    # Only keep most recently added file for each
    # observation. Rows are sorted in the query above.
    obs_ids = []
    for ii in reversed(range(len(rows))):
        if rows[ii]['obs_id'] in obs_ids:
            rows.pop(ii)
        else:
            obs_ids.append(rows[ii]['obs_id'])
    # Throw away observations that failed processing or quality control
    rows = [row for row in rows if (row['status'] != "failed") and 
                                   (row['qcpassed'] != False)]
    mjdnow = rs.utils.mjdnow()
    if not rows and ((mjdnow - obsrow['start_mjd']) < 7):
        # Observation is less than 1 week old.
        # Let's hold out hope that it can be calibrated.
        return ["Obs is less than 7 days old... maybe data still need to be copied"]
    return rows


def get_parent(file_id, db=None):
    # Connect to database if db is None
    db = db or database.Database()

    with db.transaction() as conn:
        select = db.select([db.files.c.parent_file_id]).\
                    where(db.files.c.file_id == file_id)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
        if len(rows) == 1:
            row = rows[0]
            if row['parent_file_id'] is not None:
                select = db.select([db.files]).\
                            where(db.files.c.file_id == row['parent_file_id'])
                result = conn.execute(select)
                parent = result.fetchone()
                result.close()
            else:
                parent = None
        else:
            raise errors.DatabaseError("Bad number of files (%d) with ID=%d!" % (len(rows), file_id))
    return parent


def get_file(file_id, db=None):
    # Connect to database if db is None
    db = db or database.Database()
   
    with db.transaction() as conn:
        select = db.select([db.files]).\
                    where(db.files.c.file_id == file_id)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
        if len(rows) != 1:
            raise errors.DatabaseError("Bad number of files (%d) with ID=%d!" % (len(rows), file_id))
    return rows[0] 


def get_all_obs_files(file_id, db=None):
    # Connect to database if db is None
    db = db or database.Database()
   
    with db.transaction() as conn:
        select = db.select([db.files.c.obs_id]).\
                    where(db.files.c.file_id == file_id)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
        if len(rows) == 1:
            select = db.select([db.files]).\
                        where(db.files.c.obs_id == rows[0]['obs_id']).\
                        order_by(db.files.c.file_id.desc())
            result = conn.execute(select)
            obsfiles = result.fetchall()
            result.close()
        else:
            raise errors.DatabaseError("Bad number of files (%d) with ID=%d!" % (len(rows), file_id))
    return obsfiles


def get_all_ancestors(file_id, db=None):
    # Connect to database if db is None
    db = db or database.Database()

    ancestors = [] 
    parent = get_parent(file_id)
    if parent:
        ancestors.append(parent)
        ancestors.extend(get_all_ancestors(parent['file_id'], db))
    return ancestors
    

def get_all_descendents(file_id, db=None):
    # Connect to database if db is None
    db = db or database.Database()

    decendents = [] 
    children = get_children(file_id)
    decendents.extend(children)
    for child in children:
        decendents.extend(get_all_descendents(child['file_id'], db))
    return decendents


def get_children(file_id, db=None):
    # Connect to database if db is None
    db = db or database.Database()

    with db.transaction() as conn:
        select = db.select([db.files]).\
                    where(db.files.c.parent_file_id == file_id)
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    return rows


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
                            db.obs.c.backend,
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


def get_rawdata_dirs(basedirs=None, priority=[]):
    """Get a list of directories likely to contain asterix data.
        Directories 2 levels deep with a name "YYYYMMDD" are returned.

        Input:
            basedirs: Roots of the directory trees to search.
            priority: List of directories to prioritize.
                (Default: No priorities)

        Output:
            outdirs: List of likely raw data directories.
    """
    if basedirs is None:
        basedirs = config.base_rawdata_dirs
    outdirs = []
    indirs = []
    for basedir in basedirs:
        if not priority:
            # Not prioritizing any specific pulsars
            # use wildcard to match all
            priority = ["*"]
        for name in priority:
            indirs.extend(glob.glob(os.path.join(basedir, name)))
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

    # Try L-band, S-band, and C-band
    for band, subdir_pattern in \
                    zip(['Lband', 'Sband', 'Cband'], ['1'+'[0-9]'*3, '2'+'[0-9]'*3, '[45]'+'[0-9]'*3]):
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


def make_combined_file(subdirs, subints, outdir, parfn=None, effix=False, backend=None):
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
            effix: Change observation site to eff_psrix to correct 
                for asterix clock offsets. (Default: False)
            backend: Name of the backend. (Default: leave as is)

        Outputs:
            outfn: The name of the combined archive.
    """
    # Work in a temporary directory
    tmpdir = tempfile.mkdtemp(suffix="_combine",
                              dir=config.tmp_directory)
    try:
        # Prepare subints
        preppeddirs = combine.prepare_subints(subdirs, subints,
                                      baseoutdir=os.path.join(tmpdir, 'data'),
                                      trimpcnt=6.25, effix=effix, 
                                      backend=backend)
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
    diagnose.make_composite_summary_plot_psrplot(arf, outfn=fullresfn)

    # 6.25 MHz channels
    nchans = arf['bw']/6.25
    preproc = 'C,D,B 128,F %d' % nchans
    if arf['length'] > 60:
        # one minute subintegrations
        preproc += ",T %d" % (arf['length']/60)
    lowresfn = arf.fn+".scrunched.png"
    diagnose.make_composite_summary_plot_psrplot(arf, preproc, outfn=lowresfn)
    
    # Make sure plots are group-readable
    utils.add_group_permissions(fullresfn, "r")
    utils.add_group_permissions(lowresfn, "r")

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

    # Make sure plots are group-readable
    utils.add_group_permissions(fullresfn, "r")
    utils.add_group_permissions(lowresfn, "r")

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

    # Make sure plot is group-readable
    utils.add_group_permissions(outfn, "r")
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
                    from_obj=[db.obs.\
                        outerjoin(db.files,
                            onclause=db.files.c.file_id ==
                                    db.obs.c.current_file_id)]).\
                            where((db.files.c.status == 'toload') |
                                  ((db.files.c.status == 'new') & 
                                   (db.files.c.qcpassed == True) &
                                   (db.files.c.stage == 'calibrated')))
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
                            db.obs.c.obsband,
                            db.obs.c.rcvr,
                            db.obs.c.backend,
                            db.obs.c.start_mjd],
                    from_obj=[db.obs.\
                        outerjoin(db.files,
                            onclause=db.files.c.file_id ==
                                    db.obs.c.current_file_id)]).\
                            where(whereclause)
        if action == 'calibrate':
            select = select.order_by(db.obs.c.obstype.desc())
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
    return db.obs.c.sourcename.like(utils.get_prefname(psrname))


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

    if args.only_action is not None:
        actions_to_perform = [args.only_action]
    else:
        actions_to_perform = [act for act in ACTIONS.keys() \
                              if act not in args.actions_to_exclude]

    global mjd_to_receiver
    if args.lband_rcvr_map is not None:
        mjd_to_receiver = correct.read_receiver_file(args.lband_rcvr_map)
    else:
        mjd_to_receiver = None

    inprogress = []
    try:
        priority_list = []
        for priority_str in args.priority:
            priority_list.extend(parse_priorities(priority_str))
        db = database.Database()

        # Load raw data directories
        print "Loading directories..."
        ndirs = load_directories(db, force=args.reattempt_dirs)
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
                utils.print_info("Will perform the following actions: %s" % 
                                 ", ".join(actions_to_perform), 1)
                for action in actions_to_perform:
                    if action == 'load':
                        rows = get_toload(db)[:nfree]
                    else:
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
    actgroup = parser.add_mutually_exclusive_group()
    actgroup.add_argument("-x", "--exclude", choices=ACTIONS.keys(),
                          default=[], metavar="ACTION", 
                          action='append', dest="actions_to_exclude",
                          help="Action to not perform. Multiple -x/--exclude "
                               "arguments may be provided. Must be one of '%s'. "
                               "(Default: perform all actions.)" %
                               "', '".join(ACTIONS.keys()))
    actgroup.add_argument("--only", choices=ACTIONS.keys(),
                          default=None, metavar="ACTION", 
                          dest="only_action",
                          help="Only perform the given action. Must be one of '%s'. "
                               "(Default: perform all actions.)" %
                               "', '".join(ACTIONS.keys()))
    parser.add_argument("--lband-rcvr-map", dest='lband_rcvr_map', type=str,
                        default=None,
                        help="A text file containing MJD to receiver mapping. "
                             "(Default: Try to determine the receiver "
                             "automatically from observations.)")
    parser.add_argument("--reattempt-dirs", dest="reattempt_dirs",
                        action="store_true",
                        help="Try to reload all directories regardless of "
                             "modification time. Exisiting DB entries will "
                             "not be modified or duplicated. (Default: "
                             "only load recently modified directories.)")
    args = parser.parse_args()
    main()
