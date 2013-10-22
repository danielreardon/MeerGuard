#!/usr/bin/env python

import multiprocessing
import subprocess
import warnings
import tempfile
import datetime
import fnmatch
import os.path
import shutil
import glob
import sys
import os

import config
import utils
import diagnose
import cleaners
import combine
import database
import clean_utils
import errors

import pyriseset as rs

SAVE_INTERMEDIATE = True

EFF = rs.sites.load('effelsberg')

# Observing log fields:
#                  name   from-string converter
OBSLOG_FIELDS = (('localdate', rs.utils.parse_datestr), \
                 ('scannum', str), \
                 ('utcstart', rs.utils.parse_timestr), \
                 ('lststart', rs.utils.parse_timestr), \
                 ('name', str), \
                 ('az', float), \
                 ('alt', float), \
                 ('catalog_rastr', str), \
                 ('datalog_decstr', str))


RCVR_INFO = {'P217-3': 'rcvr:name=P217-3,rcvr:hand=-1,rcvr:basis=cir', \
             'S110-1': 'rcvr:name=S110-1,rcvr:hand=-1,rcvr:basis=cir', \
             'P200-3': 'rcvr:name=P200-3,rcvr:hand=-1,rcvr:basis=cir'}


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
    ninserts = 0
    dirs = get_rawdata_dirs(*args, **kwargs)
    nn = len(dirs)
    for ii, path in utils.show_progress(enumerate(dirs), tot=nn, width=50):
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


def load_groups(db, dirrow):
    """Given a row from the DB's directories table create a group 
        listing from the asterix data stored in the directories 
        and load it into the database.

        Inputs:
            db: Database object to use.
            dirrow: A row from the directories table.

        Outputs:
            ninserts: The number of group rows inserted.
    """
    path = dirrow['path']
    dir_id = dirrow['dir_id']

    ninserts = 0
    values = []
    for dirs, fns, band in zip(*make_groups(path)):
        fns.sort()
        listfn = os.path.join(config.output_location, 'groups', \
                                "%s_%s_%dsubints.txt" % \
                                (fns[0], band, len(fns)))
        combine.write_listing(dirs, fns, listfn)
        listpath, listname = os.path.split(listfn)
        values.append({'listpath': listpath, \
                       'listname': listname, \
                       'md5sum': utils.get_md5sum(listfn)})
    try:
        with db.transaction() as conn:
            version_id = utils.get_version_id(db) 
            insert = db.groupings.insert().\
                        values(version_id = version_id, \
                               dir_id = dir_id)
            conn.execute(insert, values)
            update = db.directories.update().\
                        where(db.directories.c.dir_id==dir_id).\
                        values(status='grouped')
            conn.execute(update)
    except:
        with db.transaction() as conn:
            update = db.directories.update().\
                        where(db.directories.c.dir_id==dir_id).\
                        values(status='failed')
            conn.execute(update)
        raise
    else:
        # The following line is only reached if the execution
        # above doesn't raise an exception
        ninserts += len(values)
    return ninserts


def get_rawdata_dirs(basedir=None):
    """Get a list of directories likely to contain asterix data.
        Directories 2 levels deep with a name "YYYYMMDD" are returned.

        Input:
            basedir: Root of the directory tree to search.

        Output:
            outdirs: List of likely raw data directories.
    """
    if basedir is None:
        basedir = config.base_rawdata_dir
    outdirs = []
    indirs = glob.glob(os.path.join(basedir, '*'))
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
            band_list: List of band names.
    """
    usedirs_list = []
    groups_list = []
    band_list = []

    # Try L-band and S-band
    for band, subdir_pattern in \
                    zip(['Lband', 'Sband'], ['1'+'[0-9]'*3, '2'+'[0-9]'*3]):
        subdirs = glob.glob(os.path.join(path, subdir_pattern))
        if subdirs:
            utils.print_info("Found %d freq sub-band dirs for %s in %s. " \
                        "Will group sub-ints contained" % \
                        (len(subdirs), band, path), 2)
            usedirs, groups = combine.group_subband_dirs(subdirs)
            # Keep track of the groups and directories used
            for grp in groups:    
                band_list.append(band)
                groups_list.append(grp)
                usedirs_list.append(usedirs)
    return usedirs_list, groups_list, band_list


def make_combined_file(subdirs, subints):
    """Given lists of directories and subints combine them,
        and correct the resulting file's header.

        Inputs:
            subdirs: List of sub-band directories containing 
                sub-ints to combine
            subints: List of subint files to be combined.
                (NOTE: These are the file name only (i.e. no path)
                    Each file listed should appear in each of the
                    subdirs.)
        
        Outputs:
            outfn: The name of the combined, corrected file.
    """
    # Work in a temporary directory
    tmpdir = tempfile.mkdtemp(suffix="_combine", \
                                    dir=config.tmp_directory)
    try:
        # Prepare subints
        preppeddirs = prepare_subints(subdirs, subints, \
                            baseoutdir=os.path.join(tmpdir, 'data'))
        # Combine the now-prepped subints
        cmbfn = combine.combine_subints(preppeddirs, subints, outdir=tmpdir)
        # Correct the header
        correct_header(cmbfn)
        # Rename the corrected file
        raise NotImplementedError
    except:
        raise
    finally:
        warnings.warn("Not cleaning up temporary directory (%s)" % tmpdir)
        #utils.print_info("Removing temporary directory (%s)" % tmpdir, 1)
        #shutil.rmtree(tmpdir)

        
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
    tmpsubdirs = []
    for subdir in subdirs:
        freqdir = os.path.split(os.path.abspath(subdir))[-1]
        freqdir = os.path.join(baseoutdir, freqdir)
        os.makedirs(freqdir)
        fns = [os.path.join(subdir, fn) for fn in subints]
        utils.execute(['paz', '-j', 'convert psrfits', \
                            '-E', '6.25', '-O', freqdir] + fns)
        tmpsubdirs.append(freqdir)
    utils.print_info("Prepared %d subint fragments in %d freq sub-dirs" % \
                    (len(subints), len(subdirs)), 3)
    return tmpsubdirs


def correct_header(arfn):
    """Correct header of asterix data in place.

        Input:
            arfn: The name of the input archive file.

        Output:
            corrstr: The parameter string of corrections used with psredit.
    """
    # Load archive
    arf = utils.ArchiveFile(arfn)
    if arf['rcvr'].upper() in RCVR_INFO:
        rcvr = arf['rcvr']
    elif arf['freq'] > 2000: 
        # S-band
        rcvr = 'S110-1'
    else:
        ar = arf.get_archive()
        nchan = ar.get_nchan()
        # Scrunch
        ar.pscrunch()
        ar.tscrunch()
        # Get the relevant data
        chnwts = clean_utils.get_chan_weights(ar).astype(bool)
        stddevs = ar.get_data().squeeze().std(axis=1)
        bot = stddevs[:nchan/8][chnwts[:nchan/8]].mean()
        top = stddevs[nchan/8:][chnwts[nchan/8:]].mean()
        if top/bot > 5:
            # L-band receiver
            rcvr = 'P200-3'
        elif top/bot < 2:
            # 7-beam receiver
            rcvr = 'P217-3'
        else:
            raise utils.HeaderCorrectionError("Cannot determine receiver.")
    corrstr = "%s,be:name=asterix" % RCVR_INFO[rcvr]
    if arf['name'].endswith("_R"):
        corrstr += ",type=PolnCal"
    else:
        corrstr += ",type=Pulsar"
    if arf['name'].endswith('_R') or arf['ra'].startswith('00:00:00'):
        # Correct coordinates
        obsinfo = get_obslog_entry(arf)
        ra_deg, decl_deg = EFF.get_skyposn(obsinfo['alt'], obsinfo['az'], \
                                            lst=obsinfo['lststart'])
        rastr = rs.utils.deg_to_hmsstr(ra_deg, decpnts=3)[0]
        decstr = rs.utils.deg_to_dmsstr(decl_deg, decpnts=2)[0]
        if decstr[0] not in ('-', '+'):
            decstr = "+" + decstr
        corrstr += ",coord=%s%s" % (rastr, decstr)
    utils.execute(['psredit', '-m', '-c', corrstr, arfn])
    return corrstr


def get_obslog_entry(arf):
    """Given an archive file, find the entry in the observing log.

        Input:
            arf: ArchiveFile object.

        Output:
            obsinfo: A dictionary of observing information.
    """
    # Get date of observation
    obsdate = rs.utils.mjd_to_datetime(arf['mjd'])
    obsutc = obsdate.time()
    obsutc_hours = obsutc.hour+(obsutc.minute+(obsutc.second)/60.0)/60.0

    # Get log file
    # NOTE: Date in file name is when the obslog was written out
    obslogfns = glob.glob(os.path.join(config.obslog_dir, "*.prot"))
    obslogfns.sort()
    for currfn in obslogfns:
        currdate = datetime.datetime.strptime(os.path.split(currfn)[-1], \
                                            '%y%m%d.prot')
        obslogfn = currfn
        if currdate > obsdate:
            break
    if obslogfn is None:
        raise errors.HeaderCorrectionError("Could not find a obslog file " \
                                    "from before the obs date (%s)." % \
                                    obsdate.strftime("%Y-%b-%d"))

    with open(obslogfn, 'r') as obslog:
        logentries = []
        bestoffset = 1e10
        for line in obslog:
            valstrs = line.split()
            if len(valstrs) < len(OBSLOG_FIELDS):
                continue
            currinfo = {}
            for (key, caster), valstr in zip(OBSLOG_FIELDS, valstrs):
                currinfo[key] = caster(valstr)
            if utils.get_prefname(currinfo['name']) != arf['name']:
                continue
            utc_hours = currinfo['utcstart'][0]
            offset = obsutc_hours - utc_hours
            if offset*3600 < 120:
                logentries.append(currinfo)
        if len(logentries) != 1:
            raise errors.HeaderCorrectionError("Bad number (%d) of entries " \
                                "in obslog (%s) with correct source name " \
                                "within 120 s of observation (%s) start " \
                                "time (UTC: %s)" % \
                                (len(logentries), obslogfn, arf.fn, obsutc))
        return logentries[0]


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
    
    preproc = 'C,D,B 128,F 32'
    if arf['nsub'] > 32:
        preproc += ",T 32"
    lowresfn = arf.fn+".scrunched.png"
    diagnose.make_composite_summary_plot(arf, preproc, outfn=lowresfn)
 
    return fullresfn, lowresfn


def reduce_directory(path):
    # Create temporary working directory
    basetmpdir = tempfile.mkdtemp(suffix="_reduce", \
                                    dir=config.tmp_directory)
    utils.print_info("Reducing data in %s. Temporary working directory: %s" % \
                (path, basetmpdir), 2)
    try:
        tmpdir = os.path.join(basetmpdir, 'work')
        toignore = lambda visitdir, xx: [x for x in xx \
                        if (os.path.isfile(x) and not x.endswith('.ar'))]
        # Copy *.ar files to working directory
        shutil.copytree(path, tmpdir, ignore=toignore)
        utils.execute(['chmod', '-R', '700', tmpdir])
        utils.print_info("Copied data to working directory.", 3)
        # Change to working directory
        os.chdir(tmpdir)
        # Prepare copied files
        freqdirs = {}
        nfragments = 0
        for (dirpath, dirnames, filenames) in os.walk(tmpdir):
            utils.print_debug("Walking through data directory %s. " \
                            "Found %d directories and %d files." % \
                            (dirpath, len(dirnames), len(filenames)), 'reduce')
            if fnmatch.fnmatch(os.path.split(dirpath)[-1], '[12]???'):
                # Directory is a frequency sub-band
                dirs = freqdirs.setdefault(os.path.split(dirpath)[-1][0], [])
                dirs.append(dirpath)
                arfns = [os.path.join(dirpath, xx) for xx in filenames \
                                if xx.endswith('.ar')]
                nfragments += len(arfns)
                if arfns:
                    # Convert files to PSRFITS format
                    utils.execute(['psrconv', '-m', '-o', 'PSRFITS'] + arfns) 
                    # Remove sub-band edges
                    utils.execute(['paz', '-E', '6.25', '-m'] + arfns)
        utils.print_info("Prepared %d subint fragments in %d freq sub-dirs" % \
                        (nfragments, len(freqdirs)), 3)
        for dirs in freqdirs.values():
            # Create a sub-directory
            subdir = tempfile.mkdtemp(suffix="_subdir", dir=tmpdir)
            os.chdir(subdir)
            # Create directory for combined files
            os.mkdir('combined_files')
            
            # Combine sub-bands for each sub-int independently
            utils.execute(['combine_ff.sh'] + dirs, stderr=open(os.devnull, 'w'))

            cmbsubints = glob.glob(os.path.join(subdir, 'combined_*.ar'))
            # Join combined sub-ints together
            utils.execute(['psradd', '-O', 'combined_files', '-autoT', '-g', \
                            '3600', '-G', '119'] + cmbsubints)
            cmbfns = glob.glob(os.path.join(subdir, 'combined_files', 'combined_*'))
            utils.print_info("Combined subints into %d files" % len(cmbfns), 2)
            for tmp in cmbfns:
                to_save = [] # List of files to copy to results directory
                # Create ArchiveFile object
                arf = utils.ArchiveFile(tmp)
                # Set configurations
                config.cfg.load_configs_for_archive(arf)
                
                # Adjust header in preparation for calibration
                if arf['name'].endswith("_R"):
                    # Is a calibration scan
                    utils.execute(['psredit', '-m', '-c', 'rcvr:hand=-1,rcvr:basis=cir,type=PolnCal', tmp])
                    cleanext = ".pcal"
                else:
                    utils.execute(['psredit', '-m', '-c', 'rcvr:hand=-1,rcvr:basis=cir,type=Pulsar', tmp])
                    cleanext = ".ar"
                arf = utils.correct_asterix_header(arf)
                # Reload configuration because header has changed
                config.cfg.load_configs_for_archive(arf)
                
                # Base name (ie no extension) of output file
                baseoutfn = config.outfn_template % arf
                cleanfn = baseoutfn+cleanext
 
                # Rename combined file
                cmbfn = baseoutfn+".cmb"
                os.rename(arf.fn, cmbfn)
                arf.fn = cmbfn
                arf.get_archive().set_filename(cmbfn)
 
                # Make pre-cleaning diagnostic plots
                to_save.extend(make_summary_plots(arf))
 
                # Clean the data
                utils.print_info("Cleaning %s" % arf.fn, 1)
                # Load cleaners here because each data file might
                # have different configurations. The configurations
                # are set when the cleaner is loaded.
                cleaner_queue = [cleaners.load_cleaner('rcvrstd'), \
                                 cleaners.load_cleaner('surgical')]
 
                for cleaner in cleaner_queue:
                    cleaner.run(arf.get_archive())
                arf.get_archive().unload(cleanfn)
                to_save.append(cleanfn)
                
                cleanarf = utils.ArchiveFile(cleanfn)
                
                # Make post-cleaning diagnostic plots
                to_save.extend(make_summary_plots(cleanarf))
 
                if cleanarf['name'].endswith("_R"):
                    # This is a calibration scan

                    # Reduce number of channels to 16 per subband
                    # We use the number of subbands because occasionally some
                    # are missing, meaning we don't expect the full 128 channels
                    nchans = 16*len(dirs)
                    utils.execute(['pam', '--setnchn', '%d' % nchans, '-T', '-e', 'pcal.T', cleanfn])
                    to_save.append(cleanfn+'.T')
                else:
                    if SAVE_INTERMEDIATE:
                        # Copy combined file (before cleaning) to output directory
                        to_save.append(cmbfn)
                
                # Copy results files
                outdir = os.path.join(config.output_location, \
                                config.output_layout) % cleanarf 
                # Create output directory, if necessary
                if not os.path.exists(outdir):
                    os.makedirs(outdir)
                for fn in to_save:
                    shutil.copy(fn, os.path.join(outdir, os.path.split(fn)[-1]))
    finally:
        #warnings.warn("Not cleaning up temporary directory (%s)" % tmpdir)
        #utils.print_info("Removing temporary directory (%s)" % tmpdir, 1)
        shutil.rmtree(tmpdir)


def main():
    if args.numproc > 1:
        pool = multiprocessing.Pool(processes=args.numproc)
        results = []
        paths = []
        for path in args.path:
            paths.append(path)
            results.append(pool.apply_async(reduce_directory, args=(path,)))
        pool.close()
        pool.join()
 
        # Check results
        for path, result in zip(paths, results):
            result.get()
    else:
        for path in args.path:
            reduce_directory(path)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Automated reduction " \
                                    "of Asterix data.")
    parser.add_argument("path", nargs='+', type=str,
                        help="Directories containing Asterix data " \
                            "to reduce. Each directory listed is " \
                            "assumed to contain one subdirectory " \
                            "for each frequency sub-band. Each " \
                            "directory listed is reduced independently.")
    parser.add_argument("-P", "--num-procs", dest='numproc', type=int, \
                        default=1, \
                        help="Number of processes to run simultaneously.")
    args = parser.parse_args()
    main()
