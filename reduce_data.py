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
import database

BASEOUTFN_TEMPLATE = "%(backend_L)s_%(rcvr_L)s_%(name_U)s_%(yyyymmdd)s_%(secs)05d_reduced"
SAVE_INTERMEDIATE = True
BASEOUTDIR = "/media/part1/plazarus/timing/asterix/"
OUTDIR_TEMPLATE = os.path.join(BASEOUTDIR, "%(name_U)s/%(rcvr_L)s/%(date:%Y)s")
TMPDIR = "/media/part1/plazarus/timing/asterix/tmp/"
BASE_RAWDATA_DIR = "/media/part2/TIMING/Asterix/"


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


def get_rawdata_dirs(basedir=BASE_RAWDATA_DIR):
    """Get a list of directories likely to contain asterix data.
        Directories 2 levels deep with a name "YYYYMMDD" are returned.

        Input:
            basedir: Root of the directory tree to search.

        Output:
            outdirs: List of likely raw data directories.
    """
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
                    (['Lband', 'Sband'], ['1'+'[0-9]'*3, '2'+'[0-9]'*3]):
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
    basetmpdir = tempfile.mkdtemp(suffix="_reduce", dir=TMPDIR)
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
                baseoutfn = BASEOUTFN_TEMPLATE % arf
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
                outdir = OUTDIR_TEMPLATE % cleanarf 
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
