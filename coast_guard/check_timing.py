#!/usr/bin/env python

import os
import glob
import shutil

import numpy as np

from coast_guard import utils
from coast_guard import list_files
from coast_guard import make_template
from coast_guard import errors
from coast_guard import reduce_data

from toaster.toolkit.timfiles import readers
from toaster.toolkit.timfiles import formatters
from toaster.toolkit.toas import load_toa


STAGES = ['cleaned', 'calibrated', 'current']
RCVRS = ["P217-3", "P200-3", "S110-1"]
BASE_TEMPLATE_DIR = "/media/part1/plazarus/timing/asterix/testing/templates/"
PARFILE_DIR = "/homes/plazarus/research/epta-legacy/"
# Refined clock offsets from Mar 17, 2015
# (see email "Asterix Clock Offsets" to Ramesh, David, Kuo)
TIME_OFFSETS = {56230: "TIME -0.0972851",
                56500: "TIME +0.0972851\nTIME -0.4092691",
                56720: "TIME +0.4092691\nTIME -0.000000612",
                56981: "TIME +0.000000612 \nTIME -0.000000127",
               }
EXTRA_PARFILE_LINES = ['F2 0 0',
                       'F3 0 0',
                       'JUMP MJD 40000 56230',
                       'JUMP MJD 56230 56500',
                       'JUMP MJD 56500 56720',
                       #'JUMP MJD 56720 56760',
                       'JUMP MJD 56720 56981',
                       #'JUMP MJD 56760 99999',
                       'JUMP MJD 56981 99999',
                       #'JUMP -rcvr P200-3',
                       #'JUMP -rcvr P217-3',
                       #'JUMP -rcvr S110-1',
                       #'JUMP -type cleaned',
                       #'JUMP -type calibrated',
                       'JUMP -grp P200-3_clean',
                       'JUMP -grp P217-3_clean',
                       'JUMP -grp S110-1_clean',
                       'JUMP -grp P200-3_cal',
                       'JUMP -grp P217-3_cal',
                       'JUMP -grp S110-1_cal']


def main():
    psrname = utils.get_prefname(args.psrname)

    if args.nchan == 1:
        ext = '.FTp'
        scrunchargs = ['-F']
    elif args.nchan > 1:
        ext = '.Tp.F%d' % args.nchan
        scrunchargs = ['--setnchn', '%d' % args.nchan]
    else:
        raise ValueError("Cannot scrunch using negative number of "
                         "channels (nchan=%d)" % args.nchan)

    #psrdirs = dict([(utils.get_prefname(os.path.basename(dd)),
    #                 os.path.basename(dd))
    #                for dd in glob.glob(os.path.join(PARFILE_DIR, '*'))
    #                if os.path.isdir(dd)])

    #if psrname in psrdirs:
    #    legacydir = os.path.join('/homes/plazarus/research/epta-legacy/',
    #                             psrdirs[psrname])
    #else:
    #    legacydir = None

    # Copy EPTA legacy TOAs
    #if legacydir and not os.path.exists("epta-legacy"):
    #    os.mkdir("epta-legacy")
    #    shutil.copytree(os.path.join(legacydir, "tims"), "epta-legacy/tims")
    #    shutil.copy(os.path.join(legacydir,
    #                             "%s_t2noise.model" % psrdirs[psrname]),
    #                "epta-legacy")

    # Find parfile
    if args.parfile is not None:
        if not os.path.exists(args.parfile):
            raise errors.InputError("Parfile specified (%s) doesn't exist!" %
                                    args.parfile)
        inparfn = args.parfile
    else:
        # Create parfile
        #inparfn = os.path.join('/homes/plazarus/research/epta-legacy/',
        #                       psrdirs[psrname], "%s.par" % psrdirs[psrname])
        inparfn = reduce_data.PARFILES[psrname]
    #intimfn = os.path.join('/homes/plazarus/research/epta-legacy/',
    #                       psrdirs[psrname], "%s_all.tim" % psrdirs[psrname])

    outparfn = "%s.T2.par" % psrname
    with open(inparfn, 'r') as inff, open(outparfn, 'w') as outff:
        for line in inff:
            # Don't copy over JUMPs or EFACs to 'outff'
            if not line.startswith("JUMP") and \
                    not 'EFAC' in line:
                outff.write(line)
        outff.write("\n".join(EXTRA_PARFILE_LINES))

    template_dir = os.path.join(BASE_TEMPLATE_DIR, psrname)
    for stage in STAGES:
        if stage == "current":
            continue
        for rcvr in RCVRS:
            template_name = "%s_%s_%s.std" % (psrname, rcvr, stage)
            # First, check if templates exists
            if not os.path.isfile(os.path.join(template_dir, template_name)):
                # Make template
                utils.print_info("No template (%s) found!" % template_name, 1)
                try:
                    os.makedirs(template_dir)
                except: pass
                try:
                    print psrname, stage, rcvr
                    stdfn = make_template.make_template(template_dir, psrname,
                                                        stage, rcvr)
                    utils.print_info("Made template: %s" % stdfn, 1)
                except errors.TemplateGenerationError:
                    pass

    timfns = []
    for stage in STAGES:
        # List files to reduce
        rows = list_files.get_files([psrname], stage)
        print len(rows)
        fns = {}
        # Initialize list of file names for each receiver
        for rcvr in RCVRS:
            fns[rcvr] = []
        for row in rows:
            if row['stage'] not in ('cleaned', 'calibrated'):
                continue
            fn = os.path.join(row['filepath'], row['filename'])
            fns[row['rcvr']].append(fn)
        stagetimfn = "%s_%s.tim" % (psrname, stage)
        print "Opening %s" % stagetimfn
        stagetimff = open(stagetimfn, 'w')
        # Create file listings and generate TOAs
        for rcvr in RCVRS:
            print rcvr, len(fns[rcvr])
            if not fns[rcvr]:
                # No files
                continue
            # Check for existing scrunched files
            toscrunch = []
            scrunchedfns = []
            scrunchdir = os.path.join("scrunched", rcvr)
            for fn in fns[rcvr]:
                scrunchfn = os.path.join(scrunchdir, os.path.basename(fn)+ext)
                scrunchedfns.append(scrunchfn)
                if not os.path.exists(scrunchfn):
                    toscrunch.append(fn)
            # Scrunch files
            try:
                os.makedirs(scrunchdir)
            except: pass
            print "Working on %s %s" % (rcvr, stage)
            for fn in utils.show_progress(toscrunch, width=50):
                # Create a copy of the file with the 'eff_psrix' site
                cmd = ['psredit', '-c', 'site=eff_psrix', '-O', scrunchdir, fn]
                cmd.extend(['-e', fn.split('.')[-1]+ext])
                utils.execute(cmd)
                arfn = os.path.join(scrunchdir, os.path.basename(fn+ext))
                parfn = utils.get_norm_parfile(arfn)
                # Re-install ephemeris
                cmd = ['pam', '-Tp', '-E', parfn, '-m', arfn]+scrunchargs
                utils.execute(cmd)

            toas = []
            mjds = []
            for row in rows:
                if row['rcvr'] != rcvr:
                    continue
                if row['stage'] not in ('cleaned', 'calibrated'):
                    continue
                template_name = "%s_%s_%s.std" % (psrname, rcvr, row['stage'])
                template = os.path.join(template_dir, template_name)
                # Generate TOAs
                fn = os.path.join(scrunchdir, row['filename']) + ext
                print fn
                stdout, stderr = utils.execute(["pat", "-T", "-A", "FDM",
                                                "-f", "tempo2", "-C", "rcvr chan", "-d",
                                                "-s", template, fn])
                # Parse TOAs
                toalines = stdout.split('\n')
                for line in toalines:
                    toainfo = readers.tempo2_reader(line)
                    if toainfo is not None:
                        # Formatter expects 'file' field to be called 'rawfile'
                        toainfo['rawfile'] = toainfo['file']
                        toainfo['telescope_code'] = toainfo['telescope']
                        toainfo['type'] = stage
                        toainfo['rcvr'] = rcvr
                        toainfo['file_id'] = row['file_id']
                        toainfo['obs_id'] = row['obs_id']
                        toainfo['shortstage'] = row['stage'][:2].upper()
                        if row['stage'] == 'cleaned':
                            toainfo['grp'] = "%s_clean" % rcvr
                        else:
                            toainfo['grp'] = "%s_cal" % rcvr
                        toainfo['chan'] = toainfo['extras']['chan']
                        toas.append(toainfo)
                        mjds.append(toainfo['imjd'])
            # Sort TOAs
            utils.sort_by_keys(toas, ['fmjd', 'imjd'])

            # Format timfile
            sysflag = 'EFF.AS.%(rcvr)s.%(shortstage)s'
            timlines = formatters.tempo2_formatter(toas, flags=[('rcvr', '%(rcvr)s'),
                                                                ('type', '%(type)s'),
                                                                ('grp', '%(grp)s'),
                                                                ('sys', sysflag),
                                                                ('obsid', '%(obs_id)d'),
                                                                ('fileid', '%(file_id)d'),
                                                                ('chan', '%(chan)s')])

            mjds.sort()
            #offsetmjds = sorted(TIME_OFFSETS.keys())
            #inds = np.searchsorted(mjds, offsetmjds)+1
            # Insert extra lines from back of list
            #for ind, key in reversed(zip(inds, offsetmjds)):
            #    timlines[ind:ind] = ["\n"+TIME_OFFSETS[key]+"\n"]

            # Write out timfile
            timfn = "%s_%s_%s.tim" % (psrname, rcvr, stage)
            with open(timfn, 'w') as ff:
                for line in timlines:
                    ff.write(line+"\n")
            utils.print_info("Wrote out timfile: %s" % timfn)
            timfns.append(timfn)
            stagetimff.write("INCLUDE %s\n" % timfn)
        stagetimff.close()

    #outtimfn = os.path.join("epta-legacy", os.path.basename(intimfn))
    #with open(intimfn, 'r') as inff, open(outtimfn, 'w') as outff:
    #    for line in inff:
    #        outff.write(line)
    #    for rcvr in RCVRS:
    #        timfn = "%s_%s_cleaned.tim" % (psrname, rcvr)
    #        if os.path.exists(timfn):
    #            outff.write("INCLUDE ../%s\n" % timfn)

    # Count TOAs
    #toas = load_toa.parse_timfile(outtimfn, determine_obssystem=False)
    systems = {}
    #for toa in toas:
    #    if toa['is_bad']:
    #        continue
    #    if not 'sys' in toa['extras']:
    #        print toa
    #    else:
    #        nn = systems.get(toa['extras']['sys'], 0)
    #        systems[toa['extras']['sys']] = nn+1

    outparfn = "%s.T2.par" % psrname
    #outparfn2 = os.path.join("epta-legacy", os.path.basename(inparfn))
    with open(inparfn, 'r') as inff, open(outparfn, 'w') as outff:#, \
            #open(outparfn2, 'w') as outff2:
        for line in inff:
            # Don't copy over JUMPs or EFACs to 'outff'
            # Copy JUMPs and EFACs to 'outff2' and fit
            #if line.startswith("JUMP"):
            #    if "-sys" in line:
            #        obssys = line.split()[2]
            #        if systems.get(obssys, 0):
            #            # Observing system has TOAs
            #            # Replace all system jumps by 0 and set the fit flag
            #            outff2.write(" ".join(line.split()[:3])+" 0 1\n")
            #    else:
            #        outff2.write(line)
            #elif line.startswith("T2EFAC"):
            #    outff2.write(line)
            #elif line.startswith("NITS"):
            #    pass
            #else:
                outff.write(line)
                # Remove fit-flags for 'outff2'
                #outff2.write(" ".join(line.split()[:2])+'\n')
        outff.write("\n".join(EXTRA_PARFILE_LINES))
        #outff2.write("\n".join(["JUMP -sys EFF.AS.%s.CL 0 1" % rcvr for rcvr in RCVRS]))
        #outff2.write("\nNITS 3\n")

    # Create a master timfile
    master_timfn = "%s_all.tim" % psrname
    with open(master_timfn, 'w') as ff:
        for timfn in timfns:
            ff.write("INCLUDE %s\n" % timfn)
    utils.print_info("Wrote out master timfile: %s" % master_timfn)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Check timing of a pulsar.")
    parser.add_argument('-p', '--psr', dest='psrname', type=str,
                        required=True,
                        help='Name of the pulsar to fetch files for.')
    parser.add_argument('-E', '--parfile', dest='parfile', type=str,
                        help="Parfile to prepare for checking timing."
                             "(Default: use parfile from %s" % PARFILE_DIR)
    parser.add_argument('--nchan', dest='nchan', type=int,
                        default=1,
                        help="Number of channels to use (both when scrunching "
                             "and when generating TOAs.; Default=1)")
    parser.add_argument('--effix', dest='effix', action='store_true',
                        help="Set site to 'eff_psrix' to get TEMPO2 to "
                             "use clock correction file that includes the "
                             "Asterix clock offsets. "
                             "(Default: keep site=effelsberg)")
    args = parser.parse_args()
    main()
