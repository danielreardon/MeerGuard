#!/usr/bin/env python

import os
import sys
import glob

import numpy as np

from coast_guard import utils
from coast_guard import list_files
from coast_guard import make_template
from coast_guard import errors
from toaster.toolkit.timfiles import readers
from toaster.toolkit.timfiles import formatters


STAGES = ['cleaned', 'calibrated']
RCVRS = ["P217-3", "P200-3", "S110-1"]
BASE_TEMPLATE_DIR = "/media/part1/plazarus/timing/asterix/testing/templates/"
PARFILE_DIR = "/homes/plazarus/research/epta-legacy/"
TIME_OFFSETS = {56230: "TIME -0.0972846",
                56500: "TIME +0.0972846\nTIME -0.409269648",
                56720: "TIME +0.409269648"}
EXTRA_PARFILE_LINES = ['JUMP MJD 40000 56230',
                       'JUMP MJD 56230 56500',
                       'JUMP MJD 56500 99999',
                       'JUMP -rcvr P200-3',
                       'JUMP -rcvr P217-3',
                       'JUMP -rcvr S110-1',
                       'JUMP -type cleaned',
                       'JUMP -type calibrated',
                       'JUMP -grp P200-3_clean',
                       'JUMP -grp P217-3_clean',
                       'JUMP -grp S110-1_clean',
                       'JUMP -grp P200-3_cal',
                       'JUMP -grp P217-3_cal',
                       'JUMP -grp S110-1_cal']


def main():
    psrname = utils.get_prefname(args.psrname)

    # Find parfile
    if args.parfile is not None:
        if not os.path.exists(args.parfile):
            raise errors.InputError("Parfile specified (%s) doesn't exist!" %
                                    args.parfile)
        inparfn = args.parfile
    else:
        psrdirs = dict([(utils.get_prefname(os.path.basename(dd)),
                         os.path.basename(dd))
                        for dd in glob.glob(os.path.join(PARFILE_DIR, '*'))
                        if os.path.isdir(dd)])
        # Create parfile
        inparfn = os.path.join('/homes/plazarus/research/epta-legacy/',
                               psrdirs[psrname], "%s.par" % psrdirs[psrname])
    outparfn = "%s.T2.par" % psrname
    with open(inparfn, 'r') as inff, open(outparfn, 'w') as outff:
        for line in inff:
            # Don't copy over JUMPs or EFACs
            if line.startswith("JUMP") or line.startswith("T2EFAC"):
                continue
            outff.write(line)
        outff.write("\n".join(EXTRA_PARFILE_LINES))

    template_dir = os.path.join(BASE_TEMPLATE_DIR, psrname)
    for stage in STAGES:
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
                    stdfn = make_template.make_template(template_dir, psrname,
                                                        stage, rcvr)
                    utils.print_info("Made template: %s" % stdfn, 1)
                except errors.TemplateGenerationError:
                    pass

    timfns = []
    for stage in STAGES:
        # List files to reduce
        rows = list_files.get_files(psrname, stage)
        fns = {}
        # Initialize list of file names for each receiver
        for rcvr in RCVRS:
            fns[rcvr] = []
        for row in rows:
            fn = os.path.join(row['filepath'], row['filename'])
            fns[row['rcvr']].append(fn)

        # Create file listings and generate TOAs
        for rcvr in RCVRS:
            if not fns[rcvr]:
                # No files
                continue
            # Check for existing scrunched files
            toscrunch = []
            scrunchedfns = []
            scrunchdir = os.path.join("scrunched", rcvr)
            for fn in fns[rcvr]:
                scrunchfn = os.path.join(scrunchdir, os.path.basename(fn)+".DTFp")
                scrunchedfns.append(scrunchfn)
                if not os.path.exists(scrunchfn):
                    toscrunch.append(fn)
            # Scrunch files
            try:
                os.makedirs(scrunchdir)
            except: pass
            print "Working on %s %s" % (rcvr, stage)
            for fn in utils.show_progress(toscrunch, width=50):
                utils.execute(['pam', '-DFTp', '-u', scrunchdir, '-e',
                               fn.split('.')[-1]+'.DTFp', fn])

            template_name = "%s_%s_%s.std" % (psrname, rcvr, stage)
            template = os.path.join(template_dir, template_name)
            # Create file listing
            listfn = "%s_%s_%s_listing.txt" % (psrname, rcvr, stage)
            with open(listfn, 'w') as ff:
                for fn in scrunchedfns:
                    ff.write(fn+"\n")
            # Generate TOAs
            stdout, stderr = utils.execute(["pat", "-TF", "-M", listfn,
                                            "-f", "tempo2", "-C", "rcvr",
                                            "-s", template])
            # Parse TOAs
            toalines = stdout.split('\n')
            toas = []
            mjds = []
            for line in toalines:
                toainfo = readers.tempo2_reader(line)
                if toainfo is not None:
                    # Formatter expects 'file' field to be called 'rawfile'
                    toainfo['rawfile'] = toainfo['file']
                    toainfo['telescope_code'] = toainfo['telescope']
                    toainfo['type'] = stage
                    toainfo['rcvr'] = rcvr
                    if stage == 'cleaned':
                        toainfo['grp'] = "%s_clean" % rcvr
                    else:
                        toainfo['grp'] = "%s_cal" % rcvr
                    toas.append(toainfo)
                    mjds.append(toainfo['imjd'])
            # Sort TOAs
            utils.sort_by_keys(toas, ['fmjd', 'imjd'])

            # Format timfile
            timlines = formatters.tempo2_formatter(toas, flags=[('rcvr', '%(rcvr)s'),
                                                                ('type', '%(type)s'),
                                                                ('grp', '%(grp)s')])

            mjds.sort()
            offsetmjds = sorted(TIME_OFFSETS.keys())
            inds = np.searchsorted(mjds, offsetmjds)+1
            # Insert extra lines from back of list
            for ind, key in reversed(zip(inds, offsetmjds)):
                timlines[ind:ind] = ["\n"+TIME_OFFSETS[key]+"\n"]

            # Write out timfile
            timfn = "%s_%s_%s.tim" % (psrname, rcvr, stage)
            with open(timfn, 'w') as ff:
                for line in timlines:
                    ff.write(line+"\n")
            utils.print_info("Wrote out timfile: %s" % timfn)
            timfns.append(timfn)

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
    args = parser.parse_args()
    main()
