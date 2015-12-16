#!/usr/bin/env python
"""
make_template.py

Make a template using paas and load it into the database.

Patrick Lazarus, Mar 14, 2014
"""

import os
import shutil
import tempfile

from coast_guard import config
from coast_guard import utils
from coast_guard import list_files
from coast_guard import errors


def get_files_to_combine(rows, max_span=1, min_snr=0):
    """Given a list of result sets from the database return a list of
        filenames to combine to make a template.

        Inputs:
            rows: A list of database result sets as returned by
                'get_files()'.
            max_span: The maximum allowable span, in days, from the 
                first data file to the last data file to combine. 
                (Default: 1 day)
            min_snr: Ignore data files with SNR lower than this value.
                (Default: 0)

        Output:
            files: A list of file names to combine.
    """
    utils.sort_by_keys(rows, ['start_mjd'])
    info = []
    for ii, row in enumerate(rows):
        jj = ii
        tot = 0
        for jj in range(ii, len(rows)):
            if (rows[jj]['start_mjd']-row['start_mjd']) > max_span:
                break
            snr = (rows[jj]['snr'] or 0)  # This will replace None values with 0
            if snr >= min_snr:
                tot += snr
            jj += 1
        info.append((ii, tot, jj-ii))
    if not info:
        return []
    ind, snr, nn = max(info, key=lambda aa: aa[1])
    utils.print_info("Highest total SNR is %g for %d files starting "
                     "at index %d." % (snr, nn, ind), 2)
    touse = rows[ind:ind+nn]
    utils.sort_by_keys(touse, ['snr_r'])
    return [os.path.join(rr['filepath'], rr['filename']) 
            for rr in touse if (rr['snr'] or 0) >= min_snr]


def combine_files(rawfns):
    """Combine raw data files using psradd. The files are
        blindly combined.

        Intput:
            rawfns: A list of data files to combine.

        Output:
            cmbfn: The path to the combined fully scrunched file.
    """
    tmpfile, tmpfn = tempfile.mkstemp(suffix='.cmb', 
                                      dir=config.tmp_directory)
    os.close(tmpfile)
    
    cmd = ['psradd', '-F', '-ip', '-P', '-j', 'DTFp', '-T', '-o', tmpfn] + rawfns
    utils.execute(cmd)
    return tmpfn


def make_template(outdir, psrname, stage, rcvr, max_span=1, min_snr=0):
    if os.path.isdir(outdir):
        outdir = outdir
    else:
        raise errors.InputError("Output directory (%s) doesn't exist!" %
                                outdir)
    filerows = list_files.get_files([psrname], stage, rcvr)
    print "Found %d matching files" % len(filerows)
    fns = get_files_to_combine(filerows, max_span, min_snr)
    if not fns:
        raise errors.TemplateGenerationError("No files for type=%s, "
                                             "psr=%s, rcvr=%s" %
                                             (stage, psrname, rcvr))
    print "Combining %d files" % len(fns)
    cmbfn = combine_files(fns)

    runpaas = True
    tmpdir = tempfile.mkdtemp(suffix="cg_paas", dir=config.tmp_directory)
    while runpaas:
        try:
            print "Running paas"
            utils.execute(['paas', '-D', '-i', cmbfn], dir=tmpdir)
        except:
            if raw_input("Failure! Give up? (y/n): ").lower()[0] == 'y':
                runpaas = False
        else:
            if raw_input("Success! Keep template? (y/n): ").lower()[0] == 'y':
                runpaas = False
                outbasenm = os.path.join(outdir,
                                         "%s_%s_%s" % (psrname, rcvr, stage))
                tmpbasenm = os.path.join(tmpdir, 'paas')
                shutil.copy(tmpbasenm+'.m', outbasenm+'.m')
                shutil.copy(tmpbasenm+'.std', outbasenm+'.std')
                shutil.copy(cmbfn, outbasenm+".add")
    # Clean up paas files
    try:
        shutil.rmtree(tmpdir)
    except: pass
    try:
        os.remove(cmbfn)
    except: pass
    return outbasenm+'.std'


def main():
    if args.outdir is None:
        outdir = os.getcwd()
    else:
        outdir = args.outdir
    psrname = utils.get_prefname(args.psr)
    stdfn = make_template(outdir, psrname, args.stage, args.rcvr,
                          args.max_span, args.min_snr)
    print "Made template: %s", stdfn


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Combine multiple files close "
                                                "in MJD to create a high-SNR "
                                                "profile to generate a template "
                                                "using paas")
    parser.add_argument('-p', '--psr', dest='psr', type=str, 
                        required=True,
                        help="The pulsar to create a template for.")
    parser.add_argument('--rcvr', dest='rcvr', type=str, required=True,
                        help="The name of the receiver for "
                             "which to make a template.")
    parser.add_argument('-C', "--calibrated", dest='stage', action='store_const',
                        default='cleaned', const='calibrated',
                        help="Make template from calibrated pulsar observations.")
    parser.add_argument("-m", "--min-snr", dest='min_snr',
                        type=float, default=0,
                        help="Minimum archive SNR to consider when "
                             "adding data files. (Default: no minimum)")
    parser.add_argument("-g", "--max-span", dest='max_span',
                        type=float, default=1,
                        help="Maximum span, in days, between observations when "
                             "adding data files. (Default: 1 day)")
    parser.add_argument("-o", "--output-dir", dest='outdir', type=str,
                        help="Output directory. (Default: current directory)")
    args = parser.parse_args()
    main()
