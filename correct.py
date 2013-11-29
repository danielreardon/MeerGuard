#!/usr/bin/env python
import os.path
import pytz
import glob
import datetime
import pprint

import pyriseset as rs

import config
import utils
import clean_utils
import errors

EFF = rs.sites.load('effelsberg')
UTC_TZ = pytz.utc
BERLIN_TZ = pytz.timezone("Europe/Berlin")

HOURS_PER_MIN = 1/60.0


# Observing log fields:
#                (name,   from-string converter)
OBSLOG_FIELDS = (('localdate', rs.utils.parse_datestr), \
                 ('scannum', str), \
                 ('utcstart', rs.utils.parse_timestr), \
                 ('lststart', rs.utils.parse_timestr), \
                 ('name', str), \
                 ('az', float), \
                 ('alt', float), \
                 ('catalog_rastr', str), \
                 ('catalog_decstr', str))


RCVR_INFO = {'P217-3': 'rcvr:name=P217-3,rcvr:hand=-1,rcvr:basis=cir', \
             'S110-1': 'rcvr:name=S110-1,rcvr:hand=-1,rcvr:basis=cir', \
             'P200-3': 'rcvr:name=P200-3,rcvr:hand=-1,rcvr:basis=cir'}


def correct_header(arfn, obsinfo=None):
    """Correct header of asterix data in place.

        Input:
            arfn: The name of the input archive file.
            obsinfo: A dictionary of observing log information to use.
                (Default: search observing logs for matching entry)

        Output:
            corrfn: The name of the corrected file.
            corrstr: The parameter string of corrections used with psredit.
            note: A note about header correction
    """
    note = None
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
            raise errors.HeaderCorrectionError("Cannot determine receiver.")
    corrstr = "%s,be:name=asterix" % RCVR_INFO[rcvr]
    if arf['name'].endswith("_R"):
        corrstr += ",type=PolnCal"
    else:
        corrstr += ",type=Pulsar"
    if obsinfo is not None or arf['name'].endswith('_R') or arf['ra'].startswith('00:00:00'):
        # Correct coordinates
        try:
            if obsinfo is None:
                # Search for observing log entry
                obsinfo = get_obslog_entry(arf)
        except errors.HeaderCorrectionError as exc:
            note = exc.get_message() + "\n(Could not correct coordinates)" 
        else:
            ra_deg, decl_deg = EFF.get_skyposn(obsinfo['alt'], obsinfo['az'], \
                                                lst=obsinfo['lststart'])
            rastr = rs.utils.deg_to_hmsstr(ra_deg, decpnts=3)[0]
            decstr = rs.utils.deg_to_dmsstr(decl_deg, decpnts=2)[0]
            if decstr[0] not in ('-', '+'):
                decstr = "+" + decstr
            corrstr += ",coord=%s%s" % (rastr, decstr)
    else:
        note = "No reason to correct coords."
    # Correct the file using 'psredit'
    utils.execute(['psredit', '-e', 'corr', '-c', corrstr, arfn], \
                    stderr=open(os.devnull))
    # Assume the name of the corrected file
    corrfn = os.path.splitext(arfn)[0]+".corr"
    # Confirm that our assumed file name is accurate
    if not os.path.isfile(corrfn):
        raise errors.HeaderCorrectionError("The corrected file (%s) does not " \
                                "exist!" % corrfn)
    return corrfn, corrstr, note


def parse_obslog_line(line):
    """Given a line from a observing log, parse it.

        Input:
            line: A single line from an observing log.

        Output:
            info: A dictionary of information parsed from the
                observing log entry.
    """
    valstrs = line.split()
    if len(valstrs) < len(OBSLOG_FIELDS):
        # Not a valid observation log entry
        raise errors.FormatError("Observing log entry has bad format. " \
                        "Require at least %d fields." % len(OBSLOG_FIELDS))
    currinfo = {}
    for (key, caster), valstr in zip(OBSLOG_FIELDS, valstrs):
        currinfo[key] = caster(valstr)
    return currinfo


def get_obslog_entry(arf):
    """Given an archive file, find the entry in the observing log.

        Input:
            arf: ArchiveFile object.

        Output:
            obsinfo: A dictionary of observing information.
    """
    # Get date of observation
    obsdt_utc = rs.utils.mjd_to_datetime(arf['mjd'])
    obsdt_utc = UTC_TZ.localize(obsdt_utc)
    obsdt_local = obsdt_utc.astimezone(BERLIN_TZ)
    obsutc = obsdt_utc.time()
    obsdate = obsdt_local.date() # NOTE: discrepancy between timezones for time and date
                                 # This is a bad idea, but is done to be consistent with
                                 # what is used in the observation log files.
    obsutc_hours = obsutc.hour+(obsutc.minute+(obsutc.second)/60.0)/60.0

    # Get log file
    # NOTE: Date in file name is when the obslog was written out
    obslogfns = glob.glob(os.path.join(config.obslog_dir, "*.prot"))
    obslogfns.sort()
    
    tosearch = []
    for currfn in obslogfns:
        fndatetime = datetime.datetime.strptime(os.path.split(currfn)[-1], \
                                            '%y%m%d.prot')
        fndate = fndatetime.date()

        if fndate == obsdate:
            tosearch.append(currfn)
        elif fndate > obsdate:
            tosearch.append(currfn)
            break
    if not tosearch:
        raise errors.HeaderCorrectionError("Could not find an obslog file " \
                                    "for the obs date (%s)." % \
                                    obsdate.strftime("%Y-%b-%d"))
    
    logentries = []
    check = False
    for obslogfn in tosearch:
        with open(obslogfn, 'r') as obslog:
            for line in obslog:
                try:
                    currinfo = parse_obslog_line(line)
                except errors.FormatError:
                    # Not a valid observation log entry
                    continue
                if check:
                    if (obsdate >= previnfo['localdate']) and \
                            (obsdate <= currinfo['localdate']) and \
                            (obsutc_hours >= (previnfo['utcstart']-HOURS_PER_MIN)) and \
                            (obsutc_hours <= (currinfo['utcstart']+HOURS_PER_MIN)):
                        logentries.append(previnfo)
                # Check in next iteration if observation's source name matches
                # that of the current obslog entry
                check = (utils.get_prefname(currinfo['name']) == arf['name'])
                previnfo = currinfo
    if len(logentries) != 1:
        msg = "Bad number (%d) of entries " \
              "in obslogs (%s) with correct source name " \
              "within 120 s of observation (%s) start time (UTC: %s)" % \
                    (len(logentries), ", ".join(tosearch), arf.fn, obsutc)
        if len(logentries) > 1:
            msg += ":\n%s" % \
                    "\n".join([pprint.pformat(entry) for entry in logentries])
        raise errors.HeaderCorrectionError(msg)
    return logentries[0]


def main():
    print ""
    print "        correct.py"
    print "     Patrick  Lazarus"
    print ""
    
    if len(args.files):
        print "Number of input files: %d" % len(args.files)
    else:
        raise errors.InputError("No files to correct!")

    for fn in args.files:
        corrfn, corrstr, note = correct_header(fn)
        print "    Output corrected file: %s" % corrfn


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Correct header of Asterix " \
                                    "data files.")
    parser.add_argument('files', nargs='*', help="Files to correct.")
    args = parser.parse_args()
    main()
