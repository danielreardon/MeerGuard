#!/usr/bin/env python
import os.path
import pytz
import glob
import datetime
import pprint
import shutil

import numpy as np

import pyriseset as rs

import sqlalchemy as sa

from coast_guard import config
from coast_guard import utils
from coast_guard import clean_utils
from coast_guard import errors
from coast_guard import database

EFF = rs.sites.load('effelsberg')
UTC_TZ = pytz.utc
BERLIN_TZ = pytz.timezone("Europe/Berlin")

HOURS_PER_MIN = 1/60.0

# Band-to-receiver mapping for non-L-band receivers
# (There is more than one L-band receiver, so we can't
#  simply determine which was used from the observing band)
BAND_TO_RECEIVER = {'Cband': 'S60-2',
                    'Xband': 'S36-5',
                    'Sband': 'S110-1'}

# Observing log fields:
#                (name,   from-string converter)
OBSLOG_FIELDS = (('localdate', rs.utils.parse_datestr),
                 ('scannum', str),
                 ('utcstart', rs.utils.parse_timestr),
                 ('lststart', rs.utils.parse_timestr),
                 ('name', str),
                 ('az', float),
                 ('alt', float),
                 ('catalog_rastr', str),
                 ('catalog_decstr', str))


RCVR_INFO = {'P217-3': 'rcvr:name=P217-3,rcvr:hand=-1,rcvr:basis=cir',
             'S110-1': 'rcvr:name=S110-1,rcvr:hand=-1,rcvr:basis=cir',
             'P200-3': 'rcvr:name=P200-3,rcvr:hand=-1,rcvr:basis=cir',
             'S60-2':  'rcvr:name=S60-2,rcvr:hand=-1,rcvr:basis=cir', # Not sure about handedness
             'S36-5':  'rcvr:name=S36-5,rcvr:hand=-1,rcvr:basis=cir'} # Not sure about handedness


def read_receiver_file(fn='lband_receivers.txt'):
    rcvrs = {}
    with open(fn, 'r') as ff:
        for orig in ff:
            line = orig.partition('#')[0].strip()
            if not line:
                continue
            split = line.split()
            if len(split) < 2:
                raise ValueError("Bad number of elements in line (need at least 2):\n%s" % orig)
            rcvrs[int(split[0])] = split[1]
    return rcvrs


def get_coordinates(arf, obsinfo=None, tolerant=False):
    """Given an archive file try to compute the telescope coordinates
        from the observation log.

        Inputs:
            arfn: The name of the input archive file.
            obsinfo: A dictionary of observing log information to use.
            tolerant: Be tolerant with name matching. 
                This is important for flux-cal observations.
                (Default: False)
        
        Outputs:
            rastr:  RA in hms format
            decstr: Dec in dms format
    """
    ra_deg, decl_deg = EFF.get_skyposn(obsinfo['alt'], obsinfo['az']+180, \
                                       lst=obsinfo['lststart'])
    rastr = rs.utils.deg_to_hmsstr(ra_deg, decpnts=3)[0]
    decstr = rs.utils.deg_to_dmsstr(decl_deg, decpnts=2)[0]
    if decstr[0] not in ('-', '+'):
        decstr = "+" + decstr
    return rastr, decstr


def determine_receiver(arf, use_weights=True):
    """Given an ArchiveFile object determin the Effelsberg
        reciver name.

        Input:
            arf: An ArchiveFile object.
            use_weights: If True, use weights as-is. If not reset weights to be uniform.
                (Default: use weights as-is)

        Output:
            rcvr: The name of the receiver.
    """
    if arf['band'] == 'Lband':
        # L-band
        ar = arf.get_archive()
        if not use_weights:
            ar.uniform_weight(1.0)
        nchan = ar.get_nchan()
        # Scrunch
        ar.pscrunch()
        ar.tscrunch()
        # Get the relevant data
        chnwts = clean_utils.get_chan_weights(ar).astype(bool)
        stddevs = ar.get_data().squeeze().std(axis=1)
        freqs = clean_utils.get_frequencies(ar)
        # Outside P200-3 receiver's response
        iout = (freqs < 1285.0) | (freqs > 1437.0)
        if np.sum(iout) < 5:
            raise errors.HeaderCorrectionError("Cannot determine L-band "
                                               "receiver. Too few channels "
                                               "(%d) outside P200-3's response." %
                                               np.sum(iout))
        outside = stddevs[iout][chnwts[iout]].mean()
        inside = stddevs[~iout][chnwts[~iout]].mean()

        if inside/outside > 5:
            # There does not appear to be signal outside the P200-3 receiver's response:
            # single-pixel receiver
            rcvr = 'P200-3'
        elif inside/outside < 2:
            # There appears to be signal outside the P200-3 receiver's response:
            # 7-beam receiver
            rcvr = 'P217-3'
        else:
            raise errors.HeaderCorrectionError("Cannot determine receiver. " \
                                               "(Outside: %d chan, avg stddev=%g; "
                                               "Inside: %d chan, avg stddev=%g)" %
                                               (np.sum(iout), outside, np.sum(~iout),
                                                inside))
    elif arf['band'] in BAND_TO_RECEIVER.keys():
        rcvr = BAND_TO_RECEIVER[arf['band']] 
    else:
        raise errors.HeaderCorrectionError("Not set up to correct headers for "
                                           "%s observations." % arf['band'])
    return rcvr


def get_correction_string(arfn, obsinfo=None, backend='asterix', 
                          receiver=None, fixcoords=False):
    """Get psredit command string that will correct the file header.

        Input:
            arfn: The name of the input archive file.
            obsinfo: A dictionary of observing log information to use.
                (Default: search observing logs for matching entry)
            backend: Override backend name with this value.
                (Default: asterix)
            receiver: Override receiver name with this value.
                (Default: Determine receiver automatically)
            fixcoords: Force fixing of coordinates.
                (Default: Don't bother if they seem to be correct)

        Output:
            corrstr: The parameter string of corrections used with psredit.
            note: A note about header correction
    """
    note = ""
    # Load archive
    arf = utils.ArchiveFile(arfn)
    if receiver is None:
        rcvr = determine_receiver(arf)
    elif receiver in ('P217-3', 'P200-3', 'S110-1', 'S60-2', 'S36-5'):
        rcvr = receiver
    else:
        raise ValueError("Receiver provided (%s) is not recognized." % receiver)

    if arf['rcvr'] != rcvr:
        note += "Receiver is wrong (%s) setting to '%s'. " % \
                (arf['rcvr'], rcvr)
    corrstr = "%s,be:name=%s" % (RCVR_INFO[rcvr], backend)
    if fixcoords or (obsinfo is not None) or arf['name'].endswith('_R') or \
                        arf['ra'].startswith('00:00:00'):
        try:
            if obsinfo is None:
                # Search for observing log entry
                obsinfo = get_obslog_entry(arf, tolerant=True)
                utils.print_debug("Information from matching observing log line:\n%s" %
                                  pprint.pformat(obsinfo), 'correct')
            rastr, decstr = get_coordinates(arf, obsinfo)
        except errors.HeaderCorrectionError as exc:
            note += exc.get_message() + "\n(Could not correct coordinates)"
            raise
        else:
            corrstr += ",coord=%s%s" % (rastr, decstr)
    else:
        note += "No reason to correct coords."

    if obsinfo is not None:
        name = obsinfo['name']
        corrstr += ",name=%s" % obsinfo['name']
    else:
        name = arf['name']

    if name.endswith("_R"):
        # Calibration diode was fired.
        # Observation could be pol-cal scan or flux-cal scan
        if any([name.startswith(fluxcal) for fluxcal
                in utils.read_fluxcal_names(config.fluxcal_cfg)]):
            # Flux calibrator
            if name.endswith("_S_R") or name.endswith("_N_R"):
                corrstr += ",type=FluxCal-Off"
            elif name.endswith("_O_R"): 
                corrstr += ",type=FluxCal-On"
        else:
            # Polarization calibrator
            corrstr += ",type=PolnCal"
    else:
        corrstr += ",type=Pulsar"
    return corrstr, note


def correct_header(arfn, obsinfo=None, outfn=None, 
                   backend='asterix', receiver=None):
    """Correct header of asterix data in place.

        Input:
            arfn: The name of the input archive file.
            obsinfo: A dictionary of observing log information to use.
                (Default: search observing logs for matching entry)
            outfn: Output file name.
                (Default: same as input file name, but with .corr extension)
            backend: Override backend name with this value.
                (Default: asterix)
            receiver: Override receiver name with this value.
                (Default: Determine receiver automatically)

        Output:
            corrfn: The name of the corrected file.
            corrstr: The parameter string of corrections used with psredit.
            note: A note about header correction
    """
    corrstr, note = get_correction_string(arfn, obsinfo, 
                                          receiver=receiver, 
                                          backend=backend)
    # Correct the file using 'psredit'
    utils.execute(['psredit', '-e', 'corr', '-c', corrstr, arfn],
                  stderr=open(os.devnull))
    # Assume the name of the corrected file
    corrfn = os.path.splitext(arfn)[0]+".corr"
    # Confirm that our assumed file name is accurate
    if not os.path.isfile(corrfn):
        raise errors.HeaderCorrectionError("The corrected file (%s) does not " \
                                           "exist!" % corrfn)
    # Rename output file
    if outfn is not None:
        arf = utils.ArchiveFile(corrfn)
        fn = outfn % arf
        shutil.move(corrfn, fn)
        corrfn = fn
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


def get_obslog_entry(arf, tolerant=False):
    """Given an archive file, find the entry in the observing log.

        Inputs:
            arf: ArchiveFile object.
            tolerant: Be tolerant with name matching. 
                This is important for flux-cal observations.
                (Default: False)

        Output:
            obsinfo: A dictionary of observing information.
    """
    obsdt_utc, names = __prep_obslog_search(arf, tolerant)

    logentries = __obslog_db_match(obsdt_utc, names)
    if not logentries:
        utils.print_info('No matches found in obslog DB. Searching text files.', 1)
        logentries = __obslog_file_match(obsdt_utc, names)

    if len(logentries) != 1:
        msg = "Bad number (%d) of entries " \
              "in obslog with correct source name (%s) " \
              "close to observation (%s) start time (UTC: %s)" % \
                    (len(logentries), arf['name'], arf.fn, obsdt_utc.strftime('%c'))
        if len(logentries) > 1:
            msg += ":\n%s" % \
                    "\n".join([pprint.pformat(entry) for entry in logentries])
        raise errors.HeaderCorrectionError(msg)
    return logentries[0]


def __prep_obslog_search(arf, tolerant):
    """Prepare some observation info for searching
        for observing log entries.

        Inputs:
            arf: ArchiveFile object.
            tolerant: Be tolerant with name matching. 
                This is important for flux-cal observations.
                (Default: False)

        Outputs:
            obsdt_utc: The UTC datetime at the start of the observation
            names: Object names to match
    """
    # Use tolerant name matching
    # Be sure to use the original name recorded in the header
    names = (arf['origname'],)
    if tolerant and arf['origname'].endswith("_R") and \
            not (("_O" in arf['origname']) or ("_N" in arf['origname']) or ("_S" in arf['origname'])):
        base = arf['origname'][:-2]
        if base in utils.read_fluxcal_names():
            # Be tolerant with name matching
            names += (base+"_N_R", base+"_O_R", base+"_S_R")
        else:
            names += (base.lstrip('BJ')+'_R',)
    utils.print_debug("Will check for the following name for obs-log " \
                      "matching: %s" % ", ".join(names), 'correct')

    # Get date of observation
    obsdt_utc = rs.utils.mjd_to_datetime(arf['mjd'])
    obsdt_utc = UTC_TZ.localize(obsdt_utc)
    return obsdt_utc, names


def __obslog_db_match(obsdt_utc, names):
    """Find entries in observing log database matching the given information.

        Inputs:
            obsdt_utc: The UTC datetime at the start of the observation
            names: Object names to match

        Outputs:
            logentries: Matching log entries.
    """
    db = database.Database('obslog')

    utcstart_col = sa.cast(db.obsinfo.c.obstimestamp, sa.DateTime)

    # Find entries within +- 1 day of observation start time
    start = obsdt_utc - datetime.timedelta(days=1)
    end = obsdt_utc + datetime.timedelta(days=1)
    with db.transaction() as conn:
        select = db.select([db.obsinfo.c.object.label('name'), 
                            (db.obsinfo.c.lst/3600.0).label('lststart'), 
                            utcstart_col.label('utcstart'), 
                            db.obsinfo.c.azim.label('az'), 
                            db.obsinfo.c.elev.label('alt'),
                            db.obsinfo.c.scan.label('scannum'),
                            db.obsinfo.c.lon, 
                            db.obsinfo.c.lat]).\
                    where(db.obsinfo.c.object.in_(names) & (utcstart_col >= start) & 
                          (utcstart_col <= end))
        result = conn.execute(select)
        rows = result.fetchall()
        result.close()

    utils.print_debug("Found %d matching obslog DB entries " 
                      "(name: %s; UTC: %s)" % 
                      (len(rows), ", ".join(names), obsdt_utc.strftime("%c")), 
                      'correct')
    logentries = []
    for row in rows:
        # refine matching based on time
        utils.print_debug("%s" % row, 'correct')
        twentyfivesec = datetime.timedelta(seconds=25)
        logdt_utc = UTC_TZ.localize(row['utcstart'])
        if (logdt_utc-twentyfivesec) <= obsdt_utc <= (logdt_utc+twentyfivesec):
            # Compute a few values to be consistent with obslog file parsing
            utc_hrs = row['utcstart'].hour + (row['utcstart'].minute + 
                                               (row['utcstart'].second + 
                                                row['utcstart'].microsecond*1e-6)/60.0)/60.0 

            logdt_local = logdt_utc.astimezone(BERLIN_TZ)
            localdate = logdt_local.date()

            entry = dict(row)
            entry['scannum'] = str(row['scannum'])
            entry['utcstart'] = utc_hrs
            entry['utc'] = row['utcstart'].strftime('%c')
            entry['localdate'] = localdate
            entry['catalog_rastr'] = rs.utils.deg_to_hmsstr(row['lon'], decpnts=3, style='units')[0]
            entry['catalog_decstr'] = rs.utils.deg_to_dmsstr(row['lat'], decpnts=3, style='units')[0]

            logentries.append(entry)
    return logentries


def __obslog_file_match(obsdt_utc, names):
    """Find entries in observing log files matching the given information.

        Inputs:
            obsdt_utc: The UTC datetime at the start of the observation
            names: Object names to match

        Outputs:
            logentries: Matching log entries.
    """
    obsdt_local = obsdt_utc.astimezone(BERLIN_TZ)
    obsutc = obsdt_utc.time()
    obsdate = obsdt_local.date() # NOTE: discrepancy between timezones for time and date
                                 # This is a bad idea, but is done to be consistent with
                                 # what is used in the observation log files.
    obsutc_hours = obsutc.hour+(obsutc.minute+(obsutc.second)/60.0)/60.0
    obsutc_hhmm = obsutc.hour+(obsutc.minute)/60.0
    if obsutc.second > 30:
        delta = HOURS_PER_MIN
    else:
        delta = -HOURS_PER_MIN

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
                                           "for the obs date (%s)." %
                                           obsdate.strftime("%Y-%b-%d"))
    
    utils.print_debug('Searching obs log files:\n    %s' % 
                      "\n    ".join(tosearch), 'correct')
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
                    utils.print_debug("Checking obslog line:\n%s\n"
                                      "Obs date: %s, obs log date: %s, next date: %s\n"
                                      "Obs UTC: %f, obs log UTC: %f, next UTC: %f\n" % 
                                      (prevline, obsdate, previnfo['localdate'], 
                                       currinfo['localdate'], obsutc_hhmm,
                                       previnfo['utcstart'], currinfo['utcstart']), 
                                      'correct')
                    if (obsdate >= previnfo['localdate']) and \
                            (obsdate <= currinfo['localdate']) and \
                            (is_close(obsutc_hhmm, previnfo['utcstart'], 1) or \
                             is_close(obsutc_hhmm+delta, previnfo['utcstart'], 1)):
                             #and (obsutc_hhmm <= currinfo['utcstart']): # Not needed anymore?
                        utils.print_debug("Matching observing log line:\n%s" % 
                                          prevline, 'correct')
                        logentries.append(previnfo)
                # Check in next iteration if observation's source name matches
                # that of the current obslog entry
                check = (utils.get_prefname(currinfo['name']) in names)
                prevline = line
                previnfo = currinfo
    utils.print_debug("Found %d potentially matching obs-log entries" % len(logentries), 'correct')
    return logentries


def is_close(hr1, hr2, delta=1):
    """Check it two times, in hours, are close.

        Inputs:
            hr1: A time in hours.
            hr2: Another time in hours.
            delta: The maximum difference (in seconds) for the two input
                times to be considered close. (Default: 1 s)

        Output:
            close: True if the times fall within 'delta' of each other.
    """
    return abs(hr1-hr2) < (delta/3600.0)


def main():
    print ""
    print "        correct.py"
    print "     Patrick  Lazarus"
    print ""
    
    if len(args.files):
        print "Number of input files: %d" % len(args.files)
    else:
        raise errors.InputError("No files to correct!")

    if args.obslog_line is not None:
        obsinfo = parse_obslog_line(args.obslog_line)
    else:
        obsinfo = None

    for fn in args.files:
        corrfn, corrstr, note = correct_header(fn, obsinfo=obsinfo,
                                               outfn=args.outfn,
                                               backend=args.backend_name)
        print "    Output corrected file: %s" % corrfn
        print "        Notes: %s" % note


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Correct header of Asterix " \
                                    "data files.")
    parser.add_argument('files', nargs='*', help="Files to correct.")
    parser.add_argument('--obslog-line', dest='obslog_line', type=str, \
                        help="Line from observing log to use. " \
                            "(Default: search observing logs for " \
                            "the appropriate line.)")
    parser.add_argument('-b', '--backend-name', dest='backend_name', type=str, \
                        help="Name of backend to use. (Default: 'asterix')", \
                        default='asterix')
    parser.add_argument('-o', '--outname', dest='outfn', type=str, \
                        help="The output (reduced) file's name. " \
                            "(Default: '%s.corr')" % \
                                config.outfn_template.replace("%", "%%"), \
                        default=config.outfn_template+".corr")
    args = parser.parse_args()
    main()
