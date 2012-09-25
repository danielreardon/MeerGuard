"""
This file contains various utility code that is used in various 
parts of the CoastGuard timing pipeline.

Patrick Lazarus, Nov. 10, 2011
"""
import os
import os.path
import warnings
import hashlib
import glob
import optparse
import sys
import subprocess
import types
import inspect

import numpy as np

import config
import errors
import colour

header_param_types = {'freq': float, \
                      'length': float, \
                      'bw': float, \
                      'mjd': float, \
                      'intmjd': int, \
                      'fracmjd': float, \
                      'backend': str, \
                      'rcvr': str, \
                      'telescop': str, \
                      'name': str, \
                      'nchan': int, \
                      'npol': int, \
                      'nbin': int, \
                      'nsub': int, \
                      'tbin': float, \
                      'period': float, \
                      'dm': float}

site_to_telescope = {'i': 'WSRT',
                     'wt': 'WSRT',
                     'wsrt': 'WSRT',
                     'westerbork': 'WSRT',
                     'g': 'Effelsberg', 
                     'ef': 'Effelsberg',
                     'eff': 'Effelsberg',
                     'effelsberg': 'Effelsberg',
                     '8': 'Jodrell',
                     'jb': 'Jodrell',
                     'jbo': 'Jodrell',
                     'jodrell bank': 'Jodrell',
                     'jodrell bank observatory': 'Jodrell',
                     'lovell': 'Jodrell',
                     'f': 'Nancay',
                     'nc': 'Nancay',
                     'ncy': 'Nancay',
                     'nancay': 'Nancay',
                     'sardinia': 'SRT',
                     'srt': 'SRT'}

# A cache for pulsar preferred names
prefname_cache = {}

def print_info(msg, level=1):
    """Print an informative message if the current verbosity is
        higher than the 'level' of this message.

        The message will be colourized as 'info'.

        Inputs:
            msg: The message to print.
            level: The verbosity level of the message.
                (Default: 1 - i.e. don't print unless verbosity is on.)

        Outputs:
            None
    """
    if config.verbosity >= level:
        if config.excessive_verbosity:
            # Get caller info
            fn, lineno, funcnm = inspect.stack()[1][1:4]
            colour.cprint("INFO (level: %d) [%s:%d - %s(...)]:" % 
                    (level, os.path.split(fn)[-1], lineno, funcnm), 'infohdr')
            msg = msg.replace('\n', '\n    ')
            colour.cprint("    %s" % msg, 'info')
        else:
            colour.cprint(msg, 'info')


def print_debug(msg, category):
    """Print a debugging message if the given debugging category
        is turned on.

        The message will be colourized as 'debug'.

        Inputs:
            msg: The message to print.
            category: The debugging category of the message.

        Outputs:
            None
    """
    if config.debug.is_on(category):
        if config.helpful_debugging:
            # Get caller info
            fn, lineno, funcnm = inspect.stack()[1][1:4]
            to_print = colour.cstring("DEBUG %s [%s:%d - %s(...)]:\n" % \
                        (category.upper(), os.path.split(fn)[-1], lineno, funcnm), \
                            'debughdr')
            msg = msg.replace('\n', '\n    ')
            to_print += colour.cstring("    %s" % msg, 'debug')
        else:
            to_print = colour.cstring(msg, 'debug')
        sys.stderr.write(to_print + '\n')
        sys.stderr.flush()


def get_md5sum(fn, block_size=16*8192):
    """Compute and return the MD5 sum for the given file.
        The file is read in blocks of 'block_size' bytes.

        Inputs:
            fn: The name of the file to get the md5 for.
            block_size: The number of bytes to read at a time.
                (Default: 16*8192)

        Output:
            md5: The hexidecimal string of the MD5 checksum.
    """
    f = open(fn, 'rb')
    md5 = hashlib.md5()
    block = f.read(block_size)
    while block:
        md5.update(block)
        block = f.read(block_size)
    f.close()
    return md5.hexdigest()


def get_githash():
    """Get the Coast Guard project's git hash.

        Inputs:
            None

        Output:
            githash: The githash
    """
    if is_gitrepo_dirty():
        warnings.warn("Git repository has uncommitted changes!", \
                        errors.CoastGuardWarning)
    codedir = os.path.split(__file__)[0]
    stdout, stderr = execute("git rev-parse HEAD", dir=codedir)
    githash = stdout.strip()
    return githash


def is_gitrepo_dirty():
    """Return True if the git repository has local changes.

        Inputs:
            None

        Output:
            is_dirty: True if git repository has local changes. False otherwise.
    """
    codedir = os.path.split(__file__)[0]
    try:
        stdout, stderr = execute("git diff --quiet", dir=codedir)
    except errors.SystemCallError:
        # Exit code is non-zero
        return True
    else:
        # Success error code (i.e. no differences)
        return False


def get_header_vals(fn, hdritems):
    """Get a set of header params from the given file.
        Returns a dictionary.

        Inputs:
            fn: The name of the file to get params for.
            hdritems: List of parameters (recognized by vap) to fetch.

        Output:
            params: A dictionary. The keys are values requested from 'vap'
                the values are the values reported by 'vap'.
    """
    hdrstr = ",".join(hdritems)
    if '=' in hdrstr:
        raise ValueError("'hdritems' passed to 'get_header_vals' " \
                         "should not perform and assignments!")
    cmd = "vap -n -c '%s' %s" % (hdrstr, fn)
    outstr, errstr = execute(cmd)
    outvals = outstr.split()[1:] # First value is filename (we don't need it)
    if errstr:
        raise errors.SystemCallError("The command: %s\nprinted to stderr:\n%s" % \
                                (cmd, errstr))
    elif len(outvals) != len(hdritems):
        raise errors.SystemCallError("The command: %s\nreturn the wrong " \
                            "number of values. (Was expecting %d, got %d.)" % \
                            (cmd, len(hdritems), len(outvals)))
    params = {}
    for key, val in zip(hdritems, outvals):
        if val == "INVALID":
            raise errors.SystemCallError("The vap header key '%s' " \
                                            "is invalid!" % key)
        elif val == "*" or val == "UNDEF":
            warnings.warn("The vap header key '%s' is not " \
                            "defined in this file (%s)" % (key, fn), \
                            errors.CoastGuardWarning)
            params[key] = None
        else:
            # Get param's type to cast value
            caster = header_param_types.get(key, str)
            params[key] = caster(val)
    return params


def get_archive_snr(fn):
    """Get the SNR of an archive using psrstat.
        Fully scrunch the archive first.

        Input:
            fn: The name of the archive.

        Output:
            snr: The signal-to-noise ratio of the fully scrunched archive.
    """
    cmd = "psrstat -Qq -j DTFp -c 'snr' %s" % fn
    outstr, errstr = execute(cmd)
    snr = float(outstr)
    return snr


def exclude_files(file_list, to_exclude):
    return [f for f in file_list if f not in to_exclude]


def execute(cmd, stdout=subprocess.PIPE, stderr=sys.stderr, dir=None): 
    """Execute the command 'cmd' after logging the command
        to STDOUT. Execute the command in the directory 'dir',
        which defaults to the current directory is not provided.

        Output standard output to 'stdout' and standard
        error to 'stderr'. Both are strings containing filenames.
        If values are None, the out/err streams are not recorded.
        By default stdout is subprocess.PIPE and stderr is sent 
        to sys.stderr.

        Returns (stdoutdata, stderrdata). These will both be None, 
        unless subprocess.PIPE is provided.
    """
    # Log command to stdout
    print_debug("'%s'" % cmd, 'syscalls')

    stdoutfile = False
    stderrfile = False
    if type(stdout) == types.StringType:
        stdout = open(stdout, 'w')
        stdoutfile = True
    if type(stderr) == types.StringType:
        stderr = open(stderr, 'w')
        stderrfile = True
    
    # Run (and time) the command. Check for errors.
    pipe = subprocess.Popen(cmd, shell=True, cwd=dir, \
                            stdout=stdout, stderr=stderr)
    (stdoutdata, stderrdata) = pipe.communicate()
    retcode = pipe.returncode 
    if retcode < 0:
        raise errors.SystemCallError("Execution of command (%s) terminated by signal (%s)!" % \
                                (cmd, -retcode))
    elif retcode > 0:
        raise errors.SystemCallError("Execution of command (%s) failed with status (%s)!" % \
                                (cmd, retcode))
    else:
        # Exit code is 0, which is "Success". Do nothing.
        pass
    
    # Close file objects, if any
    if stdoutfile:
        stdout.close()
    if stderrfile:
        stderr.close()

    return (stdoutdata, stderrdata)


def group_by_ctr_freq(infns):
    """Given a list of input files group them according to their
        centre frequencies.

        Input:
            infns: A list of input ArchiveFile objects.

        Outputs:
            grouped: A dict where each key is the centre frequency
                in MHz and where each value is a list of archive
                names with all the same centre frequency.

    """
    ctr_freqs = np.asarray([fn['freq'] for fn in infns])
    groups_dict = {}
    for ctr_freq in np.unique(ctr_freqs):
        # Collect the input files that are part of this sub-band
        indices = np.argwhere(ctr_freqs==ctr_freq)
        groups_dict[ctr_freq] = [infns[ii] for ii in indices]
    return groups_dict


def group_subints(infns):
    """Given a list of input subint files group them.
        This function assumes files with the same name exist in
        seperate subdirectories. Each subdirectory corresponds to
        a subband. Only subints from the same time that appear in
        all subbands are included in the groups, others are discarded.

        Input:
            infns: A list of input PSRCHIVE subints.

            grouped: A dict where each key is the centre frequency
                in MHz and where each value is a list of archive
                names with all the same centre frequency.
    """
    groups_dict = {}

    for infn in infns:
        dir, fn = os.path.split(infn.fn)
        groups_dict.setdefault(dir, set()).add(fn)

    # Determine intersection of all subbands
    intersection = set.intersection(*groups_dict.values())
    union = set.union(*groups_dict.values())
    
    print "Number of subints not present in all subbands: %s" % \
                len(union-intersection)

    subbands_dict = {}
    for infn in infns:
        dir, fn = os.path.split(infn.fn)
        if fn in intersection:
            subbands_dict.setdefault(float(infn['freq']), list()).append(infn)
    return subbands_dict


def enforce_file_consistency(infns, param, expected=None, discard=False, warn=False):
    """Check that all files have the same value for param
        in their header.

        Inputs:
            infns: The ArchiveFile objects that should have consistent header params.
            param: The header param to use when checking consistency.
            expected: The expected value. If None use the mode.
            discard: A boolean value. If True, files with param not matching
                the mode will be discarded. (Default: False)
            warn: A boolean value. If True, issue a warning if files are
                inconsistent. If False, raise an error. (Default: raise errors).

        Output:
            outfns: (optional - only if 'discard' is True) A list of consistent files.
    """
    params = [infn[param] for infn in infns]
    if expected is None:
        mode, count = get_mode(params)
    else:
        mode = expected
        count = len([p for p in params if p==expected])

    if discard:
        if count != len(infns):
            outfns = [fn for (fn, p) in zip(infns, params) if p==mode]
            if count != len(outfns):
                raise ValueError("Wrong number of files discarded! (%d != %d)" % \
                                    (len(infns)-count, len(infns)-len(outfns)))
            print_info("Check of header parameter '%s' has caused %d files " \
                        "with value != '%s' to be discarded" % \
                                (param, len(infns)-count, mode), 2)
            return outfns
        else:
            return infns
    else:
        if count != len(infns):
            msg = "There are %d files where the value of '%s' doesn't " \
                    "match other files (modal value: %s)" % \
                            (len(infns)-count, param, mode)
            if warn:
                warnings.warn(msg)
            else:
                raise errors.BadFile(msg)


def get_mode(vals):
    counts = {}
    for val in vals:
        count = counts.setdefault(val, 0)
        counts[val] = 1+count

    maxcount = max(counts.values())
    for key in counts.keys():
        if counts[key] == maxcount:
            return key, counts[key]


def group_subbands(infns):
    """Group subband files according to their base filename 
        (i.e. ignoring their extension).

        Input:
            infns: A list of input file names.

        Output:
            groups: A list of tuples, each being a group of subband
                files to combine.
    """
    get_basenm = lambda arf: os.path.splitext(os.path.split(arf.fn)[-1])[0]
    basenms = set([get_basenm(infn) for infn in infns])

    print_debug("Base names: %s" % ", ".join(basenms), 'grouping')

    groups = []
    for basenm in basenms:
        groups.append([arf for arf in infns if get_basenm(arf)==basenm])
    return groups


def get_files_from_glob(option, opt_str, value, parser):
    """optparse Callback function to turn a glob expression into
        a list of input files.

        Inputs:
            options: The Option instance.
            opt_str: The option provided on the command line.
            value: The value provided to the command line option.
            parser: The OptionParser.

        Outputs:
            None
    """
    glob_file_list = getattr(parser.values, option.dest)
    glob_file_list.extend(glob.glob(value))


def get_prefname(psrname):
    """Use 'psrcat' program to find the preferred name of the given pulsar.
        NOTE: B-names are preferred over J-names.

        Input:
            psrname: Name of the pulsar.

        Output:
            prefname: Preferred name of the pulsar.
    """
    global prefname_cache

    if psrname in prefname_cache:
        prefname = prefname_cache[psrname]
    else:
        cmd = "psrcat -nohead -nonumber -c 'PSRJ PSRB' -o short -null '' '%s'" % psrname
        stdout, stderr = execute(cmd)

        names = [line.strip().split() for line in stdout.split('\n') \
                        if line.strip() and not line.startswith("WARNING:")]
    
        if len(names) == 1:
            prefname = names[0][-1]
        elif len(names) == 0:
            prefname = psrname
            warnings.warn("Pulsar name '%s' cannot be found in psrcat. " \
                            "No preferred name available." % psrname, \
                            errors.CoastGuardWarning)
        else:
            raise errors.BadPulsarNameError("Pulsar name '%s' has a bad number of " \
                                    "matches (%d) in psrcat" % (psrname, len(names)))
        prefname_cache[psrname] = prefname
    return prefname


def get_outfn(fmtstr, arf):
    """Replace any format string codes using file header info
        to get the output file name.

        Inputs:
            fmtstr: The string to replace header info into.
            arf: An archive file object to get header info from using vap.

        Output:
            outfn: The output filename with (hopefully) all
                format string codes replace.
    """
    if '%' not in fmtstr:
        # No format string codes
        return fmtstr

    # Cast some values and compute others
    outfn = fmtstr % arf

    if '%' in outfn:
        raise errors.BadFile("Interpolated file name (%s) shouldn't " \
                                 "contain the character '%%'!" % outfn)
    return outfn


def correct_asterix_header(arfn):
    """Effelsberg Asterix data doesn't have backend and receiver
        information correctly written into archive headers. It is
        necessary to add the information. This function guesses
        the receiver used.

        NOTES:
            - An error is raised if the receiver is uncertain.
            - The corrected archive is written with the extension 'rcvr'.

        Input:
            arfn: An ArchiveFile object.

        Output:
            outarfn: The corrected ArchiveFile object.
    """
    codedir = os.path.join(os.getcwd(), os.path.split(__file__)[0])
    cmd = "%s/correct_archives.sh %s | bash" % (codedir, arfn.fn)
    stdout, stderr = execute(cmd)
    if stdout.strip().endswith("written to disk"):
        outarf = ArchiveFile(stdout.split()[0])
    else:
        raise errors.HeaderCorrectionError("Correction of Asterix archive (%s) " \
                                    "header failed: \n%s" % (arfn.fn, stderr))
    return outarf


def mjd_to_date(mjds):
    """Convert Julian Day (JD) to a date.

        Input:
            mjds: Array of Modified Julian days

        Outputs:
            years: Array of years.
            months: Array of months.
            days: Array of (fractional) days.

        (Follow Jean Meeus' Astronomical Algorithms, 2nd Ed., Ch. 7)
    """
    JD = np.atleast_1d(mjds)+2400000.5

    if np.any(JD<0.0):
        raise ValueError("This function does not apply for JD < 0.")

    JD += 0.5

    # Z is integer part of JD
    Z = np.floor(JD)
    # F is fractional part of JD
    F = np.mod(JD, 1)

    A = np.copy(Z)
    alpha = np.floor((Z-1867216.25)/36524.25)
    A[Z>=2299161] = Z + 1 + alpha - np.floor(0.25*alpha)

    B = A + 1524
    C = np.floor((B-122.1)/365.25)
    D = np.floor(365.25*C)
    E = np.floor((B-D)/30.6001)

    day = B - D - np.floor(30.6001*E) + F
    month = E - 1
    month[(E==14.0) | (E==15.0)] = E - 13
    year = C - 4716
    year[(month==1.0) | (month==2.0)] = C - 4715

    return (year.astype('int').squeeze(), month.astype('int').squeeze(), \
                day.squeeze())


class ArchiveFile(object):
    def __init__(self, fn):
        self.fn = os.path.abspath(fn)
        self.ar = None
        if not os.path.isfile(self.fn):
            raise errors.BadFile("Archive file could not be found (%s)!" % \
                                    self.fn) 
        
        self.hdr = get_header_vals(self.fn, ['freq', 'length', 'bw', 'mjd', 
                                            'intmjd', 'fracmjd', 'backend', 
                                            'rcvr', 'telescop', 'name', 
                                            'nchan', 'asite', 'period', 'dm'])
        try:
            self.hdr['name'] = get_prefname(self.hdr['name']) # Use preferred name
        except errors.BadPulsarNameError:
            warnings.warn("No preferred name found in 'psrcat'. " \
                            "Will continue using '%s'" % self.hdr['name'], \
                            errors.CoastGuardWarning)
        self.hdr['secs'] = int(self.hdr['fracmjd']*24*3600+0.5) # Add 0.5 so we actually round
        self.hdr['yyyymmdd'] = "%04d%02d%02d" % mjd_to_date(self.hdr['mjd'])
        self.hdr['pms'] = self.hdr['period']*1000.0
    
    def __getitem__(self, key):
        if key not in self.hdr:
            if key == 'snr':
                self.hdr['snr'] = get_archive_snr(self.fn)
            else:
                self.hdr.update(get_header_vals(self.fn, [key]))
        return self.hdr[key]
    
    def get_archive(self):
        if self.ar is None:
            import psrchive
            self.ar = psrchive.Archive_load(self.fn)
        return self.ar


class DefaultOptions(optparse.OptionParser):
    def __init__(self, *args, **kwargs):
        optparse.OptionParser.__init__(self, *args, **kwargs)

    def parse_args(self, *args, **kwargs):
        # Add debug group just before parsing so it is the last set of
        # options displayed in help text
        self.add_standard_group()
        self.add_debug_group()
        return optparse.OptionParser.parse_args(self, *args, **kwargs)

    def add_standard_group(self):
        group = optparse.OptionGroup(self, "Standard Options", \
                    "The following options are used to set standard " \
                    "behaviour shared by multiple modules.")
        group.add_option('-v', '--more-verbose', action='callback', \
                          callback=self.increment_config, \
                          callback_args=('verbosity',), \
                          help="Turn up verbosity level. " \
                                "(Default: level=%d)" % \
                                config.verbosity)
        group.add_option('--less-verbose', action='callback', \
                          callback=self.decrement_config, \
                          callback_args=('verbosity',), \
                          help="Turn down verbosity level. " \
                                "(Default: level=%d)" % \
                                config.verbosity)
        group.add_option('-c', '--toggle-colour', action='callback', \
                          callback=self.toggle_config, \
                          callback_args=('colour',), \
                          help="Toggle colourised output. " \
                                "(Default: colours are %s)" % \
                                ((config.colour and "on") or "off"))
        group.add_option('--toggle-exverb', action='callback', \
                          callback=self.toggle_config, \
                          callback_args=('excessive_verbosity',), \
                          help="Toggle excessive verbosity. " \
                                "(Default: excessive verbosity is %s)" % \
                                ((config.excessive_verbosity and "on") or "off"))
        self.add_option_group(group)

    def add_debug_group(self):
        group = optparse.OptionGroup(self, "Debug Options", \
                    "The following options turn on various debugging " \
                    "statements. Multiple debugging options can be " \
                    "provided.")
        group.add_option('--toggle-helpful-debug', action='callback', \
                          callback=self.toggle_config, \
                          callback_args=('helpful_debugging',), \
                          help="Toggle helpful debugging. " \
                                "(Default: helpful debugging is %s)" % \
                                ((config.helpful_debugging and "on") or "off"))
        group.add_option('-d', '--debug', action='callback', \
                          callback=self.debugall_callback, \
                          help="Turn on all debugging modes. (Same as --debug-all).")
        group.add_option('--debug-all', action='callback', \
                          callback=self.debugall_callback, \
                          help="Turn on all debugging modes. (Same as -d/--debug).")
        group.add_option('--set-debug-mode', action='callback', \
                          type='str', callback=self.debug_callback, \
                          help="Turn on specified debugging mode. Use --list-debug-modes " \
                            "to see the list of available modes and descriptions. " \
                            "(Default: all debugging modes are off)")
        group.add_option('--list-debug-modes', action='callback', \
                          callback=self.list_debug, \
                          help="List available debugging modes and descriptions, " \
                            "and then exit.")
        self.add_option_group(group)

    def increment_config(self, option, opt_str, value, parser, param):
        val = getattr(config, param)
        setattr(config, param, val+1)

    def decrement_config(self, option, opt_str, value, parser, param):
        val = getattr(config, param)
        setattr(config, param, val-1)

    def toggle_config(self, option, opt_str, value, parser, param):
        val = getattr(config, param)
        setattr(config, param, not val)

    def override_config(self, option, opt_str, value, parser):
        config.cfg.set_override_config(option.dest, value)

    def set_override_config(self, option, opt_str, value, parser):
        config.cfg.set_override_config(option.dest, True)

    def unset_override_config(self, option, opt_str, value, parser):
        config.cfg.set_override_config(option.dest, False)
    
    def debug_callback(self, option, opt_str, value, parser):
        config.debug.set_mode_on(value)

    def debugall_callback(self, option, opt_str, value, parser):
        config.debug.set_allmodes_on()

    def list_debug(self, options, opt_str, value, parser):
        print "Available debugging modes:"
        for name, desc in config.debug.modes:
            print "    %s: %s" % (name, desc)
        sys.exit(1)
