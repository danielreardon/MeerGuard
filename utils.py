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

import numpy as np

import config
import errors

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
        warnings.warn("Git repository has uncommitted changes!")
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
        params[key] = val
    return params


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
    if config.debug.SYSCALLS:
        sys.stdout.write("\n'"+cmd+"'\n")
        sys.stdout.flush()

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
            infns: A list of input PSRCHIVE archive file names.

        Outputs:
            grouped: A dict where each key is the centre frequency
                in MHz and where each value is a list of archive
                names with all the same centre frequency.

    """
    get_freq = lambda fn: float(get_header_vals(fn, ['freq'])['freq'])
    ctr_freqs = np.asarray([get_freq(fn) for fn in infns])
    groups_dict = {}
    for ctr_freq in np.unique(ctr_freqs):
        # Collect the input files that are part of this sub-band
        indices = np.argwhere(ctr_freqs==ctr_freq)
        groups_dict[ctr_freq] = [infns[ii] for ii in indices]
    return groups_dict


def apply_to_archives(infns, funcs, arglists, kwargdicts):
    """Apply a function to each input file in 'infns' with the
        args and kwargs provided.

        Inputs:
            infns: A list of input PSRCHIVE archive file names.
            funcs: A list of functions to apply to each of the input 
                archives. These functions should return the name of 
                the processed archive. Functions are applied in order.
            arglists: A list of tuples containing additional arguments
                for each function.
            kwargdicts: A list of dicts containing additional keyword
                arguments for each function.

        Output:
            outfns: A list of output filenames.
        
        NOTE: 'func' is called in the following way:
            <outfn> = func(<infn>, *args, **kwargs)
    """
    # Extend preargs and prekwargs to make sure they have at least the 
    # same length as the list of functions.
    arglists.extend([[]]*len(funcs))
    kwargdicts.extend([{}]*len(funcs))
    outfns = []
    for infn in infns:
        for func, args, kwargs in zip(funcs, arglists, kwargdicts):
            outfns.append(func(infn, *args, **kwargs))
    return outfns
    

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


def get_outfn(fmtstr, arfn):
    """Replace any format string codes using file header info
        to get the output file name.

        Inputs:
            fmtstr: The string to replace header info into.
            arfn: An archive file name to get header info from using vap.

        Output:
            outfn: The output filename with (hopefully) all
                format string codes replace.
    """
    if '%' not in fmtstr:
        # No format string codes
        return fmtstr

    # Get header information
    hdr = get_header_vals(arfn, ['freq', 'telescop', 'site', \
                                 'rcvr', 'backend', 'name', \
                                 'mjd', 'intmjd', 'fracmjd'])
    # Cast some values and compute others
    hdr['mjd'] = float(hdr['mjd'])
    hdr['freq'] = float(hdr['freq'])
    hdr['intmjd'] = float(hdr['intmjd'])
    hdr['fracmjd'] = float(hdr['fracmjd'])
    hdr['secs'] = int(hdr['fracmjd']*24*3600)
    hdr['yyyymmdd'] = "%04d%02d%02d" % mjd_to_date(hdr['mjd'])
    outfn = fmtstr % hdr

    if '%' in outfn:
        raise errors.BadFileName("Interpolated file name (%s) shouldn't " \
                                 "contain the character '%%'!" % outfn)
    return outfn

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
                          callback=self.more_verbosity, \
                          help="Turn up verbosity level.")
        group.add_option('-c', '--toggle-colour', action='callback', \
                          callback=self.toggle_colours, \
                          help="Toggle colourised output.")
        self.add_option_group(group)

    def add_debug_group(self):
        group = optparse.OptionGroup(self, "Debug Options", \
                    "The following options turn on various debugging " \
                    "statements. Multiple debugging options can be " \
                    "provided.")
        group.add_option('-d', '--debug', action='callback', \
                          callback=self.debugall_callback, \
                          help="Turn on all debugging modes. (Same as --debug-all).")
        group.add_option('--debug-all', action='callback', \
                          callback=self.debugall_callback, \
                          help="Turn on all debugging modes. (Same as -d/--debug).")
        for m, desc in config.debug.modes:
            group.add_option('--debug-%s' % m.lower(), action='callback', \
                              callback=self.debug_callback, \
                              callback_args=(m,), \
                              help=desc)
        self.add_option_group(group)

    def more_verbosity(self, option, opt_str, value, parser):
        config.verbosity += 1

    def toggle_colours(self, option, opt_str, value, parser):
        config.colour = not config.colour

    def debug_callback(self, option, opt_str, value, parser, mode):
        config.debug.set_mode_on(mode)

    def debugall_callback(self, option, opt_str, value, parser):
        config.debug.set_allmodes_on()
