"""
This file contains various utility code that is used in various 
parts of the CoastGuard timing pipeline.

Patrick Lazarus, Nov. 10, 2011
"""
import glob
import optparse
import sys
import subprocess
import types

import numpy as np

import debug
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


def parse_psrfits_header(fn, hdritems):
    """Get a set of header params from the given file.
        Returns a dictionary.

        Inputs:
            fn: The name of the file to get params for.
            hdritems: List of parameter names to fetch.

        Output:
            params: A dictionary. The keys are values requested from 'psredit'
                the values are the values reported by 'psredit'.
    """
    hdrstr = ",".join(hdritems)
    if '=' in hdrstr:
        raise ValueError("'hdritems' passed to 'parse_psrfits_header' " \
                         "should not perform and assignments!")
    cmd = "psredit -q -Q -c '%s' %s" % (hdrstr, fn)
    outstr, errstr = execute(cmd)
    outvals = outstr.split()
    if errstr:
        raise errors.SystemCallError("The command: %s\nprinted to stderr:\n%s" % \
                                (cmd, errstr))
    elif len(outvals) != len(hdritems):
        raise errors.SystemCallError("The command: %s\nreturn the wrong " \
                            "number of values. (Was expecting %d, got %d.)" % \
                            (cmd, len(hdritems), len(outvals)))
    params = {}
    for key, val in zip(hdritems, outstr.split()):
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
    if debug.SYSCALLS:
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


def get_header_param(infn, param):
    """Given a PSRCHIVE file find and return a header value.

        This function calls PSRCHIVE's 'vap' and parses the output.

        Inputs:
            infn: The file for which the centre frequency will be found.
            param: The parameter name to grab from the achive's header.

        Output:
            val: The value corresponding to the parameter provided.
    """
    out, err = execute("vap -n -c %s %s" % (param, infn))

    # Output format of 'vap -n -c <param> <filename>' is: 
    #   <filename> <value>
    val = float(out.split()[1])
    return val


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
    ctr_freqs = np.asarray([get_header_param(fn, 'freq') for fn in infns])
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


class DefaultOptions(optparse.OptionParser):
    def __init__(self, *args, **kwargs):
        optparse.OptionParser.__init__(self, *args, **kwargs)
       
    def parse_args(self, *args, **kwargs):
        # Add debug group just before parsing so it is the last set of
        # options displayed in help text
        self.add_debug_group()
        return optparse.OptionParser.parse_args(self, *args, **kwargs)

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
        for m, desc in debug.modes:
            group.add_option('--debug-%s' % m.lower(), action='callback', \
                              callback=self.debug_callback, \
                              callback_args=(m,), \
                              help=desc)
        self.add_option_group(group)

    def debug_callback(self, option, opt_str, value, parser, mode):
        debug.set_mode_on(mode)

    def debugall_callback(self, option, opt_str, value, parser):
        debug.set_allmodes_on()
