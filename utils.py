"""
This file contains various utility code that is used in various 
parts of the CoastGuard timing pipeline.

Patrick Lazarus, Nov. 10, 2011
"""

import optparse
import sys
import subprocess
import types

import debug
import errors

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
