"""
This module maintains a list of debug options for the CoastGuard
timing pipline, and their current settings. This way the listing 
of debug modes currently enabled can be shared between different 
modules of a single process.

Patrick Lazarus, Nov. 11, 2011
"""

modes = [('syscalls', 'Print commands being executed as system calls.'), \
         ('intermediate', 'Do not remove intermediate files ' \
                            'when reducing data.'), \
         ('config', 'Print information when configuration files are loaded.'), \
         ('clean', 'Print debugging information relevant to ' \
                            'cleaning algorithms. '), \
         ('combine', 'Print debugging information relevant to ' \
                            'archive combining.'), \
         ('grouping', 'Print debugging information relevant to grouping.'), \
         ('reduce', 'Print debugging information relevant to automated data reduction.'), \
         ('queries', 'Print database queries being executed.'), \
         ('database', "Display DB connection/transaction info."), \
         ('correct', "Print debugging information about correcting header information "
                     "using Efflesberg observing logs. (Relevant for correct.py)"), \
         ('calibrate', "Print debugging information about calibration."),\
        ]

modes.sort()

# By default set all debug modes to False
for ii, (m, desc) in enumerate(modes):
    exec("%s = False" % m.upper())


def set_mode_on(*modes):
    for m in modes:
        exec "%s = True" % m.upper() in globals() 


def set_allmodes_on():
    for m, desc in modes:
        exec "%s = True" % m.upper() in globals() 


def set_allmodes_off():
    for m, desc in modes:
        exec "%s = False" % m.upper() in globals() 


def set_mode_off(*modes):
    for m in modes:
        exec "%s = False" % m.upper() in globals() 


def get_on_modes():
    on_modes = []
    for m, desc in modes:
        if eval('%s' % m.upper()):
            on_modes.append('debug.%s' % m.upper())
    return on_modes


def is_on(mode):
    return eval('%s' % mode.upper())


def print_debug_status():
    on_modes = get_on_modes()
    print "The following debugging modes are turned on:"
    for m in on_modes:
        print "    %s" % m

