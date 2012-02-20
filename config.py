import sys
import os.path
import ConfigParser

import utils

DEBUG = 1


def get_configs_for_archive(ar, base_config_dir='./configurations'):
    """Given a psrchive archive object return relevant configurations.
        This will include configurations for the telescope, frontend,
        backend, and pulsar.

        Inputs:
            ar: The psrchive archive to get configurations for.
            base_config_dir: Location of the configuration files.
                (Default: './configurations')

        Output:
            cfgs: The configurations.
    """
    fn = ar.get_filename()
    hdrparams = utils.parse_psrfits_header(fn, \
                        ['site', 'be:name', 'rcvr:name', 'name'])
    config_files = [os.path.join(base_config_dir, 'defaults.cfg')]
    telescope = utils.site_to_telescope[hdrparams['site'].lower()]
    config_files.append(os.path.join(base_config_dir, 'telescopes', \
                            "%s.cfg" % telescope.lower()))
    config_files.append(os.path.join(base_config_dir, 'receivers', \
                            "%s.cfg" % hdrparams['rcvr:name'].lower()))
    config_files.append(os.path.join(base_config_dir, 'backends', \
                            "%s.cfg" % hdrparams['be:name'].lower()))
    config_files.append(os.path.join(base_config_dir, 'pulsars', \
                            "%s.cfg" % hdrparams['name'].upper()))
    config_files.append(os.path.join(base_config_dir, 'observations', \
                            "%s.cfg" % os.path.split(fn)[-1]))

    if DEBUG:
        print "Checking for the following configurations:"
        for cfg in config_files:
            print "    %s" % cfg


def main():
    import psrchive
    ar = psrchive.Archive_load(sys.argv[1])
    print get_configs_for_archive(ar)


if __name__ == '__main__':
    main()
