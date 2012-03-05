import sys
import ast
import os.path
import ConfigParser

import utils
import debug

# Read in global configurations
execfile(os.path.join(os.path.split(__file__)[0], "global.cfg"), {}, locals())


class ConfigDict(dict):
    def __getattr__(self, key):
        return self.__getitem__(key)


class CoastGuardConfigs(object):
    def __init__(self, base_config_dir=default_config_dir):
        self.base_config_dir = base_config_dir
        self.config_dicts = {}
        self.configs = ConfigDict()

    def __getattr__(self, key):
        return self.configs[key]

    def __getitem__(self, key):
        return self.config_dicts[key]

    def read_file(self, fn, required=False):
        if required and not os.path.isfile(fn):
            raise ValueError("Configuration file (%s) doesn't exist " \
                             "and is required!" % fn)
        if not fn.endswith('.cfg'):
            raise ValueError("Coast Guard configuration files must " \
                             "end with the extention '.cfg'.")
        key = os.path.split(fn)[-1][:-4]
        self.config_dicts[key] = ConfigDict()
        execfile(fn, {}, self.config_dicts[key])
        # Load just-read configurations into current configs
        self.configs.update(self.config_dicts[key])

    def get_default_configs(self):
        """Read the default configurations and return them.
 
            Inputs:
                None

            Outputs:
                None
        """
        default_config_fn = os.path.join(self.base_config_dir, "default.cfg")
        self.read_file(default_config_fn, required=True)

    def get_configs_for_archive(self, arfn):
        """Given a psrchive archive file return relevant configurations.
            This will include configurations for the telescope, frontend,
            backend, and pulsar.
 
            Inputs:
                fn: The psrchive archive to get configurations for.
 
            Outputs:
                None
        """
        # Create a list of all the configuration files to check
        config_files = []
        telescope = utils.site_to_telescope[arfn.telescop.lower()]
        config_files.append(os.path.join(self.base_config_dir, 'telescopes', \
                                "%s.cfg" % telescope.lower()))
        config_files.append(os.path.join(self.base_config_dir, 'receivers', \
                                "%s.cfg" % arfn.rcvr.lower()))
        config_files.append(os.path.join(self.base_config_dir, 'backends', \
                                "%s.cfg" % arfn.backend.lower()))
        config_files.append(os.path.join(self.base_config_dir, 'pulsars', \
                                "%s.cfg" % arfn.name.upper()))
        config_files.append(os.path.join(self.base_config_dir, 'observations', \
                                "%s.cfg" % os.path.split(arfn.fn)[-1]))
 
        msg = "\n    ".join(["Checking for the following configurations:"] + \
                                config_files)
        utils.print_debug(msg, 'config')
        
        for fn in config_files:
            if os.path.isfile(fn):
                self.read_file(fn)
             

def main():
    cfg = CoastGuardConfigs()
    cfg.get_default_configs()
    cfg.get_configs_for_archive(sys.argv[1])
    print '-'*25
    print cfg['default']['conf'], cfg['default'].conf, cfg.conf

if __name__ == '__main__':
    main()
