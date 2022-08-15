import sys
import copy
import os

base_config_dir = os.getenv("COASTGUARD_CFG", None)
if base_config_dir is None:
    raise ValueError("COASTGUARD_CFG environment variable must be set. "
                     "(It should point to the CoastGuard configurations "
                     "directory to use.)")
# python2
#execfile(os.path.join(base_config_dir, "global.cfg"), {}, locals())

# python3
with open(os.path.join(base_config_dir, "global.cfg")) as f:
    code = compile(f.read(), os.path.join(base_config_dir, "global.cfg"), 'exec')
    exec(code, {}, locals())

class ConfigDict(dict):
    def __add__(self, other):
        newcfg = copy.deepcopy(self)
        newcfg.update(other)
        return newcfg

    def __str__(self):
        lines = []
        for key in sorted(self.keys()):
            lines.append("%s: %r" % (key, self[key]))
        return "\n".join(lines)


def read_file(fn, required=False):
    cfgdict = ConfigDict()
    if os.path.isfile(fn):
        if not fn.endswith('.cfg'):
            raise ValueError("Coast Guard configuration files must "
                             "end with the extention '.cfg'.")
        key = os.path.split(fn)[-1][:-4]
        # python2
        #execfile(fn, {}, cfgdict)
        # python3
        with open(fn) as f:
            code = compile(f.read(), fn, 'exec')
            exec(code, {}, cfgdict)
    elif required:
            raise ValueError("Configuration file (%s) doesn't exist "
                             "and is required!" % fn)
    return cfgdict


class CoastGuardConfigs(object):
    def __init__(self, base_config_dir=base_config_dir):
        self.base_config_dir = base_config_dir
        default_config_fn = os.path.join(self.base_config_dir, "default.cfg")

        self.defaults = read_file(default_config_fn, required=True)
        self.obsconfigs = ConfigDict()
        self.overrides = ConfigDict()

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __getitem__(self, key):
        if key in self.overrides:
            val = self.overrides[key]
        elif key in self.obsconfigs:
            val = self.obsconfigs[key]
        elif key in self.defaults:
            val = self.defaults[key]
        else:
            print("The configuration {0} cannot be found!".format(key))
            sys.exit()
        return val

    def __str__(self):
        allkeys = set.union(set(self.defaults.keys()),
                            set(self.obsconfigs.keys()),
                            set(self.overrides.keys()))
        lines = ["Current configurations:"]
        #for key in allkeys:
        #    lines.append("    %s: %s" % (key, self[key]))
        lines.append("    "+str(self.defaults+self.obsconfigs+self.overrides).replace("\n", "\n    "))
        lines.append("Overrides:")
        lines.append("    "+str(self.overrides).replace("\n", "\n     "))
        lines.append("Observation configurations:")
        lines.append("    "+str(self.obsconfigs).replace("\n", "\n    "))
        lines.append("Defaults:")
        lines.append("    "+str(self.defaults).replace("\n", "\n    "))
        return "\n".join(lines)
    
    def clear_obsconfigs(self):
        self.obsconfigs.clear()
    
    def clear_overrides(self):
        self.overrides.clear()

    def set_override_config(self, key, val):
        self.overrides[key] = val

    def load_configs_for_archive(self, arfn):
        """Given a psrchive archive file set current configurations to the values
            pre-set for this observation, pulsar, backend, receiver, telescope.
 
            Inputs:
                fn: The psrchive archive to get configurations for.
 
            Outputs:
                None
        """
        self.clear_obsconfigs()
        
        config_files = []  # A list of configuration files to look for

        telescope = arfn['telname']
        precedence = [arfn['telname'].lower(),
                      arfn['rcvr'].lower(),
                      arfn['backend'].lower()]
        
        cfgdir = self.base_config_dir
        for dirname in precedence:
            cfgdir = os.path.join(cfgdir, dirname)
            config_files.append(os.path.join(cfgdir, 'configs.cfg'))
        
        #config_files.append(os.path.join(self.base_config_dir, 'telescopes',
        #                    "%s.cfg" % telescope.lower()))
        #config_files.append(os.path.join(self.base_config_dir, 'receivers',
        #                    "%s.cfg" % arfn['rcvr'].lower()))
        #config_files.append(os.path.join(self.base_config_dir, 'backends',
        #                    "%s.cfg" % arfn['backend'].lower()))
        #config_files.append(os.path.join(self.base_config_dir, 'pulsars',
        #                    "%s.cfg" % arfn['name'].upper()))
        #config_files.append(os.path.join(self.base_config_dir, 'observations',
        #                    "%s.cfg" % os.path.split(arfn.fn)[-1]))
        #msg = "\n    ".join(["Checking for the following configurations:"] + \
        #                        config_files)
        
        for fn in config_files:
            self.obsconfigs += read_file(fn)


class ConfigManager(object):
    """An object to hold and manage CoastCuardConfigs objects
        from multiple threads.

        This is important because each thread may require
        different configurations.
    """

    def __init__(self):
        self.configs = {}

    def __contains__(self, name):
        return name in self.configs

    def get(self):
        name = os.getpid()
        if name not in self:
            self.configs[name] = CoastGuardConfigs()
        return self.configs[name]
   
    def load_configs_for_archive(self, arf):
        self.get().load_configs_for_archive(arf)

    def __getattr__(self, key):
        val = self.get()[key]
        return val 


cfg = ConfigManager()


def main():
    import utils
    if len(sys.argv) > 1:
        arf = utils.ArchiveFile(sys.argv[1])
        cfg.set_override_config("something", 'newvalue!')
        cfg.load_configs_for_archive(arf)
    print(cfg.get())


if __name__ == '__main__':
    main()
