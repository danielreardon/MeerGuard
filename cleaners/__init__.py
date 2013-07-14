import textwrap

import utils
import errors
import colour

registered_cleaners = ['hotbins']

__all__ = registered_cleaners


def load_cleaner(cleaner_name):
    """Import a cleaner class and return an instance.
        
        Input:
            cleaner_name: The name of the cleaner.

        Output:
            clean: A cleaner instance.
    """
    if cleaner_name not in registered_cleaners:
        raise errors.UnrecognizedValueError("The cleaner, '%s', " \
                    "is not a registered cleaner. The following " \
                    "are registered: '%s'" % \
                    (cleaner_name, "', '".join(registered_cleaners)))
    mod = __import__(cleaner_name, globals())
    return mod.Cleaner()


class BaseCleaner(object):
    """The base class of Cleaner objects.

        Cleaner objects take a single archive as input,
        cleans it, and returns a single output archive.
    """
    name = NotImplemented
    description = NotImplemented

    def __init__(self):
        self.configs = Configurations()
        self._set_config_params()
    
    def parse_config_string(self, cfgstr):
        """Parse a configuration string, setting the cleaner's
            configurable parameters accordingly.

            Input:
                cfgstr: A formatted string of configurations.
                    Format is:
                        <param1>=<val1>[,<param2>=<val2>,...]

            Outputs:
                None
        """
        self.configs.add_from_string(cfgstr)

    def _clean(self, ar):
        """Clean an ArchiveFile object in-place.

            Input:
                ar: The ArchiveFile object to clean.

            Outputs:
                None - The ArchiveFile object in cleaned in-place.
        """
        raise NotImplementedError("The '_clean' method of Cleaner "
                                    "classes must be defined.")

    def _set_config_params(self):
        """Set configuration parameters, aliases, defaults.
            NOTE: All permitted configurations must be set here.

            Inputs:
                None

            Outputs:
                None
        """
        pass
    
    def get_config_string(self):
        """Return a string of configurations.
            NOTE: Defaults will be included.

            Inputs:
                None

            Outputs:
                cfgstr: A formated configuration string including
                    default values. All parameter names will be 
                    normalised and sorted.
        """
        return self.configs.to_string()

    def get_help(self, full=False):
        helplines = []
        wrapper = textwrap.TextWrapper(subsequent_indent=" "*(len(self.name)+4))
        helplines.append("%s -- %s" % (colour.cstring(self.name, bold=True), 
                                wrapper.fill(self.description)))

        wrapper = textwrap.TextWrapper(initial_indent=" "*8, \
                                        subsequent_indent=" "*12)
        if full:
            helplines.append("    Parameters:")
            for cfg in sorted(self.configs.types):
                cfgtype = self.configs.types[cfg]
                helplines.append(wrapper.fill("%s -- %s" % (cfg, cfgtype.get_help())))
        helptext = "\n".join(helplines)
        return helptext

    def run(self, infn, outname, tmpdir=None):
        utils.print_info("Cleaning '%s' with %s" % (infn, self.name), 1)
        utils.print_debug("Cleaning parameters: %s" % self.get_config_string())
        ar = utils.ArchiveFile(infn)
        self._clean(ar)


class Configurations(dict):
    """An object for cleaner configurations.
        
        Contains methods for parsing configuration strings, and writing 
        normalised configuration strings.
    """
    types = {} # dictionary where keys are configuration names
               # and values are the caster functions

    aliases = {} # dictionary where keys are aliases and
                 # values are the normalised names, which 
                 # appear in 'types'

    def __init__(self, *args, **kwargs):
        super(Configurations, self).__init__(*args, **kwargs)
        self.cfgstrs = {}

    def __setitem__(self, key, valstr):
        key = self.aliases.get(key, key) # Normalise key 
                                         # in case an alias was provided
        self.cfgstrs[key] = valstr # Save value-string and normalised key pairs
        castedval = self.types[key](self.get(key, None), valstr) # Cast string into value
        super(Configurations, self).__setitem__(key, castedval)

    def to_string(self):
        cfgstrs = []
        for key, val in self.cfgstrs.iteritems():
            cfgstrs.append("%s=%s" % (key, val))
        cfgstrs.sort() # Sort to normalise order
        return ",".join(cfgstrs)

    def set_from_string(self, cfgstr):
        """Set configurations from a string.

            Input:
                cfgstr: A formatted string of configurations.
                    Format is:
                        <param1>=<val1>[,<param2>=<val2>,...]

            Outputs:
                None
        """
        for cfg in cfgstr.split(','):
            key, val = cfg.split('=')
            self[key] = val

    def add_param(self, name, cfgtype, default=None, aliases=[], help=""):
        """Add a single configuration parameter.

            Inputs:
                name: The normalised name of the parameter.
                cfgtype: The configuration type. This must be a
                    subclass of ConfigType.
                default: The default value of the parameter.
                    NOTE: not providing a default will mean it must be
                        provided by the user to use the cleaner.
                    (Default: No default value).
                aliases: A list of alternative ways the user can specify 
                    this parameter.
                    (Default: No aliases)
                help: Help text describing the parameter.
                    (Default: No help text)

            Outputs:
                None - The parameters are created and stored.
        """
        # Check that name and aliases are not already in use
        for key in [name]+aliases:
            if (key in self.types) or (key in self.aliases):
                raise ValueError("The name/alias (%s) is already in use. " \
                                 "Duplicates are not allowed." % key)

        # Check that the cfgtype is of ConfigType
        if issubclass(cfgtype, config_types.BaseConfigType):
            # Add the config name and type
            self.types[name] = cfgtype()
        else:
            raise ValueError("The provided 'cfgtype' is not a subclass of " \
                                "ConfigType. (type=%s)" % type(cfgtype))
        # Add the aliases
        for alias in aliases:
            self.aliases[alias] = name
        # Set the default value
        if default is not None:
            self[name] = default

