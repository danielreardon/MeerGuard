import textwrap

from coast_guard import config
from coast_guard.cleaners import config_types
from coast_guard import utils
from coast_guard import errors
from coast_guard import colour

registered_cleaners = ['hotbins', 'surgical', 'rcvrstd', 'bandwagon']

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
   
    def __repr__(self):
        return "<%s object -- params: %s>" % \
                    (self.__class__.__name__, self.configs)

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
        self.configs.set_from_string(cfgstr)

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
        wrapper2 = textwrap.TextWrapper(initial_indent=" "*12, \
                                        subsequent_indent=" "*16)
                                        
        if full:
            helplines.append("    Parameters:")
            for cfg in sorted(self.configs.types):
                cfgtype = self.configs.types[cfg]
                helpstr = self.configs.helpstrs[cfg]
                helplines.append(wrapper.fill("%s -- %s" % (cfg, helpstr)))
                helplines.append("")
                helplines.append(wrapper2.fill(cfgtype.get_help()))
                if cfg in self.configs.cfgstrs:
                    helplines.append(wrapper2.fill("Default: %s" % \
                                            self.configs.cfgstrs[cfg]))
                else:
                    helplines.append(wrapper2.fill("Required"))
                helplines.append("")
        helptext = "\n".join(helplines)
        return helptext

    def run(self, ar):
        utils.print_info("Cleaning '%s' with %s" % (ar.get_filename(), self.name), 1)
        utils.print_debug("Cleaning parameters: %s" % self.get_config_string(), 'clean')
        self._clean(ar)


class Configurations(dict):
    """An object for cleaner configurations.
        
        Contains methods for parsing configuration strings, and writing 
        normalised configuration strings.
    """
    def __init__(self, *args, **kwargs):
        super(Configurations, self).__init__(*args, **kwargs)
        self.cfgstrs = {}
        self.types = {} # dictionary where keys are configuration names
                        # and values are the caster functions

        self.aliases = {} # dictionary where keys are aliases and
                          # values are the normalised names, which 
                          # appear in 'types'
        self.helpstrs = {} # dictionary where keys are configuration names
                           # and values are help strings.

    def __str__(self):
        return self.to_string()

    def __setitem__(self, key, valstr):
        key = self.aliases.get(key, key) # Normalise key 
                                         # in case an alias was provided
        cfgtype = self.types[key]
        # Save normalized value-string and normalised key pairs
        self.cfgstrs[key] = cfgtype.normalize_param_string(valstr)
        # Convert string into value
        castedval = cfgtype.get_param_value(valstr) 
        super(Configurations, self).__setitem__(key, castedval)

    def __getattr__(self, key):
        return self[key]

    def to_string(self):
        # Sort to normalise order
        return ",".join(sorted(["%s=%s" % ii for ii in self.cfgstrs.iteritems()]))

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

    def add_param(self, name, cfgtype, default=None, aliases=[], \
                    help="", nullable=False):
        """Add a single configuration parameter.

            Inputs:
                name: The normalised name of the parameter.
                cfgtype: The configuration type. This must be a
                    subclass of ConfigType.
                aliases: A list of alternative ways the user can specify 
                    this parameter.
                    (Default: No aliases)
                help: Help text describing the parameter.
                    (Default: No help text)
                nullable: If value can be set as None.
                    (Default: not nullable).

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
            self.types[name] = cfgtype(nullable=nullable)
        else:
            raise ValueError("The provided 'cfgtype' (name=%s) is not a subclass of " 
                             "BaseConfigType. (type=%s)" % 
                             (cfgtype.__name__, type(cfgtype)))
        # Add the aliases
        for alias in aliases:
            self.aliases[alias] = name
        # Set help string
        self.helpstrs[name] = help
