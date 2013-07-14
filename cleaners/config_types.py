class BaseConfigType(object):
    """The base class of ConfigType objects.

        ConfigType objects are used to define the parameters
        of cleaner config-strings.
    """
    name = NotImplemented
    description = None

    def __init__(self):
        pass

    def __call__(self, val, paramstr):
        self._parse_param_string(val, paramstr)

    def _parse_param_string(self, val, paramstr):
        """Parse a parameter string. Combine it with the previously
            assigned value, if relevant, and return the result.

            Inputs:
                val: The previously assigned value to the parameter
                    that's being parsed.
                paramstr: The parameter string to parse.

            Output:
                newval: The new parameter value.
        """
        raise NotImplementedError("The method _parse_param_string(...) of " \
                                    "ConfigType objects must be implemented " \
                                    "by its subclases.")
    
    def get_help(self):
        helpstr = "Type: %s" % self.name.strip()
        if self.description is not None:
            helpstr += " - %s" % self.description.strip()
        return helpstr


class FloatVal(BaseConfigType):
    """A configuration type for floating-point values.
    """
    name = "float"

    def _parse_param_string(self, val, paramstr):
        """Parse 'paramstr' as a normal floating-point value.
            The previous parameter value is ignored.
        """
        return float(paramstr)


class BoolVal(BaseConfigType):
    """A configuration type for boolean values.
    """
    name = "bool"
    description = "The following values are recognised (case insensitive): " \
                    "true, 1, y, yes, false, 0, n, no"

    def _parse_param_string(self, val, paramstr):
        """Parse 'paramstr' as a boolean value. The following values 
            are recognised (case insensitive):
                true, 1, y, yes, false, 0, n, no
            The previous parameter value is ignored.
        """
        boolstr = paramstr.lower()
        if boolstr in ('true', '1', 'y', 'yes'):
            newval = True
        elif boolstr in ('false', '0', 'n', 'no'):
            newval = False
        else:
            raise ValueError("The parameter string '%s' is not recognized. " \
                                "Only the following (case-insensitive) " \
                                "values are allowed: true, 1, y, yes, " \
                                "false, 0, n, no" % paramstr)
        return newval


class IntPairListVal(BaseConfigType):
    """A configuration type for a list of integer pairs.
    """
    name = "list of integer pairs"
    description = "an integer pair <int>:<int>. The pair is append to " \
                    "the list of previously collected pairs."

    def _parse_param_string(self, val, paramstr):
        """Parse 'paramstr' as a pair of integer values. The format must
            be <int>:<int>. The parsed value is appended to the value list.
        """
        if val is None:
            val = []
        intstrs = paramstr.split(':')
        if len(intstrs) != 2:
            raise ValueError("Bad number of integer strings in '%s'. Each " \
                             "integer should be separated by ':'." % paramstr)
        val.append((int(intstrs[0]), int(intstrs[1])))
        return val
