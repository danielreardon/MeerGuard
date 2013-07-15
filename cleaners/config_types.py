import operator

class BaseConfigType(object):
    """The base class of ConfigType objects.

        ConfigType objects are used to define the parameters
        of cleaner config-strings.
    """
    name = NotImplemented
    description = None

    def __init__(self):
        pass

    def get_param_value(self, paramstr):
        """Parse a parameter string.

            Inputs:
                paramstr: The parameter string to parse.

            Output:
                newval: The new parameter value.
        """
        raise NotImplementedError("The method _get_param_value(...) of " \
                                    "ConfigType objects must be implemented " \
                                    "by its subclases.")

    def normalize_param_string(self, paramstr):
        """Return a normalized version of the parameter string.

            Inputs:
                paramstr: The parameter string to parse.

            Output:
                normed: The normalized parameter string.
        """
        return paramstr
    
    def get_help(self):
        helpstr = "Type: %s" % self.name.strip()
        if self.description is not None:
            helpstr += " - %s" % self.description.strip()
        return helpstr


class FloatVal(BaseConfigType):
    """A configuration type for floating-point values.
    """
    name = "float"

    def get_param_value(self, paramstr):
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

    def get_param_value(self, paramstr):
        """Parse 'paramstr' as a boolean value. The following values 
            are recognised (case insensitive):
                true, 1, y, yes, false, 0, n, no
            The previous parameter value is ignored.
        """
        paramstr = paramstr.lower()
        if paramstr in ('true', '1', 'y', 'yes'):
            boolval = True
        elif paramstr in ('false', '0', 'n', 'no'):
            boolval = False
        else:
            raise ValueError("The parameter string '%s' is not recognized. " \
                                "Only the following (case-insensitive) " \
                                "values are allowed: true, 1, y, yes, " \
                                "false, 0, n, no" % paramstr)
        return boolval

    def normalize_param_string(self, paramstr):
        """Return a normalized version of the parameter string.
        """
        return str(self.get_param_value(paramstr))


class IntPairListVal(BaseConfigType):
    """A configuration type for a list of integer pairs.
    """
    name = "list of integer pairs"
    description = "an integer pair <int>:<int>. The pair is append to " \
                    "the list of previously collected pairs."

    def _to_int_pair(self, paramstr):
        intstrs = paramstr.split(':')
        if len(intstrs) != 2:
            raise ValueError("Bad number of integer strings in '%s'. Each " \
                             "integer should be separated by ':'." % paramstr)
        return (int(intstrs[0]), int(intstrs[1]))

    def get_param_value(self, paramstr):
        """Parse 'paramstr' as a list of integer pairs. The format must
            be <int>:<int>[;<int>:<int>...]. 
        """
        return [self._to_int_pair(ss) for ss in paramstr.split(';')]
    
    def normalize_param_string(self, paramstr):
        """Return a normalized version of the parameter string.
        """
        # Sort to normalize order
        pairs = self.get_param_value(paramstr)
        pairs.sort(key=operator.itemgetter(1))
        pairs.sort(key=operator.itemgetter(0))
        return ";".join(["%d:%d" % pair for pair in pairs])
