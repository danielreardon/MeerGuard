import types

class BaseConfigType(object):
    """The base class of ConfigType objects.

        ConfigType objects are used to define the parameters
        of cleaner config-strings.
    """
    name = NotImplemented
    description = None

    def __init__(self, nullable=False):
        self.nullable = nullable

    def get_param_value(self, paramstr):
        """Parse a parameter string.

            Inputs:
                paramstr: The parameter string to parse.

            Output:
                val: The parameter value.
        """
        if self.nullable and paramstr.lower() == 'none':
            return None
        else:
            return self._string_to_value(paramstr)

    def _string_to_value(self, paramstr):
        """Parse a parameter string.

            Inputs:
                paramstr: The parameter string to parse.

            Output:
                val: The parameter value.
        """
        raise NotImplementedError("The method _string_to_value(...) of " \
                                    "ConfigType objects must be implemented " \
                                    "by its subclases.")

    def normalize_param_string(self, paramstr):
        """Return a normalized version of the parameter string.

            Inputs:
                paramstr: The parameter string to parse.

            Output:
                normed: The normalized parameter string.
        """
        val = self.get_param_value(paramstr)
        if val is None:
            return "None"
        else:
            return self._value_to_string(val)
    
    def get_help(self):
        helpstr = "Type: %s" % self.name.strip()
        if self.description is not None:
            helpstr += " - %s" % self.description.strip()
        return helpstr


class IntVal(BaseConfigType):
    """A configuration type for integer values.
    """
    name = "int"

    def _string_to_value(self, paramstr):
        """Parse 'paramstr' as a normal integer value.
            The previous parameter value is ignored.
        """
        return int(paramstr)
    
    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        return str(val)


class FloatVal(BaseConfigType):
    """A configuration type for floating-point values.
    """
    name = "float"

    def _string_to_value(self, paramstr):
        """Parse 'paramstr' as a normal floating-point value.
            The previous parameter value is ignored.
        """
        return float(paramstr)
    
    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        return "%g" % val


class BoolVal(BaseConfigType):
    """A configuration type for boolean values.
    """
    name = "bool"
    description = "The following values are recognised (case insensitive): " \
                    "true, 1, y, yes, false, 0, n, no"

    def _string_to_value(self, paramstr):
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

    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        return str(val)


def _str_to_intlist(paramstr):
    """Parse 'paramstr' as a list of integers. The format must
        be <int>[;<int>...]. 
    """
    if paramstr.strip():
        # Contains at least one element
        intstrs = paramstr.split(';')
        return [int(ss) for ss in intstrs]
    else:
        return []


def _str_to_int_pair(paramstr):
    # Convert ':' to ';' so we can re-use string-to-intlist function
    intlist = _str_to_intlist(paramstr.replace(':',';'))
    if len(intlist) != 2:
        raise ValueError("Bad number of integer strings in '%s'. Exactly 2 " \
                        "expected. Integers should be separated by ':'." % \
                        paramstr)
    return tuple(intlist)


class IntList(BaseConfigType):
    """A configuration type for a list of integers.
    """
    name = "list of integers"
    description = "an integer list <int>[;<int>...]"
    
    def _string_to_value(self, paramstr):
        return _str_to_intlist(paramstr)

    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        return ";".join(["%d" % ii for ii in val])


class IntListList(BaseConfigType):
    """A configuration type for a list of integer lists.
    """
    name = "list of integer lists"
    description = "an integer list <int>[;<int>...][;;<int>[;<int>...]...]"
    
    def _string_to_value(self, paramstr):
        """Parse 'paramstr' as a list of integer lists. The format must
            be <int>[;<int>...][;;<int>[;<int>...]...]. 
        """
        intlists = []
        if paramstr.strip():
            remainder = paramstr
            while remainder:
                liststr, sep, remainder = remainder.partition(';;')
                intlists.append(_str_to_intlist(liststr))
        return intlists
    
    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        intliststrs = []
        for ints in val:
            intliststrs.append(";".join(["%d" % ii for ii in ints]))
        return ";;".join(intliststrs)


class IntPairList(BaseConfigType):
    """A configuration type for a list of integer pairs.
    """
    name = "list of integer pairs"
    description = "a list of integer pairs <int>:<int>[;<int>:<int>...]. "

    def _string_to_value(self, paramstr):
        """Parse 'paramstr' as a list of integer pairs. The format must
            be <int>:<int>[;<int>:<int>...]. 
        """
        if paramstr.strip():
            # Contains at least one pair
            pairstrs = paramstr.split(';')
            return [_str_to_int_pair(ss) for ss in pairstrs]
        else:
            return []
    
    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        return ";".join(["%d:%d" % pair for pair in val])


class IntOrIntPairList(BaseConfigType):
    """A configuration type for a list of integer-or-integer-pairs.
    """
    name = "list of integer-or-integer-pairs"
    description = "a list of integers or integer pairs " \
                    "(<int>|<int>:<int>)[;(<int>|<int>:<int>)...]." 

    def _string_to_value(self, paramstr):
        """Parse 'paramstr' as a list of integer-or-integer-pairs. The format
            must be (<int>|<int>:<int>)[;(<int>|<int>:<int>)...].
        """
        val = []
        if paramstr.strip():
            # Contains at least one element
            for element in paramstr.split(';'):
                if ':' in element:
                    val.append(_str_to_int_pair(element))
                else:
                    val.append(int(element))
        return val

    def _value_to_string(self, val):
        strs = []
        for el in val:
            if type(el) is types.TupleType:
                strs.append("%d:%d" % el)
            else:
                strs.append("%d" % el)
        return ";".join(strs)


def _str_to_floatlist(paramstr):
    """Parse 'paramstr' as a list of floats. The format must
        be <float>[;<float>...]. 
    """
    if paramstr.strip():
        # Contains at least one element
        floatstrs = paramstr.split(';')
        return [float(ss) for ss in floatstrs]
    else:
        return []


def _str_to_float_pair(paramstr):
    # Convert ':' to ';' so we can re-use string-to-floatlist function
    floatlist = _str_to_floatlist(paramstr.replace(':',';'))
    if len(floatlist) != 2:
        raise ValueError("Bad number of float strings in '%s'. Exactly 2 " \
                        "expected. Floats should be separated by ':'." % \
                        paramstr)
    return tuple(floatlist)


class FloatList(BaseConfigType):
    """A configuration type for a list of floats.
    """
    name = "list of floats"
    description = "an float list <float>[;<float>...]"
    
    def _string_to_value(self, paramstr):
        return _str_to_floatlist(paramstr)

    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        return ";".join(["%g" % ii for ii in val])


class FloatPair(BaseConfigType):
    """A configuration type for a float pair.
    """
    name = "a float pair"
    description = "a pair of floats <float>:<float>. "

    def _string_to_value(self, paramstr):
        """Parse 'paramstr' as a pair of floats. The format must
            be <float>:<float>. 
        """
        return _str_to_float_pair(paramstr)
    
    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        return "%g:%g" % val


class FloatPairList(BaseConfigType):
    """A configuration type for a list of float pairs.
    """
    name = "list of float pairs"
    description = "a list of float pairs <float>:<float>[;<float>:<float>...]. "

    def _string_to_value(self, paramstr):
        """Parse 'paramstr' as a list of float pairs. The format must
            be <float>:<float>[;<float>:<float>...]. 
        """
        if paramstr.strip():
            # Contains at least one pair
            pairstrs = paramstr.split(';')
            return [_str_to_float_pair(ss) for ss in pairstrs]
        else:
            return []
    
    def _value_to_string(self, val):
        """Return a normalized version of the value.
        """
        return ";".join(["%g:%g" % pair for pair in val])


class FloatOrFloatPairList(BaseConfigType):
    """A configuration type for a list of float-or-float-pairs.
    """
    name = "list of float-or-float-pairs"
    description = "a list of floats or float pairs " \
                    "(<float>|<float>:<float>)[;(<float>|<float>:<float>)...]." 

    def _string_to_value(self, paramstr):
        """Parse 'paramstr' as a list of float-or-float-pairs. The format
            must be (<float>|<float>:<float>)[;(<float>|<float>:<float>)...].
        """
        val = []
        if paramstr.strip():
            # Contains at least one element
            for element in paramstr.split(';'):
                if ':' in element:
                    val.append(_str_to_float_pair(element))
                else:
                    val.append(float(element))
        return val

    def _value_to_string(self, val):
        strs = []
        for el in val:
            if type(el) is types.TupleType:
                strs.append("%g:%g" % el)
            else:
                strs.append("%g" % el)
        return ";".join(strs)
