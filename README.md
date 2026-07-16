# MeerGuard
The MeerTime copy of coast_guard: https://github.com/plazar/coast_guard

The code has been stripped for only RFI excision, and modified for use on wide-bandwidth data.

The surgical cleaner can now read in a template, which it subtracts from the data to form profile residuals. Unlike for coast_guard, the template has to be frequency-dependent and is used to identify an off-pulse region. The statistics used by the surgical cleaner are calculated only using this off-pulse region.

The code can be installed using

```
python setup.py install
```

The MeerGuard pipeline needs at least three options to run: the archive file to clean, the output file name, and a 2D template archive file. The routine subtracts the template to avoid zapping bright scintles.

Example usage:

```
python clean_archive.py *rf -T my_template_file.rf -e mg -C config_file
```

Check out `examples` for an example MeerKAT UHF archive file and template of J0045-7319 that work with `MeerGuard`. Note that because J0045-7319 is so faint and the file contains only one subintegration, the pulse is not visible in this one file. This is just an example for RFI excision testing!

Note: for MeerKAT observations where each sub-integration is stored in a separate file, add all files together with `psradd` before running `MeerGuard` for best results.
