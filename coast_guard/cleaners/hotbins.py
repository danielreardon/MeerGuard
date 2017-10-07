import numpy as np
from coast_guard import config
from coast_guard import cleaners
from coast_guard.cleaners import config_types
from coast_guard import utils


class HotbinsCleaner(cleaners.BaseCleaner):
    name = 'hotbins'
    description = 'Replace profile bins that are significantly brighter ' \
                    'than the profile average with white noise.'


    def _set_config_params(self):
        self.configs.add_param('threshold', config_types.FloatVal, \
                               aliases=['thresh'], \
                               help='The threshold (in number of sigmas) for a ' \
                                    'bin to be removed.')
        self.configs.add_param('fscrunchfirst', config_types.BoolVal, \
                               help='Determine which bins to removed by ' \
                                    'looking at frequency scrunched data. Remove ' \
                                    'the hot bins in all frequency channels.')
        self.configs.add_param('tscrunchfirst', config_types.BoolVal, \
                               help='Determine which bins to removed by ' \
                                    'looking at time scrunched data. Remove ' \
                                    'the hot bins in all sub-integrations.')
        self.configs.add_param('onpulse', config_types.IntPairList, \
                               help='On-pulse regions to be ignored when ' \
                                    'computing profile statistics. A list ' \
                                    'of 2-tuples is expected.')
        self.configs.add_param('iscal', config_types.BoolVal, \
                               help='Whether or not the observations ' \
                                    'is a calibrator scan. If True, ' \
                                    'the "onpulse" config is ignored ' \
                                    'and the on-cal region is determined ' \
                                    'by correlating with a top-hat of ' \
                                    'width "calfrac".')
        self.configs.add_param('calfrac', config_types.FloatVal, \
                                help='The duty cycle of the cal.')
        self.parse_config_string(config.cfg.hotbins_default_params)


    def _clean(self, ar):
        reference = ar.clone()
        reference.pscrunch()
        if self.configs.fscrunchfirst:
            if ar.get_dedispersed():
                raise errors.CleanError('The "hotbins" cleaner "fscrunchfirst"' \
                                        'an only be used on non-dedispersed data.')
            utils.print_debug('Determining hotbins based on f-scrunched data', 'clean')
            reference.set_dispersion_measure(0)
            reference.fscrunch()
        if self.configs.tscrunchfirst:
            utils.print_debug('Determining hotbins based on t-scrunched data', 'clean')
            reference.tscrunch()

        if self.configs.iscal:
            calbins = self.__locate_cal(ar)
            # Clean on-cal region
            self.__find_and_replace_hotbins(ar, reference, calbins)
            # Clean off-cal region
            self.__find_and_replace_hotbins(ar, reference, ~calbins)
        else:
            offbins = np.ones(ar.get_nbin(), dtype='bool')
            for lobin, hibin in self.configs.onpulse:
                offbins[lobin:hibin] = False
            self.__find_and_replace_hotbins(ar, reference, offbins)


    def __find_and_replace_hotbins(self, ar, reference, offbins):
        nbins = ar.get_nbin()
        indices = np.arange(nbins)
        offbin_indices = indices[offbins]
        for isub in np.arange(reference.get_nsubint()):
            for ichan in np.arange(reference.get_nchan()):
                # Always use first polarization channel
                # (i.e. use total intensity - data are p-scrunched)
                prof = reference.get_Profile(int(isub), 0, int(ichan))
                data = prof.get_amps()
                offdata = data[offbins]
                med = np.median(offdata)
                mad = np.median(np.abs(offdata-med))
                std = mad*1.4826 # This is the approximate relation between the
                                 # standard deviation and the median absolute
                                 # deviation (assuming normally distributed data).
                ioffbad = np.abs(offdata-med) > std*self.configs.threshold
                ibad = offbin_indices[ioffbad]
                igood = offbin_indices[~ioffbad]
                nbad = np.sum(ioffbad)
                utils.print_debug('isub: %d, ichan: %d, ipol: %d\n' \
                                  '    med: %g, mad: %g\n' \
                                  '    %d hotbins found (ibin: %s)' % \
                                 (isub, ichan, 0, med, mad, nbad, ibad), 'clean')
                # Replace data in cleaned archive with noise
                if self.configs.fscrunchfirst:
                    chans_to_clean = np.arange(ar.get_nchan())
                else:
                    chans_to_clean = [int(ichan)]
                if self.configs.tscrunchfirst:
                    subints_to_clean = np.arange(ar.get_nsubint())
                else:
                    subints_to_clean = [int(isub)]
                # We always p-scrunch
                pols_to_clean = np.arange(ar.get_npol())

                for jsub in subints_to_clean:
                    for jchan in chans_to_clean:
                        for jpol in pols_to_clean:
                            cleanedprof = ar.get_Profile(int(jsub), int(jpol), int(jchan))
                            cleaneddata = cleanedprof.get_amps()
                            gooddata = cleaneddata[igood]
                            avg = gooddata.mean()
                            std = gooddata.std()
                            if std > 0:
                                noise = np.random.normal(avg, std, size=nbad).astype('float32')
                                cleaneddata[ibad] = noise


    def __locate_cal(self, ar):
        return utils.locate_cal(ar, calfrac=self.configs.calfrac)


Cleaner = HotbinsCleaner
