import numpy as np

import config
import cleaners
import config_types
import utils

class HotbinsCleaner(cleaners.BaseCleaner):
    name = 'hotbins'
    description = 'Replace profile bins that are significantly brighter ' \
                    'than the profile average with white noise.'

    def _set_config_params(self):
        self.configs.add_param('threshold', config_types.FloatVal, \
                         default=str(config.cfg.clean_hotbins_thresh), \
                         aliases=['thresh'], \
                         help='The threshold (in number of sigmas) for a ' \
                                'bin to be removed.')
        self.configs.add_param('fscrunchfirst', config_types.BoolVal, \
                         default=str(config.cfg.clean_hotbins_fscrunchfirst), \
                         help='Determine which bins to removed by ' \
                              'looking at frequency scrunched data. Remove ' \
                              'the hot bins in all frequency channels.')
        self.configs.add_param('onpulse', config_types.IntPairListVal, \
                         default="",
                         help='On-pulse regions to be ignored when computing ' \
                              'profile statistics. A list of 2-tuples is expected.')

    def _clean(self, ar):
        nbins = ar.get_nbin()
        indices = np.arange(nbins)
        offbins = np.ones(nbins, dtype='bool')
        offbin_indices = indices[offbins]
        for lobin, hibin in self.configs.onpulse:
            offbins[lobin:hibin] = False
      
        if self.configs.fscrunchfirst:
            utils.print_debug("Determining hotbins based on f-scrunched data", 'clean')
            reference = ar.clone()
            reference.set_dispersion_measure(0)
            reference.fscrunch()
        else:
            reference = ar
        nsub = reference.get_nsubint()
        for isub in np.arange(nsub):
            for ichan in np.arange(reference.get_nchan()):
                for ipol in np.arange(reference.get_npol()):
                    prof = reference.get_Profile(int(isub), int(ipol), int(ichan))
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
                                (isub, ichan, ipol, med, mad, nbad, ibad), 'clean')
                    # Replace data in cleaned archive with noise
                    if self.configs.fscrunchfirst:
                        # We need to clean all frequency channels
                        for jchan in np.arange(ar.get_nchan()):
                            cleanedprof = ar.get_Profile(int(isub), int(ipol), int(jchan))
                            cleaneddata = cleanedprof.get_amps()
                            gooddata = cleaneddata[igood]
                            avg = gooddata.mean()
                            std = gooddata.std()
                            if std > 0:
                                noise = np.random.normal(avg, std, size=nbad).astype('float32')
                                cleaneddata[ibad] = noise
                    else:
                        gooddata = data[igood]
                        avg = gooddata.mean()
                        std = gooddata.std()
                        if std > 0:
                            noise = np.random.normal(avg, std, size=nbad).astype('float32')
                            data[ibad] = noise

Cleaner = HotbinsCleaner

