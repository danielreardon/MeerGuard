import numpy as np

from coast_guard import config
from coast_guard import cleaners
from coast_guard import clean_utils
from coast_guard.cleaners import config_types
from coast_guard import utils

class SurgicalScrubCleaner(cleaners.BaseCleaner):
    name = 'surgical'
    description = 'De-weight profiles that stand out compared to others ' \
                    'in the same subint/channel using multiple stats.'

    def _set_config_params(self):
        self.configs.add_param('chanthresh', config_types.FloatVal, \
                         aliases=['cthresh'], \
                         help='The threshold (in number of sigmas) a ' \
                                'profile needs to stand out compared to ' \
                                'others in the same channel for it to ' \
                                'be removed.')
        self.configs.add_param('subintthresh', config_types.FloatVal, \
                         aliases=['sthresh'], \
                         help='The threshold (in number of sigmas) a ' \
                                'profile needs to stand out compared to ' \
                                'others in the same sub-int for it to ' \
                                'be removed.')
        self.configs.add_param('chan_order', config_types.IntList, \
                        aliases=['corder', 'chanorder'], \
                        help='The order of polynomial to remove from piecewise ' \
                                'segements of each channel. Multiple values ' \
                                'will cause channels to be detrended multiple ' \
                                'times in sequence, each time with the next ' \
                                'parameter.')
        self.configs.add_param('chan_breakpoints', config_types.IntListList, \
                        aliases=['cbp', 'chanbreakpoints', 'chanbp'], \
                        nullable=True, \
                        help='The breakpoints to use for defining piecewise ' \
                            'segments of each channel when detrending. ' \
                            'Multiple values will cause channels to be ' \
                            'detrended multiple times in sequence, each ' \
                            'time with the next list of breakpoints.')
        self.configs.add_param('chan_numpieces', config_types.IntList, \
                        aliases=['cnp', 'channumpieces', 'channp'], \
                        help='The number of equally sized peices to use for ' \
                            'defining piecewise segments of each channel when '
                            'detrending. Multiple values will cause channels ' \
                            'to be detrended multiple times in sequence, each ' \
                            'time with the next parameter.')
        self.configs.add_param('subint_order', config_types.IntList, \
                        aliases=['sorder', 'subintorder'], \
                        help='The order of polynomial to remove from piecewise ' \
                                'segements of each sub-int. Multiple values ' \
                                'will cause sub-ints to be detrended multiple ' \
                                'times in sequence, each time with the next ' \
                                'parameter.')
        self.configs.add_param('subint_breakpoints', config_types.IntListList, \
                        aliases=['sbp', 'subintbreakpoints', 'subintbp'], \
                        nullable=True, \
                        help='The breakpoints to use for defining piecewise ' \
                            'segments of each sub-int when detrending. ' \
                            'Multiple values will cause sub-ints to be ' \
                            'detrended multiple times in sequence, each ' \
                            'time with the next list of breakpoints.')
        self.configs.add_param('subint_numpieces', config_types.IntList, \
                        aliases=['snp', 'subintnumpieces', 'subintnp'], \
                        help='The number of equally sized peices to use for ' \
                            'defining piecewise segments of each sub-int when '
                            'detrending. Multiple values will cause sub-ints ' \
                            'to be detrended multiple times in sequence, each ' \
                            'time with the next parameter.')
        self.parse_config_string(config.cfg.surgical_default_params)

    def _clean(self, ar):
        patient = ar.clone()
        patient.pscrunch()
        patient.remove_baseline()
        
        # Remove profile from dedispersed data
        patient.dedisperse()
        data = patient.get_data().squeeze()
        template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
        clean_utils.remove_profile_inplace(patient, template)
        # re-set DM to 0
        patient.dededisperse()
        
        # Get weights
        weights = patient.get_weights()
        # Get data (select first polarization - recall we already P-scrunched)
        data = patient.get_data()[:,0,:,:]
        data = clean_utils.apply_weights(data, weights)
       
        # Mask profiles where weight is 0
        mask_2d = np.bitwise_not(np.expand_dims(weights, 2).astype(bool))
        mask_3d = mask_2d.repeat(ar.get_nbin(), axis=2)
        data = np.ma.masked_array(data, mask=mask_3d)
        
        # RFI-ectomy must be recommended by average of tests
        avg_test_results = clean_utils.comprehensive_stats(data, axis=2, \
                                    chanthresh=self.configs.chanthresh, \
                                    subintthresh=self.configs.subintthresh, \
                                    chan_order=self.configs.chan_order, \
                                    chan_breakpoints=self.configs.chan_breakpoints, \
                                    chan_numpieces=self.configs.chan_numpieces, \
                                    subint_order=self.configs.subint_order, \
                                    subint_breakpoints=self.configs.subint_breakpoints, \
                                    subint_numpieces=self.configs.subint_numpieces, \
                                    )
        for (isub, ichan) in np.argwhere(avg_test_results>=1):
            # Be sure to set weights on the original archive, and
            # not the clone we've been working with.
            integ = ar.get_Integration(int(isub))
            integ.set_weight(int(ichan), 0.0)
      

Cleaner = SurgicalScrubCleaner

