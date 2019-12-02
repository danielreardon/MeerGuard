import numpy as np
from coast_guard import config
from coast_guard import cleaners
from coast_guard import clean_utils
from coast_guard.cleaners import config_types
from coast_guard import utils
from scipy.optimize import leastsq
from scipy.signal import savgol_filter

# for the template, would be better to have it elsewhere and just get the numpy array here
import psrchive

# For Daniel's debugging..
plot_diagnostic = False

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
        self.configs.add_param('template', config_types.StrVal,
                               aliases=[],
                               nullable=True,
                               help="Filename for a template to use yadayada")
        self.parse_config_string(config.cfg.surgical_default_params)


    def _clean(self, ar):
        patient = ar.clone()
        patient.pscrunch()
        patient.remove_baseline()

        # Remove profile from dedispersed data
        patient.dedisperse()
        data = patient.get_data().squeeze()
        print('Loading template')
        if self.configs.template is None:
            template = np.apply_over_axes(np.sum, data, (0, 1)).squeeze()
        else:
            template_ar = psrchive.Archive_load(self.configs.template)
            template_ar.pscrunch()
            template_ar.remove_baseline()
            template_ar.dedisperse()
            if len(template_ar.get_frequencies()) > 1 and len(template_ar.get_frequencies()) < len(patient.get_frequencies()):
                print("Template channel number doesn't match data... f-scrunching!")
                template_ar.fscrunch()
            template_ar.remove_baseline()
            template = np.apply_over_axes(np.sum, template_ar.get_data(), (0, 1)).squeeze()
            # make sure template is 1D
            if len(np.shape(template)) > 1:  # sum over frequencies too
                template_ar.fscrunch()  
                print("2D template found. Assuming it has same frequency coverage and channels as data!")
                template_phs = np.apply_over_axes(np.sum, template_ar.get_data(), (0, 1)).squeeze()
            else:
                template_phs = template

        print('Estimating template and profile phase offset')
        if self.configs.template is None:
            phs = 0
        else:
            # Calculate phase offset of template in number of bins, using full obs
            profile = patient.clone()
            profile.dedisperse()
            profile.tscrunch()
            profile.fscrunch()
            # Get profile data of full obs
            profile = profile.get_data()[0,0,0,:]
            if np.shape(template_phs) != np.shape(profile):
                print('template and profile have different numbers of phase bins')
            err = (lambda (amp, phs, base): amp*clean_utils.fft_rotate(template_phs, phs) +base - profile)
            amp_guess = max(profile)-min(profile) - max(template_phs)
            phase_guess = - np.argmax(profile) + np.argmax(template_phs)
            params, status = leastsq(err, [amp_guess, phase_guess, min(profile)])
            phs = params[1]
            print('Template phase offset = {0}'.format(round(phs, 3)))

        print('Removing profile from patient')
        preop_patient = patient.clone()
        preop_patient.remove_baseline()
        preop_weights = preop_patient.get_weights()

        clean_utils.remove_profile_inplace(patient, template, phs)
       
        print('Accessing weights and applying to patient')
        # re-set DM to 0
        # patient.dededisperse()

        # Get weights
        weights = patient.get_weights()
        # Get data (select first polarization - recall we already P-scrunched)
        data = patient.get_data()[:,0,:,:]
        preop_data = preop_patient.get_data()[:,0,:,:]
        data = clean_utils.apply_weights(data, weights)
        preop_data = clean_utils.apply_weights(preop_data, weights)
        
        # Mask profiles where weight is 0
        mask_2d = np.bitwise_not(np.expand_dims(weights, 2).astype(bool))
        mask_3d = mask_2d.repeat(ar.get_nbin(), axis=2)
        data = np.ma.masked_array(data, mask=mask_3d)
        preop_data = np.ma.masked_array(preop_data, mask=mask_3d)        
 
        print('Masking on-pulse region as determined from template')
        # consider residual only in off-pulse region
        if len(np.shape(template)) > 1:  # sum over frequencies
            print('Estimating on-pulse region by f-scrunching 2D template')
            template_ar.fscrunch()
            template_1D = np.apply_over_axes(np.sum, template_ar.get_data(), (0, 1)).squeeze()
        else:
            template_1D = template
        # Rotate template by apropriate amount
        template_rot = clean_utils.fft_rotate(template_1D, phs).squeeze()
        # masked_template = np.ma.masked_greater(template_rot, np.min(template_rot) + 0.01*np.ptp(template_rot))
        masked_template = np.ma.masked_greater(template_rot, np.median(template_rot))
        masked_std = np.ma.std(masked_template)
        # use this std of masked data as cutoff
        masked_template = np.ma.masked_greater(template_rot, np.median(template_rot) + 3*masked_std)
        if plot_diagnostic:
            import matplotlib.pyplot as plt
            plt.plot(np.apply_over_axes(np.sum, preop_data, (0, 1)).squeeze(), alpha=0.8)
            plt.plot(np.apply_over_axes(np.sum, data, (0, 1)).squeeze(), alpha=0.8)
            # Do fit again to scale template
            subchan, err, params = clean_utils.remove_profile1d(np.apply_over_axes(np.sum, preop_data, (0, 1)).squeeze(), 0, 0, template_rot, 0, return_params=True)
            plt.plot(params[0]*template_rot + params[1], alpha=0.8)
            plt.plot(params[0]*masked_template + params[1], color='k', alpha=1)
        
        # Loop through chans and subints to mask on-pulse phase bins
        for ii in range(0, np.shape(data)[0]):
            for jj in range(0, np.shape(data)[1]):
                  data.mask[ii, jj, :] = masked_template.mask
        data = np.ma.masked_array(data, mask=data.mask)
        
        if plot_diagnostic:
            plt.plot(np.apply_over_axes(np.ma.sum, data, (0, 1)).squeeze(), alpha=0.8)
            plt.legend(('Summed pre-op data', 'Summed data', 'Scaled rotated template', 'Scaled masked template', 'Masked data'))
            plt.savefig('diagnostic.png')

        print('Calculating robust statistics to determine where RFI removal is required')
        # RFI-ectomy must be recommended by average of tests
        # BWM: Ok, so this is where the magical stuff actually happens - need to know actually WHAT are the comprehensive stats
        # DJR: At this stage the stats are; (found to work well experimentally) 
        #          geometric mean, peak-to-peak, standard deviation, kurtosis, and skewness. 
        #      In original coast_guard they were;
        #          mean, peak-to-peak, standard deviation, and max value of FFT
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

        print('Applying RFI masking weights to archive')
        for (isub, ichan) in np.argwhere(avg_test_results>=1):
            # Be sure to set weights on the original archive, and
            # not the clone we've been working with.
            integ = ar.get_Integration(int(isub))
            integ.set_weight(int(ichan), 0.0)
        
        freq_fraczap = clean_utils.freq_fraczap(ar)

Cleaner = SurgicalScrubCleaner
