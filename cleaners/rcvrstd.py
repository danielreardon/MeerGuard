import types

import numpy as np

import config
import cleaners
import clean_utils
import config_types
import utils

class ReceiverBandCleaner(cleaners.BaseCleaner):
    name = 'rcvrstd'
    description = 'Prune, and tidy the observing band by trimming edges, ' \
                    'and removing bad channels/freq ranges.'

    def _set_config_params(self):
        self.configs.add_param('response', config_types.FloatPair, \
                        aliases=['resp'], nullable=True, \
                        help='The range of frequencies between which the ' \
                            'receiver has sensitivity. Any channels ' \
                            'outside this range will be de-weighted.')
        self.configs.add_param('trimnum', config_types.IntVal, \
                        help='The number of channels to de-weight at ' \
                            'each edge of the band.')
        self.configs.add_param('trimfrac', config_types.FloatVal, \
                        help='The fraction of each band-edge to ' \
                            'de-weight (a floating-point number between 0 and 0.5).')
        self.configs.add_param('trimbw', config_types.FloatVal, \
                        help='The bandwidth of each band-edge to ' \
                            'de-weight (in MHz).')
        self.configs.add_param('badsubints', config_types.IntOrIntPairList, \
                        nullable=True, \
                        help='Bad subints and/or (inclusive) subint-intervals ' \
                            'to de-weight. Note: Subints are indexed starting at 0.')
        self.configs.add_param('badchans', config_types.IntOrIntPairList, \
                        nullable=True, \
                        help='Bad channels and/or (inclusive) channel-intervals ' \
                            'to de-weight. Note: Channels are indexed starting at 0.')
        self.parse_config_string(config.cfg.rcvrstd_default_params)

    def _clean(self, ar):
        raise NotImplementedError("Cleaning method is not implemented!")
        pass

    def __prune_band_edges(self, ar):
        """Prune the edges of the band. This is useful for
            removing channels where there is no response.
            The file is modified in-place. However, zero-weighting 
            is used for pruning, so the process is reversible.
 
            Inputs:
                ar: The psrchive archive object to clean.

            Outputs:
                None
        """
        if self.configs.response is None:
            utils.print_info("No freq range specified for band pruning. Skipping...", 2)
        else:
            lofreq, hifreq = self.configs.response
            # Use absolute value in case band is flipped (BW<0)
            bw = ar.get_bandwidth()
            nchan = ar.get_nchan()
            chanbw = bw/nchan
            utils.print_info("Pruning frequency band to (%g-%g MHz)" % response, 2)
            # Loop over channels
            for ichan in xrange(nchan):
                # Get profile for subint=0, pol=0
                prof = ar.get_Profile(0, 0, ichan)
                freq = prof.get_centre_frequency()
                if (freq < lofreq) or (freq > hifreq):
                    clean_utils.zero_weight_chan(ar, ichan)

    def __trim_edge_channels(infn, nchan_to_trim=None, frac_to_trim=None):
        """Trim the edge channels of an input file to remove 
            band-pass roll-off and the effect of aliasing. 
            The file is modified in-place. However, zero-weighting 
            is used for trimming, so the process is reversible.

            Inputs:
                ar: The psrchive archive object to clean.

            Outputs:
                None
        """
        nchan = ar.get_nchan()
        bw = float(ar.get_bandwidth())
        num_to_trim = max(self.confgs.trimnum, \
                          int(self.configs.trimfrac*nchan+0.5), \
                          int(self.configs.trimbw/bw*nchan+0.5))
        if num_to_trim > 0:
            utils.print_info("Trimming %d channels from each band-edge." % \
                            num_to_trim, 2)
            for ichan in xrange(num_to_trim):
                clean_utils.zero_weight_chan(ar, ichan) # trim at beginning
                clean_utils.zero_weight_chan(ar, nchan-ichan-1) # trim at end

    def __remove_bad_subints(ar):
        """Zero-weights bad subints.
            The file is modified in-place. However, zero-weighting 
            is used for trimming, so the process is reversible.

            Inputs:
                ar: The psrchive archive object to clean.
        
            Outputs:
                None
        """
        for tozap in self.configs.badsubints:
            if type(tozap) is types.IntType:
                clean_utils.zero_weight_subint(ar, tozap)
            else:
                losubint, hisubint = tozap
                for xx in xrange(losubint, hisubint+1):
                    clean_utils.zero_weight_subint(ar, xx)

    def __remove_bad_channels(ar):
        """Zero-weight bad channels and channels containing bad
            frequencies. However, zero-weighting 
            is used for trimming, so the process is reversible.

            Inputs:
                ar: The psrchive archive object to clean.
        
            Outputs:
                None
        """
        if badchans is None:
            badchans = config.cfg.badchans
        if badchan_intervals is None:
            badchan_intervals = config.cfg.badchan_intervals
        if badfreqs is None:
            badfreqs = config.cfg.badfreqs
        if badfreq_intervals is None:
            badfreq_intervals = config.cfg.badfreq_intervals

        zaplets = []
        if badchans:
            zaplets.append("-z '%s'" % " ".join(['%d' % zz for zz in badchans]))
        if badchan_intervals:
            zaplets.extend(["-Z '%d %d'" % lohi for lohi in badchan_intervals])
        if badfreqs:
            zaplets.append("-f '%s'" % " ".join(['%f' % ff for ff in badfreqs]))
        if badfreq_intervals:
            zaplets.extend(["-F '%f %f'" % lohi for lohi in badfreq_intervals])

        if zaplets:
            utils.print_info("Removing bad channels.", 2)
            utils.execute("paz -m %s %s" % (" ".join(zaplets), infn.fn))

Cleaner = ReceiverBandCleaner
