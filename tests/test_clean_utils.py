"""Unit tests for coast_guard.clean_utils.

Only the pure-numeric helpers that do not require a live psrchive Archive are
tested here. Functions that take an ``ar`` (Archive) argument -- e.g.
``get_frequencies``, ``remove_profile_inplace``, ``write_psrsh_script`` -- need
psrchive and are covered by integration tests.
"""
import numpy as np
import numpy.testing as npt
import pytest

from coast_guard import clean_utils as cu


# ---------------------------------------------------------------------------
# fft_rotate
# ---------------------------------------------------------------------------
class TestFftRotate:
    def test_integer_shift_left(self):
        data = np.array([1., 2., 3., 4., 5., 6., 7., 8.])
        rotated = cu.fft_rotate(data, 2)
        expected = np.array([3., 4., 5., 6., 7., 8., 1., 2.])
        npt.assert_allclose(rotated, expected, atol=1e-9)

    def test_zero_shift_is_identity(self):
        # Even-length input (see test_odd_length_bug for the odd-length case).
        data = np.array([1., 5., 2., 8., 3., 4.])
        npt.assert_allclose(cu.fft_rotate(data, 0), data, atol=1e-9)

    def test_odd_length_integer_shift(self):
        # Previously fft_rotate used np.arange(size/2+1) (float division),
        # which produced the wrong phasor length for odd-size input. Now
        # fixed with floor division and an explicit irfft length.
        data = np.array([1., 5., 2., 8., 3.])  # length 5 (odd)
        rotated = cu.fft_rotate(data, 1)
        # Shift left by one bin.
        npt.assert_allclose(rotated, np.array([5., 2., 8., 3., 1.]), atol=1e-9)

    def test_odd_length_zero_shift_is_identity(self):
        data = np.array([1., 5., 2., 8., 3.])  # length 5 (odd)
        assert cu.fft_rotate(data, 0).size == data.size
        npt.assert_allclose(cu.fft_rotate(data, 0), data, atol=1e-9)

    def test_full_period_shift_is_identity(self):
        data = np.array([1., 2., 3., 4.])
        npt.assert_allclose(cu.fft_rotate(data, 4), data, atol=1e-9)

    def test_negative_shift_moves_right(self):
        data = np.array([1., 2., 3., 4.])
        npt.assert_allclose(cu.fft_rotate(data, -1),
                            np.array([4., 1., 2., 3.]), atol=1e-9)

    def test_preserves_sum(self):
        data = np.array([2., 4., 6., 8., 10., 12.])
        rotated = cu.fft_rotate(data, 2.5)
        npt.assert_allclose(np.sum(rotated), np.sum(data), atol=1e-9)


# ---------------------------------------------------------------------------
# apply_weights
# ---------------------------------------------------------------------------
class TestApplyWeights:
    def test_zeroes_masked_channels(self):
        data = np.ones((2, 3, 4))
        weights = np.array([[1, 0, 1], [0, 1, 0]])
        out = cu.apply_weights(data.copy(), weights)
        # Each bin-row sums to 4 where weight==1, else 0.
        npt.assert_array_equal(out.sum(axis=2),
                               np.array([[4., 0., 4.], [0., 4., 0.]]))

    def test_fractional_weights_scale(self):
        data = np.ones((1, 2, 3))
        weights = np.array([[0.5, 2.0]])
        out = cu.apply_weights(data.copy(), weights)
        npt.assert_allclose(out[0, 0], np.full(3, 0.5))
        npt.assert_allclose(out[0, 1], np.full(3, 2.0))

    def test_shape_preserved(self):
        data = np.random.rand(3, 5, 7)
        weights = np.ones((3, 5))
        out = cu.apply_weights(data.copy(), weights)
        assert out.shape == (3, 5, 7)


# ---------------------------------------------------------------------------
# get_profile
# ---------------------------------------------------------------------------
class TestGetProfile:
    def test_sums_over_axis0(self):
        data = np.array([[1., 2., 3.], [4., 5., 6.]])
        npt.assert_array_equal(cu.get_profile(data), np.array([5., 7., 9.]))


# ---------------------------------------------------------------------------
# scale_chans
# ---------------------------------------------------------------------------
class TestScaleChans:
    def test_single_subband_subtracts_median(self):
        data = np.array([10., 12., 14., 100.])
        # median is 13.0, so result = data - 13
        out = cu.scale_chans(data, nchans=4)
        npt.assert_allclose(out, np.array([-3., -1., 1., 87.]))

    def test_masked_channels_set_to_zero(self):
        data = np.array([10., 12., 14., 100.])
        weights = np.array([1, 1, 1, 0], dtype=bool)
        out = cu.scale_chans(data, nchans=4, chanweights=weights)
        # median of unmasked [10,12,14] == 12, masked entry forced to 0
        npt.assert_allclose(out[:3], np.array([-2., 0., 2.]))
        assert out[3] == 0.0

    def test_multiple_subbands(self):
        data = np.array([1., 3., 100., 102.])
        out = cu.scale_chans(data, nchans=2)
        # subband0 median 2 -> [-1,1]; subband1 median 101 -> [-1,1]
        npt.assert_allclose(out, np.array([-1., 1., -1., 1.]))


# ---------------------------------------------------------------------------
# scale_subints
# ---------------------------------------------------------------------------
class TestScaleSubints:
    def test_kernel_size_one_subtracts_self(self):
        data = np.array([1., 2., 3., 4., 5.])
        # kernel_size=1 -> only neighbour is the point itself -> all zeros
        npt.assert_allclose(cu.scale_subints(data, kernel_size=1),
                            np.zeros(5))

    def test_constant_data_gives_zeros(self):
        data = np.full(6, 7.0)
        npt.assert_allclose(cu.scale_subints(data, kernel_size=3),
                            np.zeros(6))

    def test_output_length_matches_input(self):
        data = np.arange(10, dtype=float)
        assert len(cu.scale_subints(data, kernel_size=5)) == 10


# ---------------------------------------------------------------------------
# get_robust_std
# ---------------------------------------------------------------------------
class TestGetRobustStd:
    def test_matches_mad_formula(self):
        data = np.arange(11, dtype=float)  # 0..10, median 5
        weights = np.ones(11, dtype=bool)
        # MAD of 0..10 about median 5 is 3, robust std = 1.4826*3
        npt.assert_allclose(cu.get_robust_std(data, weights), 1.4826 * 3.0)

    def test_respects_weights(self):
        data = np.array([0., 1., 2., 3., 1000.])
        weights = np.array([1, 1, 1, 1, 0], dtype=bool)
        # Outlier masked out; median of [0,1,2,3]=1.5, |dev|=[1.5,.5,.5,1.5]
        # MAD = median([1.5,.5,.5,1.5]) = 1.0
        npt.assert_allclose(cu.get_robust_std(data, weights), 1.4826)


# ---------------------------------------------------------------------------
# fit_poly
# ---------------------------------------------------------------------------
class TestFitPoly:
    def test_linear_recovers_coefficients(self):
        x = np.arange(5, dtype=float)
        y = 2.0 * x + 3.0
        coeffs, poly = cu.fit_poly(np.ma.asarray(y), np.ma.asarray(x), order=1)
        # coeffs are [intercept, slope]
        npt.assert_allclose(coeffs, np.array([3.0, 2.0]), atol=1e-9)
        npt.assert_allclose(poly, y, atol=1e-9)

    def test_quadratic_recovers_coefficients(self):
        x = np.arange(6, dtype=float)
        y = 1.0 + 2.0 * x + 3.0 * x ** 2
        coeffs, poly = cu.fit_poly(np.ma.asarray(y), np.ma.asarray(x), order=2)
        npt.assert_allclose(coeffs, np.array([1.0, 2.0, 3.0]), atol=1e-6)

    def test_all_masked_raises(self):
        y = np.ma.masked_all(4)
        x = np.ma.asarray(np.arange(4, dtype=float))
        with pytest.raises(ValueError):
            cu.fit_poly(y, x, order=1)


# ---------------------------------------------------------------------------
# detrend
# ---------------------------------------------------------------------------
class TestDetrend:
    def test_removes_linear_trend(self):
        y = np.arange(20, dtype=float) * 3.0 + 5.0
        detrended = cu.detrend(y, order=1)
        npt.assert_allclose(detrended, np.zeros(20), atol=1e-9)

    def test_flat_input_unchanged(self):
        y = np.full(10, 4.0)
        detrended = cu.detrend(y, order=1)
        npt.assert_allclose(detrended, np.zeros(10), atol=1e-9)

    def test_returns_plain_array_for_plain_input(self):
        y = np.arange(10, dtype=float)
        out = cu.detrend(y, order=1)
        assert not np.ma.isMaskedArray(out)

    def test_returns_masked_for_masked_input(self):
        y = np.ma.masked_array(np.arange(10, dtype=float),
                               mask=np.zeros(10, dtype=bool))
        out = cu.detrend(y, order=1)
        assert np.ma.isMaskedArray(out)


# ---------------------------------------------------------------------------
# iterative_detrend
# ---------------------------------------------------------------------------
class TestIterativeDetrend:
    def test_all_masked_returns_masked(self):
        y = np.ma.masked_all(5)
        out = cu.iterative_detrend(y)
        assert np.ma.count(out) == 0

    def test_linear_trend_detrended(self):
        y = np.ma.asarray(np.arange(30, dtype=float) * 2.0 + 1.0)
        out = cu.iterative_detrend(y, order=1)
        # The bulk of the (masked) result should be near zero.
        npt.assert_allclose(out.filled(0.0), np.zeros(30), atol=1e-6)


# ---------------------------------------------------------------------------
# channel_scaler / subint_scaler
# ---------------------------------------------------------------------------
class TestScalers:
    def test_channel_scaler_shape(self):
        arr = np.random.RandomState(0).normal(size=(8, 4))
        scaled = cu.channel_scaler(arr, chan_order=[1], chan_breakpoints=None,
                                   chan_numpieces=None)
        assert scaled.shape == arr.shape

    def test_subint_scaler_shape(self):
        arr = np.random.RandomState(0).normal(size=(8, 4))
        scaled = cu.subint_scaler(arr, subint_order=[1], subint_breakpoints=None,
                                  subint_numpieces=None)
        assert scaled.shape == arr.shape

    def test_channel_scaler_centres_data(self):
        # A clean linear channel should scale to roughly zero-median.
        arr = np.zeros((10, 1))
        arr[:, 0] = np.arange(10) * 1.0
        scaled = cu.channel_scaler(arr, chan_order=[1], chan_breakpoints=None,
                                   chan_numpieces=None)
        assert np.isfinite(np.ma.median(scaled))


# ---------------------------------------------------------------------------
# comprehensive_stats
# ---------------------------------------------------------------------------
class TestComprehensiveStats:
    def test_output_shape(self):
        data = np.random.RandomState(1).normal(size=(4, 6, 16))
        res = cu.comprehensive_stats(data, axis=2, chanthresh=5, subintthresh=5)
        assert res.shape == (4, 6)

    def test_scores_are_nonnegative_and_finite(self):
        # Scores are built from absolute, scaled diagnostics, so they must be
        # non-negative and finite for well-behaved input.
        data = np.random.RandomState(2).normal(size=(5, 6, 32))
        res = cu.comprehensive_stats(data, axis=2, chanthresh=5, subintthresh=5)
        assert np.all(res >= 0)
        assert np.all(np.isfinite(res))

    def test_deterministic(self):
        data = np.random.RandomState(9).normal(size=(4, 5, 16))
        r1 = cu.comprehensive_stats(data, axis=2, chanthresh=5, subintthresh=5)
        r2 = cu.comprehensive_stats(data, axis=2, chanthresh=5, subintthresh=5)
        npt.assert_array_equal(r1, r2)

    def test_aggressive_uses_max_and_is_geq_average(self):
        data = np.random.RandomState(3).normal(size=(3, 5, 16))
        avg = cu.comprehensive_stats(data, axis=2, chanthresh=5, subintthresh=5,
                                     aggressive=False)
        agg = cu.comprehensive_stats(data, axis=2, chanthresh=5, subintthresh=5,
                                     aggressive=True)
        # max over diagnostics >= mean over diagnostics, elementwise.
        assert np.all(agg + 1e-9 >= avg)


# ---------------------------------------------------------------------------
# subint_scaler: piecewise-MAD scaling (frequency direction)
# ---------------------------------------------------------------------------
class TestSubintScalerBackwardCompat:
    """With the flag off (or a single MAD segment) the piecewise path must
    reduce EXACTLY to the original global-MAD behaviour."""

    _KW = dict(subint_order=[1], subint_breakpoints=[[]], subint_numpieces=[1])

    def test_flag_off_matches_global(self):
        rng = np.random.default_rng(7)
        arr = np.ma.masked_array(rng.normal(size=(4, 100)))
        base = cu.subint_scaler(arr, piecewise_scale=False, **self._KW)
        # Flag on but only one MAD segment -> identical to global.
        same = cu.subint_scaler(arr, piecewise_scale=True,
                                subint_mad_numpieces=1, **self._KW)
        npt.assert_array_equal(np.ma.getdata(base), np.ma.getdata(same))
        # Flag on, mad numpieces defaults to subint_numpieces[-1] == 1.
        same2 = cu.subint_scaler(arr, piecewise_scale=True, **self._KW)
        npt.assert_array_equal(np.ma.getdata(base), np.ma.getdata(same2))

    def test_default_is_global(self):
        # No piecewise kwargs at all -> original behaviour, reproduced by hand.
        rng = np.random.default_rng(8)
        arr = np.ma.masked_array(rng.normal(size=(2, 50)))
        kw = dict(subint_order=[1], subint_breakpoints=[[]], subint_numpieces=[2])
        out = cu.subint_scaler(arr, **kw)
        exp = np.empty_like(arr)
        for i in range(arr.shape[0]):
            d = cu.iterative_detrend(arr[i, :], order=1, bp=[], numpieces=2)
            med = np.ma.median(d)
            mad = np.ma.median(np.abs(d - med))
            exp[i, :] = (d - med) / (mad * 1.4826)
        npt.assert_array_equal(np.ma.getdata(out), np.ma.getdata(exp))


class TestSubintScalerPiecewise:
    """Piecewise MAD keeps the 'sigma' definition local in frequency so a
    Tsys gradient does not inflate high-Tsys channels' significance."""

    _KW = dict(subint_order=[1], subint_breakpoints=[[]], subint_numpieces=[1])

    @staticmethod
    def _robust_spread(x):
        x = np.asarray(x, dtype=float).ravel()
        med = np.median(x)
        return 1.4826 * np.median(np.abs(x - med))

    def _tsys_ramp(self, nsub=4, nchan=512, seed=0):
        # Per-channel off-pulse scatter ramps x5 across the band (1 -> 5).
        rng = np.random.default_rng(seed)
        sigma = 1.0 + 4.0 * np.arange(nchan) / (nchan - 1)
        noise = rng.normal(size=(nsub, nchan)) * sigma[None, :]
        return np.ma.masked_array(noise), sigma

    def test_global_overflags_high_tsys_piecewise_balances(self):
        arr, _ = self._tsys_ramp()
        g = np.ma.getdata(cu.subint_scaler(arr, piecewise_scale=False, **self._KW))
        p = np.ma.getdata(cu.subint_scaler(arr, piecewise_scale=True,
                                           subint_mad_numpieces=8, **self._KW))
        low, high = slice(0, 64), slice(448, 512)
        # Global MAD: high-Tsys channels have a much larger scaled spread ->
        # they exceed any fixed threshold "just because" the noise is higher.
        g_ratio = self._robust_spread(g[:, high]) / self._robust_spread(g[:, low])
        assert g_ratio > 3.0
        # Piecewise MAD: spread ~1 in both sub-bands (balanced, no over-flag).
        p_low = self._robust_spread(p[:, low])
        p_high = self._robust_spread(p[:, high])
        assert 0.6 < (p_high / p_low) < 1.7
        npt.assert_allclose(p_low, 1.0, atol=0.35)
        npt.assert_allclose(p_high, 1.0, atol=0.35)

    def test_equal_excess_outlier_judged_locally(self):
        # Two spikes of EQUAL absolute excess, one in a low-Tsys sub-band
        # (a real, locally-significant outlier) and one in a high-Tsys
        # sub-band (within the local noise).
        arr, _ = self._tsys_ramp(seed=1)
        delta = 18.0
        lo_chan, hi_chan = 32, 480
        arr[0, lo_chan] += delta
        arr[0, hi_chan] += delta
        g = np.ma.getdata(cu.subint_scaler(arr, piecewise_scale=False, **self._KW))
        p = np.ma.getdata(cu.subint_scaler(arr, piecewise_scale=True,
                                           subint_mad_numpieces=8, **self._KW))
        # Global MAD gives the two equal-excess spikes near-equal significance
        # (it cannot tell the real RFI from the high-Tsys noise excursion).
        assert abs(g[0, lo_chan]) > 5 and abs(g[0, hi_chan]) > 5
        assert 0.6 < abs(g[0, hi_chan]) / abs(g[0, lo_chan]) < 1.7
        # Piecewise MAD: the low-Tsys spike is a strong local outlier; the
        # high-Tsys one sits within its sub-band's noise.
        assert abs(p[0, lo_chan]) > 8
        assert abs(p[0, hi_chan]) < 5
        assert abs(p[0, lo_chan]) > 2 * abs(p[0, hi_chan])


class TestPiecewiseMadEdgeCases:
    """Sparse, all-masked and zero-MAD segments must fall back gracefully."""

    def test_sparse_masked_and_degenerate_segments(self):
        # 4 MAD segments of 16 channels each:
        #   seg0 fully masked, seg1 all-equal (MAD==0), seg2 <16 unmasked,
        #   seg3 normal noise.
        rng = np.random.default_rng(4)
        nchan = 64
        row = rng.normal(size=nchan)
        row[16:32] = 5.0  # constant segment -> local MAD 0
        arr = np.ma.masked_array(row.reshape(1, nchan),
                                 mask=np.zeros((1, nchan), dtype=bool))
        arr.mask[0, 0:16] = True    # fully-masked segment
        arr.mask[0, 32:34] = True   # leaves 14 unmasked in seg2 (<16)
        out = cu.subint_scaler(arr, subint_order=[0], subint_breakpoints=[[]],
                               subint_numpieces=[1], piecewise_scale=True,
                               subint_mad_numpieces=4)
        # No NaN/Inf anywhere, even in the fallback segments.
        assert np.all(np.isfinite(np.ma.filled(out, 0.0)))
        # Masked input entries stay masked in the output.
        assert np.all(out.mask[0, 0:16])
        assert np.all(out.mask[0, 32:34])

    def test_helper_matches_global_for_one_piece(self):
        # _piecewise_mad_scale with numpieces=1 == a single global MAD scale.
        rng = np.random.default_rng(5)
        d = np.ma.masked_array(rng.normal(size=200))
        med = np.ma.median(d)
        mad = np.ma.median(np.abs(d - med))
        expected = (d - med) / (mad * 1.4826)
        got = cu._piecewise_mad_scale(d, numpieces=1)
        npt.assert_allclose(np.ma.getdata(got), np.ma.getdata(expected), atol=1e-12)


class TestPiecewiseMadIntegration:
    """End-to-end check on a real wideband archive (needs psrchive + data)."""

    def test_uwl_tsys_gradient(self):
        pytest.skip(
            "Integration test: requires psrchive and a real UWL archive with a "
            "strong Tsys gradient. Run the surgical cleaner with "
            "piecewise_scale=True and assert the zapped-channel fraction in "
            "high-Tsys sub-bands drops to that of low-Tsys sub-bands, while "
            "injected narrowband RFI is still removed.")
