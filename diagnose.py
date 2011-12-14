#!/usr/bin/env python

"""
Given a PSRCHIVE archive create diagnostic plots.

Patrick Lazarus, Dec. 12, 2011
"""

import sys

import numpy as np
import matplotlib.cm
import matplotlib.pyplot as plt

import psrchive

def plot(ar, outname):
    """Plot.

        Inputs:
            ar: The archive to make the plot for.
            outname: The file to save the plot to.

        Outputs:
            None
    """
    clone = ar.clone()
    clone.set_dispersion_measure(0)
    clone.dedisperse()
    clone.pscrunch()
    clone.fscrunch()
    nsubs = clone.get_nsubint()
    data = clone.get_data().squeeze()

    for ii in np.arange(nsubs):
        data[ii,:]/=np.median(data[ii,:])

    plt.figure()
    plt.axes([0.7,0.1,0.2,0.8])
    plt.plot(data.std(axis=1), np.arange(nsubs), label='std dev')
    plt.plot(data.ptp(axis=1), np.arange(nsubs), label='max-min')
    plt.axis('tight')
    
    plt.axes([0.1,0.1,0.6,0.8])
    plt.imshow(data, origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.legend(loc='best')
    plt.ylabel('subint number')
    plt.xlabel('bin number')
    
    clone = ar.clone()
    clone.dedisperse()
    clone.pscrunch()
    clone.tscrunch()
    nchans = clone.get_nchan()
    data = clone.get_data().squeeze()

    data_toplot = []
    channums_toplot = []
    for ii in np.arange(nchans):
        median = np.median(data[ii,:])
        if median:
            data_toplot.append(data[ii,:]/median)
            channums_toplot.append(ii)

    data_toplot = np.asarray(data_toplot)

    print nchans, data.shape
    plt.figure()
    plt.axes([0.7,0.1,0.2,0.8])
    plt.plot(data_toplot.std(axis=1), channums_toplot, label='std dev')
    plt.plot(data_toplot.ptp(axis=1), channums_toplot, label='max-min')
    plt.axis('tight')
    
    plt.axes([0.1,0.1,0.6,0.8])
    plt.imshow(data, origin='bottom', aspect='auto', \
                cmap=matplotlib.cm.gist_heat, interpolation='nearest')
    plt.legend(loc='best')
    plt.ylabel('chan number')
    plt.xlabel('bin number')
    plt.show()


def main():
    ar = psrchive.Archive_load(sys.argv[1])
    plot(ar, 'bogusname.png')


if __name__ == '__main__':
    main()
