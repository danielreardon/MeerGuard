#!/usr/bin/env python

import matplotlib.pyplot as plt

from coast_guard import config
from coast_guard import database
from coast_guard import utils

import numpy as np
from math import isinf

def get_files(psrname):
    """Get a list of database rows the given pulsar.

        Inputs:
            psrname: The name of the pulsar to match.

        Outputs:
            rows: A list of rows containing file and obs
                information for each matching file.
    """
    db = database.Database()
    print "Before", psrname
    psrname = utils.get_prefname(psrname)
    print "After", psrname
   

    whereclause = (db.obs.c.sourcename == psrname) & \
                  (db.files.c.stage == 'cleaned') & \
                  (db.obs.c.obstype == 'pulsar')
    with db.transaction() as conn:
        select = db.select([db.files,
                            db.obs.c.sourcename,
                            db.obs.c.start_mjd,
                            db.obs.c.rcvr],
                    from_obj=[db.files.\
                            outerjoin(db.obs,
                                onclause=(db.files.c.obs_id ==
                                            db.obs.c.obs_id))]).\
                        where(whereclause).\
                        order_by(db.files.c.added.asc())

        result = conn.execute(select)
        rows = result.fetchall()
        result.close()
    return rows

       

def main():
    #psrnames = open(args.psrfile)   

    psrname = args.psrname
    if True:
	rows = get_files(psrname)  
     
        data = {'detect-L': ([], []),
                'RFI-L': ([], []),
                'non-detect-L': ([], []),
    	        'detect-S': ([], []),
                'RFI-S': ([], []),
                'non-detect-S': ([], [])}
        for row in rows:
	    if row['rcvr'] == 'S110-1':
	        rx = "-S"
            else:
                rx = "-L"
            if row['qcpassed']:
                if row['snr']:
                    data['detect'+rx][0].append(row['start_mjd'])
                    data['detect'+rx][1].append(row['snr'])
            else:
                if (row['note'] is not None) and ('RFI' in row['note']):
                    data['RFI'+rx][0].append(row['start_mjd'])
                    data['RFI'+rx][1].append(0)
                else:
                    data['non-detect'+rx][0].append(row['start_mjd'])
                    data['non-detect'+rx][1].append(0)
                
        plt.figure(1)
        for ii, rx in enumerate(['-L', '-S'], 1):
	    plt.subplot(2,1,ii)
	    plt.title("%s-band" % (rx[1]))
            plt.scatter(data['detect'+rx][0], data['detect'+rx][1], marker='o', c='k')
            plt.scatter(data['RFI'+rx][0], data['RFI'+rx][1], marker='x', c='r')
            plt.scatter(data['non-detect'+rx][0], data['non-detect'+rx][1], marker='o', facecolors='none')
	    plt.xlabel("MJD")
 	    plt.ylabel("snr")
	    if data['detect'+rx][1]:
	    	print "Detections", data['detect'+rx][1], "\n", max(data['detect'+rx][1])
		plt.ylim(0 - 0.1*max(data['detect'+rx][1]),  max(data['detect'+rx][1]) + 0.1*max(data['detect'+rx][1]) )
		plt.xlim(55564, max(data['detect'+rx][0]) + 100)
	    else:
		print "Detections", data['detect'+rx][1], "\n"
		plt.ylim(-4,100)
		plt.xlim(55564, 57023)
        plt.suptitle(psrname, fontsize=14)
#     	plt.show(1)
	plt.figure(2)
	for ii, rx in enumerate(['-L', '-S'], 1):
	    if data['detect'+rx][1] + data['RFI'+rx][1] + data['non-detect'+rx][1]:
            	plt.subplot(2,2,ii)
            	plt.title("%s-band" % (rx[1]))
            	plt.hist(data['detect'+rx][1] + data['RFI'+rx][1] + data['non-detect'+rx][1],15)
	    	plt.xlabel("snr")
	    	print "combineddata", data['detect'+rx][1] + data['RFI'+rx][1] + data['non-detect'+rx][1]
#	    	logdata = np.log10(data['detect'+rx][1] + data['RFI'+rx][1] + data['non-detect'+rx][1])
#	    	print logdata	    
#	    	for i in range(len(logdata)):
#			if isinf(logdata[i]):
#				print logdata[i]
#				logdata[i] = 0.1
#				print "After", logdata[i]		

#	    	print np.log10(logdata)
	    	plt.subplot(2,2,ii+2)
	    	print "plot number", ii+2
		hist, bins = np.histogram(data['detect'+rx][1] + data['RFI'+rx][1] + data['non-detect'+rx][1],15)
		print "hist", hist
	    	percentage = (100.0 * np.array(hist[:]))/np.array(sum(hist[:]))
	    	widths = np.diff(bins)
	    	print "snr bins", bins[:-1], "\n", "percentage", percentage, "\n", widths
#	    	snrbins = pow(10,bins)
	    	plt.bar(bins[:-1], percentage, widths) 
	    	plt.xlabel("snr") 
	    	plt.ylabel("percentage (%)")

	    else:
		plt.title("%s-band" % (rx[1]))
		plt.subplot(2,2,ii)
		plt.scatter(data['detect'+rx][0], data['detect'+rx][1], marker='o', c='k')
   		plt.ylim(0,30)
                plt.xlim(0,20)
		plt.subplot(2,2,ii+2)
		plt.scatter(data['detect'+rx][0], data['detect'+rx][1], marker='o', c='k')
                plt.ylim(0,30)
                plt.xlim(0,20)

	plt.suptitle(psrname, fontsize=14)
   	plt.show()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description="Check detections for a pulsar.")
    parser.add_argument('-p', '--psr', dest='psrname', type=str,
                        required=True,
                        help='Name of the pulsar to check.')
#    parser.add_argument('--file', dest='psrlist', type=str,
#                        required=True,
#                        help='sfsfdf')
    parser.add_argument('--files', dest='psrfile', type=str, 
			 help="gfger")   
    args = parser.parse_args()
    main()             
