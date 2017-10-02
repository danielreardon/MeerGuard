#!/bin/bash

if [[ $# -lt 3 ]]; then
    echo "Minimum three command line arguments required! (Only $# provided)" 1>&2
    echo "USAGE: $0 <OUTFILE> <INFILE> <INFILE>"
    exit 1
fi

ALWAYS_ADD=1

outfn=$1
echo "Output file name: $outfn"
shift

if [ -f $outfn ]; then
    echo "ERROR: ${outfn} already exists. Will not overwrite! Exiting." 1>&2
    exit 2
fi

# Sort input files by snr
first=1
temp=$(tempfile -s .tmp)
toadd=$(tempfile -s .toadd)
for fn in $(psrstat -Q -j DFTp -c snr $@ | sort -n -k2 -r | cut -f 1 -d ' '); do
    echo "Working on ${fn}"
    if [ $first -ne 0 ]; then
        echo "Oooo our first file..."
        first=0
        cp ${fn} ${outfn}
        # Normalise source name
        currname=$(vap -n -c name ${fn} | awk '{print $2}')
        prefname=$(get_prefname.py ${currname})
        pam -DFTp -m --name ${prefname} ${outfn}
        snradd=$(psrstat -Q -q -j DFTp -c snr ${outfn})
    else
        # Copy input file and normalise source name
        cp ${fn} ${toadd}
        currname=$(vap -n -c name ${fn} | awk '{print $2}')
        prefname=$(get_prefname.py ${currname})
        pam -m --name ${prefname} ${toadd}
        # Add to temp file
        phs_offset=$(pat -TF -R -s ${outfn} ${toadd} | awk '{print $4}')
        echo "Phase offset: ${phs_offset}"
        cp ${toadd} ${temp}
        pam -m -r ${phs_offset} ${temp}
        psradd -F -ip -T -o ${temp} ${temp} ${outfn}
        # Get SNR of temp file
        snrtmp=$(psrstat -Q -q -j DFTp -c snr ${temp})
        echo "snrtmp: ${snrtmp}; snradd: ${snradd}"
        # Compare with old added file
        if [ $ALWAYS_ADD -eq 1]; then
            isbetter=1
        else
            isbetter=$(echo "${snrtmp} > ${snradd}" | bc -l)
        fi
        if [ $isbetter -eq 1 ]; then
            # If SNR improved keep temp and our added file
            echo "SNR has improved from ${snradd} to ${snrtmp}"
            mv ${temp} ${outfn}
            snradd=$(psrstat -Q -q -j DFTp -c snr ${outfn})
        else
            # If SNR has decreased throw out temp and move to next input file
            echo "SNR has decreased from ${snradd} to ${snrtmp}"
            psrplot -p flux -D ${fn}.cmp.ps/PS -N 1x3 -c ch=3 ${temp} ${outfn} ${toadd}
            rm ${temp}
        fi
    fi
    echo "-------------"
done

