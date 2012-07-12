#!/bin/bash

if [[ $# -lt 3 ]]; then
    echo "Minimum three command line arguments required!" 1>&2
    echo "USAGE: $0 <OUTFILE> <INFILE> <INFILE>"
    exit 1
fi

outfn=$1
echo $outfn
shift

if [ -f $outfn ]; then
    echo "ERROR: ${outfn} already exists. Will not overwrite! Exiting." 1>&2
    exit 2
fi

# Sort input files by snr
first=1
temp=$(tempfile -s .tmp)
for fn in $(psrstat -Q -j DFTp -c snr * | sort -n -k2 -r | cut -f 1 -d ' '); do
    echo "Working on ${fn}"
    if [ $first -ne 0 ]; then
        echo "Oooo our first file..."
        first=0
        cp ${fn} ${outfn}
    else
        cp ${outfn} ${temp} # Make a backup of our progress thus far
        psradd -j TDFp -F -ip -P -o ${temp} -T ${outfn} ${fn}
        snrtmp=$(psrstat -Q -q -j DFTp -c snr ${temp})
        echo "snrtmp: ${snrtmp}; snradd: ${snradd}"
        isbetter=$(echo "${snrtmp} > ${snradd}" | bc -l)
        if [ $isbetter -eq 1 ]; then
            echo "SNR has improved from ${snradd} to ${snrtmp}"
            mv ${temp} ${outfn}
        else
            echo "SNR has decreased from ${snradd} to ${snrtmp}"
            psrplot -p flux -D ${fn}.cmp.ps/PS -N 1x3 -c ch=3 ${temp} ${outfn} ${fn}
            rm ${temp}
        fi
    fi
    echo "-------------"
    snradd=$(psrstat -Q -q -j DFTp -c snr ${outfn})
done

