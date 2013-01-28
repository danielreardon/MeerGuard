#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo "Minimum two command line arguments required!" 1>&2
    echo "USAGE: $0 <BASE DIGEST DIR> <INFILE> [<INFILE> ...]" 1>&2
    exit 1
fi

digestbasedir=$1

shift

while (( "$#" )); do
    fn=$1
    outdir=${digestbasedir}/$(get_outfn.py "%(name)s/%(rcvr)s/" ${fn})
    echo "Filename: ${fn}"
    echo "Output directory: ${outdir}"
    mkdir -p ${outdir}
    if [ -f ${outdir}/$(echo ${fn} | sed -e 's/[^.]*$/DTFp/') ]; then
        echo "Skipping ${fn}"
    else
        echo "Digesting ${fn} into ${outdir}"
        pam -u ${outdir} -e DTFp -T -F -p -D ${fn}
    fi
    shift

done
