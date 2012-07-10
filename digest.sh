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
    echo "Digesting ${fn} into ${outdir}"
    mkdir -p ${outdir}
    pam -u ${outdir} -e DTFp -T -F -p -D ${fn}
    
    shift

done
