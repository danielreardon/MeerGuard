#!/bin/bash

for fn in $*; do 
    nsub=$(vap -nc nsub ${fn} | awk '{print $NF}')
    preproc='C,D,B 128,F 32'
    if [[ ${nsub} -gt 32 ]]; then 
        preproc="$preproc,T 32"
    fi
    if [ ! -f ${fn}.scrunched.ps.gz ]; then 
        composite_plot.py -j "${preproc}" -o ${fn}.scrunched.ps ${fn}
        gzip --best ${fn}.scrunched.ps
    fi
    if [ ! -f ${fn}.ps.gz ]; then 
        composite_plot.py -o ${fn}.ps ${fn}
        gzip --best ${fn}.ps
    fi
done
