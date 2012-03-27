#!/bin/bash

RCVR_DIR="/homes/plazarus/research/pulsar-code/coast_guard/rcvr_files"

echo "# $(date)"
while (( "$#" )); do
    fn=$1
    echo "# ${fn}"
    if [[ $(psrstat -q -c '{$freq>2000}' ${fn}) -eq 1 ]]; then 
        echo pam -e rcvr --receiver ${RCVR_DIR}/sband_rcvr.txt --inst Asterix ${fn}
    else
        psrstat -j DTp -c all:rms -l chan=2-13,34- ${fn} | grep -v 'rms=0$' | awk '
            BEGIN {
                bot=0;top=0
            } 
            {
                split($2, chan, "=")
                split($3, rms, "=")
                (chan[2]<14) ? bot += rms[2] : top += rms[2]
            } 
            END {
                x=top/6.0/bot
                if (x > 5) 
                    printf("pam -e rcvr --receiver '${RCVR_DIR}'/lband_rcvr.txt --inst Asterix %s\n", $1)
                else if (x < 2) 
                    printf("pam -e rcvr --receiver '${RCVR_DIR}'/7beam_rcvr.txt --inst Asterix %s\n", $1)
                else 
                    printf("# Cannot guess reciever (score: %f). Manual inspection required.", x)
            }'
    fi
    shift
done
