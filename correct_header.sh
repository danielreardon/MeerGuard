#!/bin/bash
pamcalls=$1

echo "# $(date)" >> $pamcalls

for png in $(ls *.ar.png); do
    fn=$(basename ${png} .png)
    display ${png} &
    read -N 1 -s -p "Type receiver code> " RCVR
    case $RCVR in
    "l")
        echo ${fn} "LBAND"
        echo pam -m --receiver lband.rcvr --inst Asterix ${fn} >> $pamcalls
        ;;
    "s")
        echo ${fn} "SBAND"
        echo pam -m --receiver sband.rcvr --inst Asterix ${fn} >> $pamcalls
        ;;
    "7")
        echo ${fn} "7BEAM"
        echo pam -m --receiver 7beam.rcvr --inst Asterix ${fn} >> $pamcalls
        ;;
    *)
        echo "Unrecognized selection ${RCVR}. You suck" 1>&2
    esac
    kill $!
done
