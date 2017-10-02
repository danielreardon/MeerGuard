#!/bin/bash

pamcalls=$1
echo "# $(date)" >> $pamcalls
shift

while (( "$#" )); do
    # Set up filenames
    png=$1
    fn=$(basename ${png} .png)
    
    # Display the plot
    echo "Displaying ${png}"
    display ${png} &
    
    # Get receiver code and interpret it
    read -N 1 -s -p "Type receiver code> " RCVR
    case $RCVR in
    "l")
        echo ${fn} "LBAND"
        echo pam -e rcvr --receiver lband_rcvr.txt --inst Asterix ${fn} >> $pamcalls
        ;;
    "s")
        echo ${fn} "SBAND"
        echo pam -e rcvr --receiver sband_rcvr.txt --inst Asterix ${fn} >> $pamcalls
        ;;
    "7")
        echo ${fn} "7BEAM"
        echo pam -e rcvr --receiver 7beam_rcvr.txt --inst Asterix ${fn} >> $pamcalls
        ;;
    *)
        echo "Unrecognized selection ${RCVR}. You suck" 1>&2
        echo "# Unrecognized selection (${RCVR}) for ${fn}"
    esac
    
    # Clean up and prepare for next interation of loop
    kill $!
    shift
done
