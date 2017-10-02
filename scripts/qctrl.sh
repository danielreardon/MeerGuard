#!/bin/bash

GOODLIST=goodfiles.txt
#mkdir -p done

plotext='.ps.gz'
dispprog='gv -geometry +0+0'

eval set -- $(getopt -s bash 'e:D:h' "$@")

while true; do
    case "$1" in
        -e ) plotext="$2"
             shift; shift ;;
        -D ) dispprog="$2"
             shift; shift ;;
        -h ) help=1
             shift;;
        -- ) shift
             break;;
    esac
done

if [[ ${help} || ($# -lt 2) ]]; then
    echo "" 1>&2
    echo "Perform quality control on archive files."
    echo "" 1>&2
    echo "USAGE: $0 [OPTIONS] -- FILE [FILE ...]" 1>2&
    echo "" 1>&2
    echo "-e <plot ext>     Extension to be added to archive file names to " 1>&2
    echo "                  get the diagnostic plot filenames that should " 1>&2
    echo "                  be displayed." 1>&2
    echo "                  (Default: '.ps.gz')" 1>&2
    echo "-D                Program to use to display diagnostic plots. " 1>&2
    echo "                  (Default: gv -geometry +0+0)" 1>&2
    echo "-h                Display this help" 1>&2
    echo "" 1>&2
    exit 1
fi

for fn in $* ; do
    imgfn="${fn}${plotext}"
    
    # Check if this file has already passed
    if grep -Fq ${fn} ${GOODLIST} &>/dev/null; then
        echo "${fn} already marked as good"
        continue
    fi
    echo "Checking ${fn}"
    echo "${dispprog} ${imgfn}"

    # Display the plot
    echo "Displaying ${imgfn}"
    ${dispprog} ${imgfn} &>/dev/null &
    pid=$!

    # Get receiver code and interpret it
    read -N 1 -s -p "Good? Y/N> " GOOD
    case $GOOD in
    "y")
        echo "${fn} is good"
        # mv ${fn}* done/
        readlink -f ${fn} >> ${GOODLIST}
        ;;
    "Y")
        echo "${fn} is good"
        # mv ${fn}* done/
        readlink -f ${fn} >> ${GOODLIST}
        ;;
    "n")
        echo "${fn} still needs work"
        ;;
    "N")
        echo "${fn} still needs work"
        ;;
    *)
        echo "Unrecognized selection ${GOOD}." 1>&2
    esac
    
    # Clean up and prepare for next interation of loop
    kill $pid &> /dev/null
    shift
done
