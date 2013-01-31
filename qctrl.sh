#!/bin/bash

mkdir -p done

for fn in $* ; do
    echo ${fn}
    psfn="${fn}.scrunched.ps.gz"

    # Display the plot
    echo "Displaying ${psfn}"
    gv ${psfn} &
    pid=$!

    # Get receiver code and interpret it
    read -N 1 -s -p "Good? Y/N> " GOOD
    case $GOOD in
    "y")
        echo "${fn} is good"
        mv ${fn}* done/
        ;;
    "Y")
        echo "${fn} is good"
        mv ${fn}* done/
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
    kill $pid
    shift
done
