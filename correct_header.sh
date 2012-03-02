#!/bin/bash
for png in $(ls *.ar.png); do
    fn=$(basename ${png} .png)
    display ${png} &
    read -N 1 -s -p "Type receiver code> " RCVR
    case $RCVR in
    "l")
        echo ${fn} "LBAND"
        ;;
    "s")
        echo ${fn} "SBAND"
        ;;
    "7")
        echo ${fn} "7BEAM"
        ;;
    *)
        echo "Unrecognized selection ${RCVR}. You suck" 1>&2
    esac
    kill $!
done
