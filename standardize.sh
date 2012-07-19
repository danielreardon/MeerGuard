arfn=$1
stdfn=$(get_standard.py $1)
dir=$(dirname ${stdfn})
basefn=$(basename ${stdfn} .std)
mfn=${dir}/${basefn}.m
txtfn=${dir}/${basefn}.txt

shift

echo "Creating a standard using paas for ${arfn}"

if [ -f ${stdfn} ]; then
    echo "Standard (${stdfn}) already exists. Skipping!" 1>&2
    exit 1
else
    psrplot -p freq -j DTp -D 55/xs ${arfn}
    psrplot -p time -j DFp -D 56/xs ${arfn}
    psrplot -p flux -D 5000/xs ${arfn} # Open another plot because paas 
                                       # commandeers last opened PGPLOT window
    mkdir -p ${dir}
    paas -w ${mfn} -d 57/xs -s ${stdfn} -j ${txtfn} -i ${arfn} $@
    if [ -s ${mfn} ]; then
        # Create diagnostic
        tmpfn=$(mktemp)
        pat -t -s ${stdfn} -F -T -K ${tmpfn}/PNG ${arfn} > /dev/null
        mv ${tmpfn} ${stdfn}.png
        echo "Output files:"
        echo "    paas model file: ${mfn}"
        echo "    paas text file: ${txtfn}"
        echo "    standard profile: ${stdfn}"
        echo "    diagnostic plot: ${stdfn}.png"
    else
        if [ -f ${mfn} ]; then
            echo "Model file created by paas is empty. Removing paas output files." 1>&2
            rm -f ${stdfn} ${mfn} ${txtfn}
        else
            echo "No model file created by paas. Weird" 1>&2
            exit 2
        fi
    fi
fi
