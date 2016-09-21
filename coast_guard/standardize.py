import sys
import subprocess

import toas
import utils

for fn in sys.argv[1:]:
    arf = utils.ArchiveFile(fn)
    stdfn = toas.get_standard(arf)
    basefn = stdfn[:-4]
    mfn = basefn+".m"
    txtfn = basefn+".txt"

    cmd = "paas -w %s -D -s %s -j %s -i %s" % (mfn, stdfn, txtfn, fn)
    subprocess.call(cmd, shell=True)
