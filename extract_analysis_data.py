"""
Requires hdf5_getters from the MSD python src to be in the same directory as
this script """

from hdf5_getters import *
import h5py
import re
import tables
import pandas as pd
import string

import glob2

glob_ext = "/**/*.h5"
data_path = "./data/"
# change this to appropriate letter range
dirs = string.ascii_uppercase[0:5]

for _dir in dirs:
    print("Starting directory %s" % _dir)
    files = glob2.glob(glob_ext + _dir + glob_ext)

    print("%d files found in dir %s" % (len(files), _dir))

    trackid = lambda x: re.search('TR.*', x).group(0)[:-3] # extracts filename minus extension - may change
    track_id = lambda x: trackid(x.filename)
    # song_id = lambda x: get_song_id(x).decode("utf-8") # songid was a bytestring
    extract = [track_id, get_key, get_loudness, get_tempo, get_time_signature, get_mode]

    cols = [f.__name__[4:] for f in extract] # gets function name without "get"
    cols[0] = "track_id" # rename this as it's just a lambda function

    # main loop #
    data = []
    for i, file in enumerate(files):
        if i % 100 == 0:
            print("%i / %i files processed in directory %s" % (i, len(files), _dir))
        with tables.File(file) as song: # must open in new scope so files close
            data.append([e(song) for e in extract])

    # make and save dataframe
    df = pd.DataFrame(data, columns=cols)
    df.to_csv(_dir + ".df", index=False, header=False)
