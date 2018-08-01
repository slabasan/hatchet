##############################################################################
# Copyright (c) 2017-2018, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory.
#
# This file is part of Hatchet.
# Created by Abhinav Bhatele <bhatele@llnl.gov>.
# LLNL-CODE-741008. All rights reserved.
#
# For details, see: https://github.com/LLNL/hatchet
# Please also read the LICENSE file for the MIT License notice.
##############################################################################

#!/usr/bin/env python

from hatchet import *
import sys
import glob
import struct
import numpy as np
import time

import pandas as pd
pd.set_option('display.width', 500)
pd.set_option('display.max_colwidth', 30)


if __name__ == "__main__":
    dirname = sys.argv[1]
    num_pes = int(sys.argv[2])

    time1 = time.time()
    metricdb_files = glob.glob(dirname + '/*.metric-db')

    with open(metricdb_files[0], "rb") as metricdb:
        tag = metricdb.read(18)
        version = metricdb.read(5)
        endian = metricdb.read(1)

        if endian == 'b':
            num_nodes = struct.unpack('>i', metricdb.read(4))[0]
            num_metrics = struct.unpack('>i', metricdb.read(4))[0]

    shape = [num_nodes * num_pes, num_metrics + 2]
    metrics = np.empty(shape)

    # assumes that glob returns a sorted order
    for pe, filename in enumerate(metricdb_files[:num_pes]):
        with open(filename, "rb") as metricdb:
            metricdb.seek(32)
            arr1d = np.fromfile(metricdb, dtype=np.dtype('>f8'),
                                count=num_metrics * num_nodes)

        pe_off = pe*num_nodes

        metrics[pe_off:pe_off + num_nodes, :2].flat = arr1d.flat
        metrics[pe_off:pe_off + num_nodes, 2] = range(num_nodes)
        metrics[pe_off:pe_off + num_nodes, 3] = float(pe)

    time2 = time.time()
    print "read metric db ", time2-time1


    dataframe = pd.DataFrame(metrics, columns=('CPU TIME(I)', 'CPU TIME(E)', 'node', 'rank'))
    # print dataframe
    time3 = time.time()
    print "data frame ", time3-time2
