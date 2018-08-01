#!/usr/bin/env python
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
from __future__ import print_function

from hatchet import *
import sys
import glob
import struct
import numpy as np
import multiprocessing as mp
import multiprocessing.sharedctypes
import time

import pandas as pd
pd.set_option('display.width', 500)
pd.set_option('display.max_colwidth', 30)


def init(buf_):
    """Initialize shared array"""
    global buf
    buf = buf_



def read_file(args):
    shape, pe, num_nodes, filename = args
    with open(filename, "rb") as metricdb:
        metricdb.seek(32)
        arr1d = np.fromfile(metricdb, dtype=np.dtype('>f8'),
                            count=num_metrics * num_nodes)

    arr = np.frombuffer(buf).reshape(shape)

    pe_off = pe * num_nodes
    arr[pe_off:pe_off + num_nodes, :2].flat = arr1d.flat
    arr[pe_off:pe_off + num_nodes, 2] = range(num_nodes)
    arr[pe_off:pe_off + num_nodes, 3] = float(pe)


if __name__ == "__main__":
    dirname = sys.argv[1]

    time1 = time.time()
    metricdb_files = glob.glob(dirname + '/*.metric-db')

    if len(sys.argv) < 3:
        num_pes = len(metricdb_files)
    else:
        num_pes = int(sys.argv[2])

    print("Using dataset: %s" % dirname)
    print("%d metric-db files" % num_pes)
    print()

    with open(metricdb_files[0], "rb") as metricdb:
        tag = metricdb.read(18)
        version = metricdb.read(5)
        endian = metricdb.read(1)

        if endian == 'b':
            num_nodes = struct.unpack('>i', metricdb.read(4))[0]
            num_metrics = struct.unpack('>i', metricdb.read(4))[0]

    shape = [num_nodes * num_pes, num_metrics + 2]
    size = np.prod(shape)

    metrics = np.empty(shape)

    # assumes that glob returns a sorted order
    for pe, filename in enumerate(metricdb_files[:num_pes]):
        with open(filename, "rb") as metricdb:
            metricdb.seek(32)
            arr1d = np.fromfile(metricdb, dtype=np.dtype('>f8'),
                                count=num_metrics * num_nodes)

        pe_off = pe * num_nodes
        metrics[pe_off:pe_off + num_nodes, :2].flat = arr1d.flat
        metrics[pe_off:pe_off + num_nodes, 2] = range(num_nodes)
        metrics[pe_off:pe_off + num_nodes, 3] = float(pe)

    time2 = time.time()
    print("read metric db ", time2 - time1)

    # read in parallel and compare
    time3 = time.time()

    # shared memory buffer for multiprocessing
    buf = mp.sharedctypes.RawArray('d', size)

    pool = mp.Pool(initializer=init, initargs=(buf,))

    par_metrics = np.frombuffer(buf).reshape(shape)

    args = [(shape, pe, num_nodes, filename)
            for pe, filename in enumerate(metricdb_files[:num_pes])]
    pool.map(read_file, args)

    time4 = time.time()
    print("read metric db in parallel ", time4 - time3)

    print("EQUAL: ", np.array_equal(par_metrics, metrics))
    print("SUM (should be zero):", (par_metrics - metrics).sum())

    dataframe = pd.DataFrame(metrics, columns=('CPU TIME(I)', 'CPU TIME(E)', 'node', 'rank'))
    # print dataframe
    time3 = time.time()
    print("data frame ", time3 - time2)
