#!/usr/bin/env python
#
# Copyright 2017-2020 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from __future__ import print_function

import pandas as pd

import hatchet as ht

pd.set_option("display.width", 1500)
pd.set_option("display.max_colwidth", 20)
pd.set_option("display.max_rows", None)


if __name__ == "__main__":
    cali_file = "hatchet/tests/data/caliper-cali/caliper-ex.cali"

    grouping_attribute = "function"
    default_metric = "sum(sum#time.duration),inclusive_sum(sum#time.duration)"
    query = "select function,%s group by %s format json-split" % (
        default_metric,
        grouping_attribute,
    )

    gf = ht.GraphFrame.from_caliper(cali_file, query)

    print(gf.dataframe)
    print("\n")

    print(gf.tree())
