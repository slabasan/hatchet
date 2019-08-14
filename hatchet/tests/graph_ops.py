##############################################################################
# Copyright (c) 2017-2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory.
#
# This file is part of Hatchet.
# Created by Abhinav Bhatele <bhatele@llnl.gov>.
# LLNL-CODE-741008. All rights reserved.
#
# For details, see: https://github.com/LLNL/hatchet
# Please also read the LICENSE file for the MIT License notice.
##############################################################################

from hatchet import GraphFrame


def test_graph_equal(mock_graph_literal):
    gf = GraphFrame()
    gf.from_literal(mock_graph_literal)

    other = GraphFrame()
    other.from_literal(mock_graph_literal)

    assert gf.graph == other.graph


def test_graph_not_equal(mock_graph_literal, calc_pi_hpct_db):
    gf = GraphFrame()
    gf.from_literal(mock_graph_literal)

    other = GraphFrame()
    other.from_hpctoolkit(str(calc_pi_hpct_db))

    assert gf.graph != other.graph


def test_dag_not_equal(mock_dag_literal1, mock_dag_literal2):
    gf = GraphFrame()
    gf.from_literal(mock_dag_literal1)

    other = GraphFrame()
    other.from_literal(mock_dag_literal2)

    assert gf.graph != other.graph


def test_union_dag_same_structure(mock_dag_literal1):
    # make graphs g1 and g2 that you know are equal
    gf = GraphFrame()
    gf.from_literal(mock_dag_literal1)

    other = GraphFrame()
    other.from_literal(mock_dag_literal1)

    g1 = gf.graph
    g2 = other.graph

    assert g1 == g2

    g3 = g1.union(g2)
    assert g3 is not g1
    assert g3 is not g2
    assert g3 == g1
    assert g3 == g2


def test_union_dag_different_structure(mock_dag_literal1, mock_dag_literal2):
    # make graphs g1, g2, and g3, where you know g3 is the union of g1 and g2
    gf = GraphFrame()
    gf.from_literal(mock_dag_literal1)

    other = GraphFrame()
    other.from_literal(mock_dag_literal2)

    known_union = GraphFrame()
    known_union.from_literal(
        [
            {
                "name": "A",
                "metrics": {"time (inc)": 130.0, "time": 0.0},
                "children": [
                    {
                        "name": "B",
                        "metrics": {"time (inc)": 20.0, "time": 5.0},
                        "children": [
                            {
                                "name": "C",
                                "metrics": {"time (inc)": 5.0, "time": 5.0},
                                "children": [
                                    {
                                        "name": "D",
                                        "metrics": {"time (inc)": 8.0, "time": 1.0},
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "name": "E",
                        "metrics": {"time (inc)": 55.0, "time": 10.0},
                        "children": [
                            {"name": "F", "metrics": {"time (inc)": 1.0, "time": 9.0}},
                            {"name": "H", "metrics": {"time (inc)": 1.0, "time": 9.0}}
                        ],
                    },
                ],
            }
        ]
    )

    g1 = gf.graph
    g2 = other.graph
    g3 = known_union.graph

    assert g1 != g2

    g4 = g1.union(g2)
    assert g4 == g3
