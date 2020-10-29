# Copyright 2017-2020 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

from hatchet.node import Node, MultiplePathError
from hatchet.frame import Frame
from hatchet.graph import Graph


def test_from_lists():
    node = Node.from_lists("a")
    assert node.frame == Frame(name="a", type="None")

    a = Frame(name="a", type="None")
    b = Frame(name="b", type="None")
    c = Frame(name="c", type="None")

    node = Node.from_lists(["a", ["b", "c"]])

    assert node.frame == a
    assert node.children[0].frame == b
    assert node.children[0].children[0].frame == c


def test_from_lists_value_error():
    with pytest.raises(ValueError):
        Node.from_lists(object())


def test_traverse_pre():
    node = Node(Frame(name="a"))
    assert list(node.traverse(attrs="name")) == ["a"]

    node = Node.from_lists(["a", ["b", "d", "e"], ["c", "f", "g"]])
    assert list(node.traverse(attrs="name")) == ["a", "b", "d", "e", "c", "f", "g"]


def test_traverse_post():
    node = Node.from_lists(["a", ["b", "d", "e"], ["c", "f", "g"]])
    assert list(node.traverse(order="post", attrs="name")) == [
        "d",
        "e",
        "b",
        "f",
        "g",
        "c",
        "a",
    ]


def test_traverse_dag():
    d = Node(Frame(name="d"))
    node = Node.from_lists(["a", ["b", d], ["c", d]])
    assert list(node.traverse(attrs="name")) == ["a", "b", "d", "c"]


def test_node_repr():
    d = Node(Frame(a=1, b=2, c=3))
    assert repr(d) == "Node({'a': 1, 'b': 2, 'c': 3})"


def test_path():
    d = Node(Frame(name="d", type="function"))
    node = Node.from_lists(["a", ["b", d]])

    assert d.path() == (
        Frame(name="a", type="None"),
        Frame(name="b", type="None"),
        Frame(name="d", type="function"),
    )
    assert d.parents[0].path() == (
        Frame(name="a", type="None"),
        Frame(name="b", type="None"),
    )
    assert node.path() == (Frame(name="a", type="None"),)

    assert d.path(attrs="name") == ("a", "b", "d")
    assert d.parents[0].path(attrs="name") == ("a", "b")
    assert node.path(attrs="name") == ("a",)


def test_paths():
    d = Node(Frame(name="d"))
    Node.from_lists(["a", ["b", d], ["c", d]])
    with pytest.raises(MultiplePathError):
        d.path()

    assert d.paths() == [
        (
            Frame(name="a", type="None"),
            Frame(name="b", type="None"),
            Frame(name="d", type="None"),
        ),
        (
            Frame(name="a", type="None"),
            Frame(name="c", type="None"),
            Frame(name="d", type="None"),
        ),
    ]

    assert d.paths(attrs="name") == [("a", "b", "d"), ("a", "c", "d")]


def test_traverse_paths():
    d = Node(Frame(name="d"))
    diamond_subdag = Node.from_lists(("a", ("b", d), ("c", d)))

    g = Graph.from_lists(("e", "f", diamond_subdag), ("g", diamond_subdag, "h"))
    assert list(g.traverse(attrs="name")) == ["e", "a", "b", "d", "c", "f", "g", "h"]


def check_dag_equal():
    chain = Node.from_lists(("a", ("b", ("c", ("d",)))))

    d = Node(Frame(name="d"))
    diamond = Node.from_lists(("a", ("b", d), ("c", d)))

    tree = Node.from_lists(
        ("a", ("b", "e", "f", "g"), ("c", "e", "f", "g"), ("d", "e", "f", "g"))
    )

    assert chain.dag_equal(chain)
    assert chain.dag_equal(chain.copy())

    assert diamond.dag_equal(diamond)
    assert diamond.dag_equal(diamond.copy())

    assert tree.dag_equal(tree)
    assert tree.dag_equal(tree.copy())

    assert not chain.dag_equal(tree)
    assert not chain.dag_equal(diamond)

    assert not tree.dag_equal(chain)
    assert not tree.dag_equal(diamond)

    assert not diamond.dag_equal(chain)
    assert not diamond.dag_equal(tree)
