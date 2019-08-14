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

import sys

from .external.printtree import trees_as_text
from .util.dot import trees_to_dot
from .node import Node
from .frame import Frame


class Graph:
    """A possibly multi-rooted tree or graph from one input dataset."""

    def __init__(self, roots):
        if roots is not None:
            self.roots = roots

    def copy(self):
        """Returns a copy of the graph and a mapping of old to new nodes."""
        list_roots = []
        node_clone = {}

        if self.roots:
            for root in self.roots:
                # do a breadth-first traversal of the tree
                for node in root.traverse_bf():
                    if not node.parents:
                        clone = Node(Frame(node.frame.attrs.copy()), None)
                        list_roots.append(clone)
                    else:
                        # make a list of new parents to make connections with
                        new_parents = []
                        for parent in node.parents:
                            new_parents.append(node_clone[parent])
                        clone = Node(Frame(node.frame.attrs.copy()), None)

                        # connect child to parents and vice-versa
                        for parent in new_parents:
                            clone.add_parent(parent)
                            parent.add_child(clone)

                    node_clone[node] = clone

        return (Graph(list_roots), node_clone)

    def union(self, other):
        """Unify two graphs by comparing frame at each node and return a single
        unified graph and a mapping from self nodes to other nodes.
        """
        def node_union(union_nodes, subgraph, other_subgraph, vs=None, vo=None):
            if vs is None:
                vs = set()
            if vo is None:
                vo = set()

            vs.add(id(subgraph))
            vo.add(id(other_subgraph))

            ssorted = sorted(subgraph.children, key=lambda x: x.frame)
            osorted = sorted(other_subgraph.children, key=lambda x: x.frame)

            for ch, other_ch in zip(ssorted, osorted):
                # if node is not in union graph, add it
                if ch.frame not in union_nodes:
                    union_nodes[ch.frame] = ch
                if other_ch.frame not in union_nodes:
                    union_nodes[other_ch.frame] = other_ch

                visited_s = id(ch) in vs
                visited_o = id(other_ch) in vo

                # skip visited nodes
                if visited_s or visited_o:
                   continue

                # if subgraph node already exists in union graph, then append its
                # children and parents to the node in union graph
                if ch.frame in union_nodes:
                    for p1 in ch.parents:
                        # add parent if it is not in master graph
                        has_parent = False
                        for i in union_nodes[ch.frame].parents:
                            if p1.frame == i.frame:
                                has_parent = True
                                break
                        if not has_parent:
                            union_nodes[ch.frame].add_parent(p1)
                    for c1 in ch.children:
                        # add child if it is not in master graph
                        has_child = False
                        for i in union_nodes[ch.frame].children:
                            if c1.frame == i.frame:
                                has_child = True
                                break
                        if not has_child:
                            union_nodes[ch.frame].add_child(c1)

                # if other_subgraph node already exists in union graph, then
                # append its children and parents to the node in union graph
                if other_ch.frame in union_nodes:
                    for p2 in other_ch.parents:
                        # add parent if it is not in master graph
                        has_parent = False
                        for i in union_nodes[other_ch.frame].parents:
                            if p2.frame == i.frame:
                                has_parent = True
                                break
                        if not has_parent:
                            union_nodes[other_ch.frame].add_parent(p2)
                    for c2 in other_ch.children:
                        # add child if it is not in master graph
                        has_child = False
                        for i in union_nodes[other_ch.frame].children:
                            if c2.frame == i.frame:
                                has_child = True
                                break
                        if not has_child:
                            union_nodes[other_ch.frame].add_child(c2)

                # update union nodes with parents and children
                ch.parents = union_nodes[ch.frame].parents
                ch.children = union_nodes[ch.frame].children

                other_ch.parents = union_nodes[other_ch.frame].parents
                other_ch.children = union_nodes[other_ch.frame].children

                node_union(union_nodes, ch, other_ch, vs, vo)

        union_roots = []
        union_nodes = {}

        # if graphs are structurally equivalent, return a copy of self.graph
        if self == other:
            graph_copy, _ = self.copy()
            return graph_copy
        # if graphs are not structurally equivalent, return a new graph
        elif self != other:
            vs = set()
            vo = set()

            # sort roots by its frame
            ssorted = sorted(self.roots, key=lambda x: x.frame)
            osorted = sorted(other.roots, key=lambda x: x.frame)

            for self_root, other_root in zip(ssorted, osorted):
                # if list of roots is empty, append root
                if not union_roots:
                    union_roots.append(self_root)
                # otherwise, loop over list of roots to see if self_root or
                # other_root are already in the list, appending them if they
                # are not
                else:
                    has_node = False
                    for i in union_roots:
                        if self_root.frame == i.frame:
                            has_node = True
                            break
                    if not has_node:
                        union_roots.append(self_root)

                    has_node = False
                    for i in union_roots:
                        if other_root.frame == i.frame:
                            has_node = True
                            break
                    if not has_node:
                        union_roots.append(other_root)

                node_union(union_nodes, self_root, other_root, vs, vo)

            return Graph(union_roots)

    def to_string(
        self,
        roots=None,
        dataframe=None,
        metric="time",
        name="name",
        context="file",
        rank=0,
        threshold=0.0,
        expand_names=False,
        unicode=True,
        color=True,
    ):
        """Print the graph with or without some metric attached to each node."""
        if roots is None:
            roots = self.roots

        result = trees_as_text(
            roots,
            dataframe,
            metric,
            name,
            context,
            rank,
            threshold,
            expand_names,
            unicode=unicode,
            color=color,
        )

        if sys.version_info >= (3, 0, 0):
            return result
        else:
            return result.encode("utf-8")

    def to_dot(
        self,
        roots=None,
        dataframe=None,
        metric="time",
        name="name",
        rank=0,
        threshold=0.0,
    ):
        """Write the graph in the graphviz dot format:
        https://www.graphviz.org/doc/info/lang.html
        """
        if roots is None:
            roots = self.roots

        result = trees_to_dot(roots, dataframe, metric, name, rank, threshold)

        return result

    def __str__(self):
        """Returns a string representation of the graph."""
        return self.to_string()

    def __len__(self):
        """Size of the graph in terms of number of nodes."""
        num_nodes = 0

        for root in self.roots:
            num_nodes = sum(1 for n in root.traverse())

        return num_nodes

    def __eq__(self, other):
        """Check if two graphs have the same structure by comparing frame at each
        node.
        """
        vs = set()
        vo = set()

        # if both graphs are pointing to the same object, then graphs are equal
        if self is other:
            return True

        # if number of roots do not match, then graphs are not equal
        if len(self.roots) != len(other.roots):
            return False

        if len(self) != len(other):
            return False

        # sort roots by its frame
        ssorted = sorted(self.roots, key=lambda x: x.frame)
        osorted = sorted(other.roots, key=lambda x: x.frame)

        for self_root, other_root in zip(ssorted, osorted):
            # if frames do not match, then nodes are not equal
            if self_root.frame != other_root.frame:
                return False

            if not self_root.check_dag_equal(other_root, vs, vo):
                return False

        return True

    def __ne__(self, other):
        return not (self == other)
