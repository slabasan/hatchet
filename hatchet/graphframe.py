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
import pandas as pd
import numpy

from .hpctoolkit_reader import HPCToolkitReader
from .caliper_reader import CaliperReader
from .gprof_dot_reader import GprofDotReader
from .node import Node
from .graph import Graph
from .frame import Frame

lit_idx = 0
squ_idx = 0


class GraphFrame:
    """ An input dataset is read into an object of this type, which includes a
        graph and a dataframe.
    """

    def __init__(self, graph=None, dataframe=pd.DataFrame()):
        self.graph = graph
        self.dataframe = dataframe

    def from_hpctoolkit(self, dirname):
        """ Read in an HPCToolkit database directory.
        """
        reader = HPCToolkitReader(dirname)

        (self.graph, self.dataframe, self.exc_metrics,
            self.inc_metrics) = reader.create_graphframe()

    def from_caliper(self, filename):
        """ Read in a Caliper Json-split file.
        """
        reader = CaliperReader(filename)

        (self.graph, self.dataframe, self.exc_metrics,
            self.inc_metrics) = reader.create_graphframe()

    def from_gprof_dot(self, filename):
        """ Read in a DOT file generated by gprof2dot.
        """
        reader = GprofDotReader(filename)

        (self.graph, self.dataframe, self.exc_metrics,
            self.inc_metrics) = reader.create_graphframe()

    def from_literal(self, graph_dict):
        """ Read graph from a dict literal.
        """
        global lit_idx

        def parse_node_literal(child_dict, hparent):
            """ Create node_dict for one node and then call the function
                recursively on all children.
            """
            global lit_idx

            hnode = Node(Frame({'name': child_dict['name']},
                               ['name']),
                         hparent)

            node_dicts.append(dict({'node': hnode,
                                    'name': child_dict['name']},
                                    **child_dict['metrics']))
            lit_idx += 1
            hparent.add_child(hnode)

            if 'children' in child_dict:
                for child in child_dict['children']:
                    parse_node_literal(child, hnode)

        # start with creating a node_dict for the root
        lit_idx = 0
        graph_root = Node(Frame({'name': graph_dict['name']},
                                ['name']),
                          None)

        node_dicts = []
        node_dicts.append(dict({'node': graph_root,
                                'name': graph_dict['name']},
                                **graph_dict['metrics']))
        lit_idx += 1

        # call recursively on all children of root
        if 'children' in graph_dict:
            for child in graph_dict['children']:
                parse_node_literal(child, graph_root)

        self.exc_metrics = []
        self.inc_metrics = []
        for key in graph_dict['metrics'].keys():
            if '(inc)' in key:
                self.inc_metrics.append(key)
            else:
                self.exc_metrics.append(key)

        self.graph = Graph([graph_root])
        self.dataframe = pd.DataFrame(data=node_dicts)
        self.dataframe.set_index(['node'], drop=False, inplace=True)

    def copy(self):
        """ Return a copy of the graphframe.
        """
        graph_copy, node_clone = self.graph.copy()
        dataframe_copy = self.dataframe.copy()

        dataframe_copy['node'] = dataframe_copy['node'].apply(lambda x: node_clone[x])
        index_names = self.dataframe.index.names
        dataframe_copy.set_index(index_names, inplace=True, drop=False)

        return GraphFrame(graph_copy, dataframe_copy)

    def update_inclusive_columns(self):
        """ Update inclusive columns (typically after operations that rewire
            the graph.
        """
        for root in self.graph.roots:
            for node in root.traverse(order='post'):
                for metric in self.exc_metrics:
                    val = self.dataframe.loc[node, metric]
                    for child in node.children:
                        val += self.dataframe.loc[child, metric]
                    inc_metric = metric + ' (inc)'
                    self.dataframe.loc[node, inc_metric] = val

    def drop_index_levels(self, function=numpy.mean):
        """ Drop all index levels but 'node'
        """
        index_names = list(self.dataframe.index.names)
        index_names.remove('node')

        # create dict that stores aggregation function for each column
        agg_dict = {}
        for col in self.dataframe.columns.tolist():
            if col in self.exc_metrics + self.inc_metrics:
                agg_dict[col] = function
            else:
                agg_dict[col] = lambda x: x.iloc[0]

        # perform a groupby to merge nodes that just differ in index columns
        self.dataframe.reset_index(level='node', inplace=True, drop=True)
        agg_df = self.dataframe.groupby('node').agg(agg_dict)
        agg_df.drop(index_names, axis=1, inplace=True)

        self.dataframe = agg_df

    def filter(self, filter_function):
        """ Filter the dataframe using a user supplied function.
        """
        filtered_rows = self.dataframe.apply(filter_function, axis=1)
        filtered_df = self.dataframe[filtered_rows]

        filtered_gf = GraphFrame(self.graph, filtered_df)
        filtered_gf.exc_metrics = self.exc_metrics
        filtered_gf.inc_metrics = self.inc_metrics

        return filtered_gf

    def squash(self):
        """ Squash the graph after a filtering operation on the graphframe.
        """
        global squ_idx
        num_nodes = len(self.graph)

        # calculate number of unique nodes in the dataframe
        # and a set of filtered nodes
        if 'rank' in self.dataframe.index.names:
            num_rows_df = len(self.dataframe.groupby(['node']))
            filtered_nodes = self.dataframe.index.levels[0]
        else:
            num_rows_df = len(self.dataframe.index)
            filtered_nodes = self.dataframe.index

        node_clone = {}
        old_to_new_id = {}

        # function to connect a node to the nearest descendants that are in the
        # list of filtered nodes
        def rewire_tree(node, clone, is_root, roots):
            global squ_idx

            cur_children = node.children
            new_children = []

            # iteratively go over the children of a node
            while(cur_children):
                for child in cur_children:
                    cur_children.remove(child)
                    if child in filtered_nodes:
                        new_children.append(child)
                    else:
                        for grandchild in child.children:
                            cur_children.append(grandchild)

            label_to_new_child = {}
            if node in filtered_nodes:
                # create new clones for each child in new_children and rewire
                # with this node
                for new_child in new_children:
                    node_label = new_child.frame
                    if node_label not in label_to_new_child.keys():
                        new_child_clone = Node(new_child.frame, clone)
                        idx = squ_idx
                        squ_idx += 1
                        clone.add_child(new_child_clone)
                        label_to_new_child[node_label] = new_child_clone
                    else:
                        new_child_clone = label_to_new_child[node_label]

                    node_clone[new_child] = new_child_clone
                    rewire_tree(new_child, new_child_clone, False, roots)
            elif is_root:
                # if we reach here, this root is not in the graph anymore
                # make all its nearest descendants roots in the new graph
                for new_child in new_children:
                    new_child_clone = Node(new_child.frame, None)
                    node_clone[new_child] = new_child_clone
                    squ_idx += 1
                    roots.append(new_child_clone)
                    rewire_tree(new_child, new_child_clone, False, roots)

        squ_idx = 0

        new_roots = []
        # only do a squash if a filtering operation has been applied
        if num_nodes != num_rows_df:
            for root in self.graph.roots:
                if root in filtered_nodes:
                    clone = Node(root.frame, None)
                    new_roots.append(clone)
                    node_clone[root] = clone
                    squ_idx += 1
                    rewire_tree(root, clone, True, new_roots)
                else:
                    rewire_tree(root, None, True, new_roots)

        # create new dataframe that cloned nodes
        new_dataframe = self.dataframe.copy()
        new_dataframe['node'] = new_dataframe['node'].apply(lambda x: node_clone[x])
        new_dataframe.reset_index(level='node', inplace=True, drop=True)

        # create dict that stores aggregation function for each column
        agg_dict = {}
        for col in new_dataframe.columns.tolist():
            if col in self.exc_metrics + self.inc_metrics:
                agg_dict[col] = numpy.sum
            else:
                agg_dict[col] = lambda x: x.iloc[0]

        # perform a groupby to merge nodes with the same callpath
        index_names = self.dataframe.index.names
        agg_df = new_dataframe.groupby(index_names).agg(agg_dict)

        new_graphframe = GraphFrame(Graph(new_roots), agg_df)
        new_graphframe.exc_metrics = self.exc_metrics
        new_graphframe.inc_metrics = self.inc_metrics
        new_graphframe.update_inclusive_columns()

        return new_graphframe

    def __iadd__(self, other):
        """ Computes column-wise sum of two dataframes with identical
            graphs
        """
        for metric in self.exc_metrics + self.inc_metrics:
            self.dataframe[metric] = self.dataframe[metric].add(other.dataframe[metric])

        return self

    def __add__(self, other):
        """ Computes column-wise sum of two dataframes with identical
            graphs
        """
        gf_copy = self.copy()
        for metric in self.exc_metrics + self.inc_metrics:
            gf_copy.dataframe[metric] = gf_copy.dataframe[metric].add(other.dataframe[metric])

        return gf_copy

    def __isub__(self, other):
        """ Computes column-wise difference of two dataframes with identical
            graphs
        """
        for metric in self.exc_metrics + self.inc_metrics:
            self.dataframe[metric] = self.dataframe[metric].subtract(other.dataframe[metric])

        return self

    def __sub__(self, other):
        """ Computes column-wise difference of two dataframes with identical
            graphs
        """
        gf_copy = self.copy()
        for metric in self.exc_metrics + self.inc_metrics:
            gf_copy.dataframe[metric] = gf_copy.dataframe[metric].subtract(other.dataframe[metric])

        return gf_copy
