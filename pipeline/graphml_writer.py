"""
EDCAR requires a graphml file with all edges, nodes, and node attributes
Making the node attributes dense helps reduce EDCAR's unseemly memory
demands. For graphs with many attributes, this will make the data file
massive, but storage is cheaper than memory.
"""

import os
import sys
import logging
import argparse

import numpy as np


header = """<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
     http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">

<graph id="G" edgedefault="undirected">
"""
footer = """
</graph>
</graphml>
"""

node_tag = """<node id="{node_id}" {attributes}/>\n"""
edge_tag = """<edge source="{source}" target="{target}"/>\n"""


class Node(object):
    """Store node attributes and return as string."""

    def __init__(self, node_id, features):
        self.node_id = node_id
        self.features = features

    def __str__(self):
        attrs = ('t%d="%d"' % (term_id, tf)
                 for term_id, tf in enumerate(self.features))
        attr_list = ' '.join(attrs)
        return node_tag.format(node_id=self.node_id, attributes=attr_list)


def iter_corpus(corpus_file):
    with open(corpus_file) as f:
        f.readline()
        counts = f.readline().split()
        yield map(int, counts)
        line = f.readline().strip().split()
        while line:
            yield map(int, line)
            line = f.readline().strip().split()


def iter_corpus_terms(corpus_file):
    lines = iter_corpus(corpus_file)
    num_nodes, num_terms, num_entries = lines.next()
    features = np.array([0 for num in range(num_terms)])

    # first yield the totals for logging purposes
    yield (num_nodes, num_terms)

    cur_features = features.copy()
    node_id, term_id, tf = lines.next()
    old_id = node_id - 1
    cur_features[term_id-1] = tf

    for node_id, term_id, tf in lines:
        if (node_id - 1) != old_id:
            yield (old_id, cur_features)
            cur_features = features.copy()

        cur_features[term_id-1] = tf
        old_id = node_id - 1


def iter_edges(edgelist_file):
    with open(edgelist_file) as f:
        for line in f:
            yield line.split()


def write_dense_graph(node_iter, edge_iter, fpath='edcar-graph.graphml'):
    with open(fpath, 'w') as f:
        f.write(header)

        # log num nodes and terms
        num_nodes, num_terms = node_iter.next()
        logging.info(
            "writing %d nodes, each with %d features." % (num_nodes, num_terms))

        # write all nodes for the given PIs
        for num, node_attrs in enumerate(node_iter):
            node = Node(*node_attrs)
            if num % 1000 == 0:
                logging.info("%s nodes left to write..." % (num_nodes - num))
            f.write('%s\n' % str(node))

        # write all edges for the given PIs
        logging.info("done writing nodes.")
        logging.info("writing edges...")
        for src, target in edge_iter:
            f.write('%s\n' % edge_tag.format(source=src, target=target))

        f.write(footer)


def write_edcar_graph(node_attr_file, edgelist_file, outfile='edcar-out.graphml'):
    node_iter = iter_corpus_terms(node_attr_file)
    edge_iter = iter_edges(edgelist_file)
    write_dense_graph(node_iter, edge_iter, outfile)


def make_parser():
    parser = argparse.ArgumentParser(
        description="write dense graphml file given node attributes and edges")
    parser.add_argument(
        '-n', '--node-attributes-file', action='store',
        help='mm format node term frequency corpus')
    parser.add_argument(
        '-e', '--edgelist-file', action='store',
        help='whitespace delimited edgelist')
    parser.add_argument(
        '-o', '--outfile', action='store', default='out.graphml',
        help='name of file to use for output; graphml suffix is added')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='enable verbose logging output')
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    # set up logging
    level = logging.DEBUG if args.verbose else logging.CRITICAL
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s [%(levelname)s]: %(message)s")

    if not args.node_attributes_file or not args.edgelist_file:
        parser.print_usage()
        sys.exit(1)

    write_edcar_graph(
        args.node_attributes_file, args.edgelist_file, args.outfile)
