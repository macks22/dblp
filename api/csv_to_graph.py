"""
Utility to construct a graph from csv files. In particular, the main utility of
this module takes a csv file with ids and another with links between ids and
constructs a graph using the igraph package.
"""

import os
import sys
import csv
import logging
import argparse
import igraph


NO_SUCH_HEADER = 10
NO_SUCH_NODE_FILE = 11
NO_SUCH_EDGE_FILE = 12


def read_nodes(csvfile, id_colname):
    """Return a generator which yields the node ids from the given csv file.
    :param str csvfile: Path of the csv file to read node ids from.
    :param str id_colname: Name of the csv column for the ids.
    """
    try:
        f = open(csvfile)
    except IOError:
        logging.error('id file %s not present' % csvfile)
        sys.exit(NO_SUCH_NODE_FILE)

    reader = csv.reader(f)
    headers = reader.next()

    try:
        id_idx = headers.index(id_colname)
    except ValueError:
        logging.error('node id column name not present in %s' % csvfile)
        sys.exit(NO_SUCH_HEADER)

    for row in reader:
        yield row[id_idx]


def add_nodes(nodes, graph):
    """Add the nodes to the graph and return a dictionary which maps ids as read
    from the csv data files to the ids as assigned by igraph.
    """
    graph.add_vertices(nodes)
    idmap = {v['name']: v.index for v in graph.vs}
    return idmap


def read_edges(csvfile):
    """Return a generator with edges from the csv file. These will need to be
    converted to vertex ids if those are not contiguous starting from 0. This
    function assumes the source is the first column and the target is the
    second.
    :param str csvfile: Path of the csv file to read edges from.
    """
    try:
        f = open(csvfile)
    except IOError:
        logging.error('edge id file %s not present' % csvfile)
        sys.exit(NO_SUCH_EDGE_FILE)

    reader = csv.reader(f)
    reader.next()  # discard column headers
    for row in reader:
        yield (row[0], row[1])


def convert_edges(edges, idmap):
    """Convert the edge ids read from the csv file to their vertex id
    equivalents. This is necessary because igraph assigns its own vertex ids
    rather than using the ones used to add edges.
    :param iterator edges: An iterable for the edges to be converted. Should be
        (src, target) tuples
    :param dict idmap: Keys should be edges as they are read from the csv files,
        and values should be vertex ids as assigned by igraph during
        `add_vertices`.
    """
    for e1, e2 in edges:
        try:
            src = idmap[e1]
        except KeyError:
            logging.error('edge src id not present in id file: %s' % e1)
            continue
        try:
            target = idmap[e2]
        except KeyError:
            logging.error('edge target id not present in id file: %s' % e2)
            continue

        yield (src, target)


def make_and_write_graph(nodes, edge_ids, outfile):
    g = igraph.Graph()
    idmap = add_nodes(nodes, g)
    edges = convert_edges(edge_ids, idmap)
    g.add_edges(edges)

    # save the graph in gzipped graphml format
    g.write_graphmlz('%s.graphml.gz' % outfile)

    # save the id map
    with open('raw-id-to-node-id-map.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(('raw_id','node_id'))
        writer.writerows(idmap.items())

    return g


def make_parser():
    parser = argparse.ArgumentParser(
        description="construct a graph from csv files and save to graphml file")
    parser.add_argument(
        '-nf', '--node-file', action='store',
        help="csv file with the ids to make vertices from")
    parser.add_argument(
        '-n', '--node-name', action='store',
        help="column header for node ids in the id file")
    parser.add_argument(
        '-e', '--edge-file', action='store',
        help="csv file with the src and target ids to make edges from")
    parser.add_argument(
        '-o', '--out-file', action='store',
        help="file to write the graph to; graphml suffix will be added")
    # we assume the edges are undirected
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='enable verbose logging output')
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.ERROR)
    nodes = read_nodes(args.node_file, args.node_name)
    edges = read_edges(args.edge_file)
    g = make_and_write_graph(nodes, edges, args.out_file)
