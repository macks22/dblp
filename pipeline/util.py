import os
import csv


def write_csv(fname, header, rows):
    """Write an iterable of records to a csv file with optional header."""
    if not fname.endswith('.csv'):
        fname = '%s.csv' % os.path.splitext(fname)[0]

    with open(fname, 'w') as f:
        writer = csv.writer(f)
        if header: writer.writerow(header)
        writer.writerows(rows)


def yield_csv_records(csv_file):
    """Iterate over csv records, returning each as a list of strings."""
    with open(csv_file) as f:
        reader = csv.reader(f)
        reader.next()
        for record in reader:
            yield record


def swap_file_delim(infile, indelim, outfile, outdelim):
    with open(infile) as rf:
        in_lines = (l.strip().split(indelim) for l in rf)
        out_lines = (outdelim.join(l) for l in in_lines)
        with open(outfile, 'w') as wf:
            wf.write('\n'.join(out_lines))


def build_and_save_idmap(graph, outfile, idname='author'):
    """Save vertex ID to vertex name mapping and then return it."""
    first_col = '%s_id' % idname
    idmap = {v['name']: v.index for v in graph.vs}
    rows = sorted(idmap.items())
    util.write_csv(outfile, (first_col, 'node_id'), rows)
    return idmap


def read_idmap(idmap_fname):
    records = util.yield_csv_records(idmap_fname)
    idmap = {record[0]: int(record[1]) for record in mapreader}
    return idmap


def build_undirected_graph(nodes, edges):
    """Build an undirected graph, removing duplicates edges."""
    graph = igraph.Graph()
    graph.add_vertices(nodes)
    graph.add_edges(edges)
    graph.simplify()
    return graph

