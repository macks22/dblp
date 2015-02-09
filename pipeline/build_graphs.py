import csv
import pandas as pd
import igraph
import util


def read_paper_vertices(paper_file):
    """Iterate through paper IDs from the paper csv file."""
    for record in util.yield_csv_records(paper_file):
        yield record[0]


def read_paper_venues(paper_file):
    """Iterate through (paper_id, venue) pairs from the paper csv file."""
    for record in util.yield_csv_records(paper_file):
        yield (record[0], record[2])


def read_paper_references(refs_file, idmap):
    """Filter out references to papers outside dataset."""
    for paper_id, ref_id in util.yield_csv_records(refs_file):
        try: yield (idmap[paper_id], idmap[ref_id])
        except: pass


def build_paper_citation_graph(paper_file, author_file, refs_file,
                               idmap_fname='paper-id-to-node-id-map',
                               save=True, out_fname='paper-citation-graph'):
    """Build the citation graph for all papers. For each paper, a link is drawn
    between it and all of its references. The venue of its publication and its
    list of authors are added as node attributes. If `save` is True, the graph
    is saved in both pickle.gz and graphml.gz format. Both formats are used
    because pickle allows inclusion of the `author_ids` attributes of the nodes,
    which consists of a list of all authors of the paper. Graphml on the other
    hand, is a simpler and more space-efficient format, which will only save the
    venues as node attributes.

    :param str paper_file: File name for csv paper records.
    :param str author_file: File name for csv author records.
    :param str refs_file: File name for csv citation file.
    :param str idmap_fname: File name for paper id to node id mapping.
    :param bool save: Whether to save the graph or not.
    :param str out_fname: If saving, the graph is saved using this basename.
    """
    # get paper ids from csv file and add to graph
    refg = igraph.Graph()
    nodes = read_paper_vertices(paper_file)
    refg.add_vertices(nodes)

    # Build and save paper id to node id mapping
    idmap = util.build_and_save_idmap(refg, idmap_fname, 'paper')

    # now add venues to vertices as paper attributes
    for paper_id, venue in read_paper_venues(paper_file):
        node_id = idmap[paper_id]
        refg.vs[node_id]['venue'] = venue

    # next add author ids
    for v in refg.vs:
        v['author_ids'] = []

    for author_id, paper_id in util.yield_csv_records(author_file):
        node_id = idmap[paper_id]
        refg.vs[node_id]['author_ids'].append(author_id)

    # Finally add edges from citation records
    citation_links = read_paper_references(refs_file, idmap)
    refg.add_edges(citation_links)

    # Save if requested
    if save:
        refg.write_picklez('%s.pickle.gz' % out_fname)
        refg.write_graphmlz('%s.graphml.gz' % out_fname)

    return refg


def get_paper_edges(refg, paper_id, author_id):
    """Return a list of author-to-author edges for each paper."""
    node = refg.vs[paper_id]
    neighbors = node.neighbors()
    author_lists = [n['author_ids'] for n in neighbors]
    if not author_lists: return []
    authors = reduce(lambda x,y: x+y, author_lists)
    return zip([author_id]*len(authors), authors)


def get_edges(author_file, idmap_fname, graph_picklez_file):
    """Return all edges from a file in which each line contains an (author,
    paper) pair."""
    idmap = util.read_idmap(idmap_fname)
    refg = igraph.Graph.Read_Picklez(graph_picklez_file)
    records = util.yield_csv_records(author_file)
    rows = ((refg, idmap[paper_id], author_id)
            for author_id, paper_id in records)

    while True:
        edges = get_paper_edges(*rows.next())
        for edge in edges:
            yield edge


def build_author_citation_graph(author_file, idmap_fname, graph_picklez_file,
                                outfile='author-citation-graph.graphml.gz')
    """Build the author citation graph from the paper citation graph and the
    authorship csv records.

    :param str author_file: Filename containing authorship records. These should
        consist of (author_id, paper_id) csv records, one per line.
    :param str idmap_fname: Filename of paper id to node id csv mapping. This
        should contain (paper_id, node_id) records.
    :param str graph_picklez_file: Filename of paper citation graph in gzipped
        pickle format. The pickled version is required in order to access the
        `author_ids` attributes.
    :param str outfile: Filename to write the author-citation-graph to.
    """
    df = pd.read_csv(author_file, header=0, usecols=(0,))
    author_ids = df['author_id'].values
    edges = get_edges(author_file, idmap_fname, graph_picklez_file)
    nodes = (str(author_id) for author_id in author_ids)
    authorg = util.build_undirected_graph(nodes, edges)

    # Now write the graph to gzipped graphml file.
    if not outfile.endswith('.graphml.gz'):
        while '.' in outfile:
            outfile = os.path.splitext(outfile)[0]
        outfile = '%s.graphml.gz'
    authorg.write_graphmlz(outfile)

    # Finally, save the ID map.
    save_id_map(authorg, 'author-id-to-node-id-map')


def write_lcc(graph, outfile, idmap_fname='lcc-author-id-to-node-id-map'):
    components = graph.components()
    lcc = components.giant()
    lcc.write_graphmlz('%s.graphml.gz' % outfile)
    lcc.write_edgelist('%s-edgelist.txt' % outfile)
    save_id_map(lcc, idmap_fname)


# -----------------------------------------------------------
# build up ground-truth communities using venue info for LCC
# -----------------------------------------------------------

def load_linked_venue_frame(author_file, paper_file, lcc_idmap_fname):
    """Join the author and paper data records in order to map authors to
    venues."""
    author_df = pd.read_table(
            author_file, sep=",", header=0,
            usecols=('author_id', 'paper_id'))
    paper_df = pd.read_table(
            paper_file, sep=",", header=0,
            usecols=('id', 'venue'))
    paper_df.columns = ('paper_id', 'venue')

    # filter authors down to those in LCC
    lcc_author_df = pd.read_csv(lcc_idmap_fname, header=0, usecols=(0,))
    lcc_author_ids = df['author_id'].values
    selection = author_df['author_id'].isin(lcc_author_ids)
    author_df = author_df[selection]
    merge_df = author_df.merge(paper_df)
    del merge_df['paper_id']  # only need (author_id, venue) pairs


def assign_venue_ids(author_venue_df, outfile='lcc-venue-id-map.csv')
    """Assign each venue an id and save the assignment."""
    unique_venues = author_venue_df['venue'].unique()
    unique_venues.sort()
    venue_map = {venue: vnum for vnum, venue in enumerate(unique_venues)}
    rows = ((vnum, venue) for venue, vnum in venue_map.iteritems())
    util.write_csv(outfile, ('venue_id', 'venue_name'), rows)
    return venue_map


def add_venues_to_graph(graph, author_venue_df, venue_map, outfile):
    # Use sets in order to ensure uniqueness.
    for v in lcc.vs:
        v['venues'] = set()

    # Add the venue IDs to the node venue sets.
    for rownum, (author_id, venue) in author_venue_df.iterrows():
        node_id = lcc_idmap[str(author_id)]
        venue_id = venue_map[venue]
        graph.vs[node_id]['venues'].add(venue_id)

    # Convert the sets to tuples.
    for v in graph.vs:
        v['venues'] = tuple(v['venues'])

    # save a copy of the graph with venue info
    if not outfile.endswith('.pickle.gz'):
        while '.' in outfile:
            outfile = os.path.splitext(outfile)[0]
        outfile = '%s.pickle.gz' % outfile
    graph.write_picklez(outfile)  # lcc-author-citation-graph


def build_ground_truth(graph, venue_map,
                       outfile='lcc-ground-truth-by-venue.txt',
                       save_by_author='lcc-author-venues.txt'):
    """Build ground truth communities from the graph using the venue id
    mapping.
    
    :param {igraph.Graph} graph: The author citation graph with venues.
    :param dict venue_map: Mapping from venue names to venue ids.
    :param str outfile: Filename to write ground truth communities to.
    :param str save_by_author: If not empty or None, this filename is used to
        write a listing of each venue each author has published at. The line
        number corresponds to the author id, and the line contains a space
        separated list of venue ids.
    """
    communities = {venue_id: [] for venue_id in venue_map.itervalues()}
    for v in graph.vs:
        for venue_id in v['venues']:
            communities[venue_id].append(v.index)

    # save ground truth communities
    comms = sorted(communities.items())
    rows = (' '.join(map(str, comm)) for comm_num, comm in comms)
    with open(outfile, 'w') as f:
        f.write('\n'.join(rows))

    # save venue info for each author separately
    if save_by_author:
        records = sorted([(v.index, v['venues']) for v in graph.vs])
        rows = (' '.join(map(str, venues)) for node_id, venues in records)
        with open(save_by_author, 'w') as f:
            f.write('\n'.join(rows))
