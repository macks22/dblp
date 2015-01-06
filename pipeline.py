
# ---------------------------------------------------------
# build repdocs for each paper
# ---------------------------------------------------------

import csv, doctovec
papers_file = 'paper.csv'

f = open(papers_file)
reader = csv.reader(f)
reader.next()
records = ((r[0], '%s %s' % (r[1], r[4])) for r in reader)
docs = ((docid, doc.decode('utf-8')) for docid, doc in records)

doc_file = open('repdoc-by-paper.csv', 'w')
vec_file = open('repdoc-by-paper-vectors.csv', 'w')
doc_writer = csv.writer(doc_file)
vec_writer = csv.writer(vec_file)
headers = ('paper_id', 'doc')
doc_writer.writerow(headers)
vec_writer.writerow(headers)

for docid, doc in docs:
    doc_writer.writerow((docid, doc.encode('utf-8')))
    vector = doctovec.doctovec(doc)
    concat = '|'.join(vector).encode('utf-8')
    vec_writer.writerow((docid, concat))

f.close()
doc_file.close()
vec_file.close()

# ---------------------------------------------------------
# parse repdocs into author vectors
# ---------------------------------------------------------

from pandas import DataFrame
import csv

# big memory demand
df = DataFrame.from_csv('repdoc-by-paper-vectors.csv')
df.fillna('')

# read out authorship records
authors_file = 'author.csv'
f = open(authors_file)
reader = csv.reader(f)
reader.next()
pairs = ((int(person_id), int(paper_id)) for person_id, paper_id in reader)

# initialize repdoc dictionary from complete list of person ids
person_file = 'person.csv'
with open(person_file) as f:
    reader = csv.reader(f)
    reader.next()
    ids = (int(r[0]) for r in reader)
    repdocs = {i: [] for i in ids}

# build up repdocs for each author
for person_id, paper_id in pairs:
    doc = df.loc[paper_id]['doc']
    if isinstance(doc, basestring) and doc:
        repdocs[person_id].append(doc)

# save repdocs
out = open('repdoc-by-author-vectors.csv', 'w')
writer = csv.writer(out)
writer.writerow(('author_id', 'doc'))
rows = ((person_id, '|'.join(docs)) for person_id, docs in repdocs.iteritems())
writer.writerows(rows)
out.close()

# ---------------------------------------------------------
# convert author repdocs to tf/tfidf corpuses
# ---------------------------------------------------------

import gensim, sys

# build dictionary of terms from repdocs
f = open('repdoc-by-author-vectors.csv')
csv.field_size_limit(sys.maxint)
reader = csv.reader(f)
reader.next()
corpus = (doc.split('|') for author_id, doc in reader)
dictionary = gensim.corpora.Dictionary(corpus)
dictionary.save('repdoc-corpus.dict')

# write term frequency corpus
f.seek(0)
reader.next()
corpus = (doc.split('|') for author_id, doc in reader)
bow_corpus = (dictionary.doc2bow(doc) for doc in corpus)
fname = 'repdocs-corpus-tf.mm'
gensim.corpora.MmCorpus.serialize(fname, bow_corpus)
f.close()

# write tfidf corpus
bow_corpus = gensim.corpora.MmCorpus(fname)
tfidf = gensim.models.TfidfModel(bow_corpus)
tfidf_corpus = tfidf[bow_corpus]
fname = 'repdocs-corpus-tfidf.mm'
gensim.corpora.MmCorpus.serialize(fname, tfidf_corpus)

# -----------------------------------------------------------
# build paper citation graph using paper.csv
# -----------------------------------------------------------

import igraph, csv

# get paper ids from csv file
refg = igraph.Graph()
papers_file = 'paper.csv'
with open(papers_file) as f:
    reader = csv.reader(f)
    reader.next()
    paper_ids = (r[0] for r in reader)

    # add all papers to graph
    refg.add_vertices(paper_ids)

# paper id to node id mapping
idmap = {v['name']: v.index for v in refg.vs}
assert(len(idmap) == len(refg.vs))

# now add venues to vertices as paper attributes
with open(papers_file) as f:
    reader = csv.reader(f)
    reader.next()
    records = ((r[0], r[2]) for r in reader)
    for paper_id, venue in records:
        node_id = idmap[paper_id]
        refg.vs[node_id]['venue'] = venue

# finally add author ids
for v in refg.vs:
    v['author_ids'] = []

authors_file = 'author.csv'
with open(authors_file) as f:
    reader = csv.reader(f)
    reader.next()
    for author_id, paper_id in reader:
        node_id = idmap[paper_id]
        refg.vs[node_id]['author_ids'].append(author_id)

# add edges from graph references
def iteredges(rows):
    """Filter out references to papers outside dataset."""
    for paper_id, ref_id in rows:
        try: yield (idmap[paper_id], idmap[ref_id])
        except: pass

refs_file = 'refs.csv'
with open(refs_file) as f:
    reader = csv.reader(f)
    reader.next()
    edges = iteredges(reader)
    refg.add_edges(edges)

# save graph
refg.write_picklez('paper-cocitation-graph.pickle.gz')

# save id map from paper ids to node ids
rows = idmap.iteritems()
with open('paper-id-to-node-id-map.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(('paper_id', 'node_id'))
    writer.writerows(rows)

# -----------------------------------------------------------
# build author cocitation graph using paper cocitation graph
# -----------------------------------------------------------

# reload idmap and paper cocitation graph
# -----------------------------------------------------------------------------
fname = 'raw-id-to-node-id-map.csv'
mapfile = open(fname)
mapreader = csv.reader(mapfile)
mapreader.next()
idmap = {int(r[0]): int(r[1]) for r in mapreader}

refg = igraph.Graph.Read_picklez('paper-cocitation-graph.pickle.gz')
assert(len(idmap) == len(refg.vs))
# -----------------------------------------------------------------------------
# start here if continuing from above

# get person IDs
with open('person.csv') as f:
    reader = csv.reader(f)
    reader.next()
    author_ids = [author_id for author_id, name in reader]

# get author records to build edges from
with open('author.csv') as f:
    reader = csv.reader(f)
    reader.next()
    rows = [(idmap[paper_id], author_id) for author_id, paper_id in reader]

def get_paper_edges(paper_id, author_id):
    """Return a list of author-to-author edges for each paper."""
    node = refg.vs[paper_id]
    neighbors = node.neighbors()
    author_lists = [n['author_ids'] for n in neighbors]
    if not author_lists: return []
    authors = reduce(lambda x,y: x+y, author_lists)
    return zip([author_id]*len(authors), authors)

def get_edges(rows):
    """Return all edges from the list of (author, paper) rows."""
    while True:
        edges = get_paper_edges(*rows.next())
        for edge in edges:
            yield edge

# build the author cocitation graph and save it
authorg = igraph.Graph()
edges = get_edges(rows)
authorg.add_vertices(author_ids)
authorg.add_edges(edges)
authorg.simplify()  # remove duplicate edges
authorg.write_graphmlz('author-cocitation-graph.graphml.gz')


def save_id_map(graph, outfile):
    """Save vertex ID to vertex name mapping."""
    idmap = {v['name']: v.index for v in graph.vs}
    rows = idmap.iteritems()
    with open('%s.csv' % outfile, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(('author_id', 'node_id'))
        writer.writerows(rows)
    return idmap

# save author id to node id map
author_idmap = save_id_map(authorg, 'author-id-to-node-id-map')

# extract the largest strongly connected component and save that as well
components = authorg.components()
lcc = components.giant()

# save the graph and its id mapping
lcc.write_graphmlz('author-cocitation-graph-lcc.graphml.gz')
lcc.write_edgelist('author-cocitation-graph-edgelist.txt')
lcc_idmap = save_id_map(lcc, 'lcc-author-id-to-node-id-map')


# -----------------------------------------------------------
# build up ground-truth communities using venue info for LCC
# -----------------------------------------------------------

import pandas as pd

# load paper, venue info
headers = ('id', 'venue')
df = pd.read_table(
    'paper.csv', sep=",", header=0,
    index_col=('id'),
    usecols=headers, names=headers)

# assign each venue an id and save the assignment
unique_venues = df['venue'].unique()
venue_map = {venue: vnum for vnum, venue in enumerate(unique_venues)}
rows = ((vnum, venue) for venue, vnum in venue_map.iteritems())
with open('venue-id-map.csv', 'w') as wf:
    venue_writer = csv.writer(wf)
    venue_writer.writerow(('venue_id', 'venue_name'))
    venue_writer.writerows(rows)

# add venue information to LCC
for v in lcc.vs:
    v['venues'] = []

for venue in df['venue']:
    venue_id = venue_map[venue]
    node_id = lcc_idmap[author_id]
    node = lcc.vs[node_id]
    node['venues'].append(venue_id)

# save a copy of the graph with venue info
lcc.write_picklez('author-cocitation-graph-lcc.pickle.gz')

# build ground truth communities
communities = {venue_id: [] for venue_id in venue_map.values()}
for v in lcc.vs:
    for venue in v['venues']:
        communities[venue].append(v.index)

# save ground truth communities
fname = 'lcc-ground-truth-by-venue.txt'
comms = sorted(communities.items())
with open(fname, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(('venue_id', 'node_id'))
    for venue_id, node_ids in communities.iteritems():
        rows = [(venue_id, node_id) for node_id in node_ids]
        writer.writerows(rows)

# save venue info for each author separately
fname = 'lcc-author-venues.csv'
records = ((v.index, v['venues']) for v in lcc.vs)
with open(fname, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(('node_id', 'venue_id'))
    for node_id, venue_ids in records:
        rows = [(node_id, venue_id) for venue_id in venue_ids]
        writer.writerows(rows)

