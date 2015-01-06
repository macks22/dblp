
# ---------------------------------------------------------
# filter data to a range of years
# ---------------------------------------------------------

import os, sys

start = 2009
end = 2014

# filter the papers by year
df = pd.read_csv('paper-with-venue-and-year.csv')
df['year'] = df['year'].astype(int)
df = df[(df['year'] >= start) & (df['year'] <= end)]

# load authors and refs for later
author_df = pd.read_csv('author.csv')
person_df = pd.read_csv('person.csv')
refs_df = pd.read_csv('refs.csv')

# change to new dir to prepare to write new set of files
newdir = 'newdata'
init_wd = os.getcwd()
try: os.mkdir(newdir)
except OSError: pass
os.chdir(newdir)

# write new paper.csv file
df.to_csv('paper.csv', index=False)

# write new venue listing
rows = sorted([(venue,) for venue in df['venue'].unique()])
with open('venue.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(rows)

# write new year listing
rows = sorted([(year,) for year in df['year'].unique()])
with open('year.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(rows)

# filter authors and refs to only those in the filtered time range
paper_ids = df['id'].unique()
author_df = author_df[author_df['paper_id'].isin(paper_ids)]
author_ids = author_df['author_id'].unique()
person_df = person_df[person_df['id'].isin(author_ids)]
refs_df = refs_df[(refs_df['paper_id'].isin(paper_ids)) &
                  (refs_df['ref_id'].isin(paper_ids))]

# now write the filtered records
author_df.to_csv('author.csv', index=False)
person_df.to_csv('person.csv', index=False)
refs_df.to_csv('refs.csv', index=False)

# all done; restore previous working directory
os.chdir(init_wd)


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

import csv, pandas as pd

def write_csv(fname, header, rows):
    with open('%s.csv' % fname, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

# big memory demand
df = pd.read_csv('repdoc-by-paper-vectors.csv', index_col=(0,))
df.fillna('', inplace=True)

# read out authorship records
authors_file = 'author.csv'
author_df = pd.read_csv(authors_file, header=0, index_col=(0,))

# initialize repdoc dictionary from complete list of person ids
author_ids = author_df.index.unique()
repdocs = {i: [] for i in author_ids}

# build up repdocs for each author
for person_id, paper_id in author_df.itertuples():
    doc = df.loc[paper_id]['doc']
    repdocs[person_id].append(doc)

# save repdocs
rows = ((person_id, '|'.join(docs)) for person_id, docs in repdocs.iteritems())
write_csv('repdoc-by-author-vectors', ('author_id', 'doc'), rows)

# -----------------------------------------------------------
# build paper citation graph using paper.csv
# -----------------------------------------------------------

import igraph, csv

# get paper ids from csv file and add to graph
refg = igraph.Graph()
papers_file = 'paper.csv'

with open(papers_file) as f:
    reader = csv.reader(f)
    reader.next()
    paper_ids = (r[0] for r in reader)
    refg.add_vertices(paper_ids)

# paper id to node id mapping; make and save
idmap = {v['name']: v.index for v in refg.vs}
rows = idmap.iteritems()
write_csv('paper-id-to-node-id-map', ('paper_id', 'node_id'), rows)

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
refg.write_graphmlz('paper-cocitation-graph.graphml.gz')

# -----------------------------------------------------------
# build author cocitation graph using paper cocitation graph
# -----------------------------------------------------------

# reload idmap and paper cocitation graph
# -----------------------------------------------------------------------------
fname = 'paper-id-to-node-id-map.csv'
mapfile = open(fname)
mapreader = csv.reader(mapfile)
mapreader.next()
idmap = {r[0]: int(r[1]) for r in mapreader}

refg = igraph.Graph.Read_Picklez('paper-cocitation-graph.pickle.gz')
assert(len(idmap) == len(refg.vs))
# -----------------------------------------------------------------------------
# start here if continuing from above

import pandas as pd

# get person IDs
df = pd.read_csv('person.csv', header=0, usecols=(0,))
author_ids = df['id'].values

# get author records to build edges from
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
def build_undirected_graph(nodes, edges):
    graph = igraph.Graph()
    graph.add_vertices(nodes)
    graph.add_edges(edges)
    graph.simplify()
    return graph

f = open('author.csv')
reader = csv.reader(f)
reader.next()
rows = ((idmap[paper_id], author_id) for author_id, paper_id in reader)
edges = get_edges(rows)
nodes = (str(author_id) for author_id in author_ids)
authorg = build_undirected_graph(nodes, edges)
f.close()

authorg.write_graphmlz('author-cocitation-graph.graphml.gz')

def save_id_map(graph, outfile, idname='author'):
    """Save vertex ID to vertex name mapping and then return it."""
    first_col = '%s_id' % idname
    idmap = {v['name']: v.index for v in graph.vs}
    rows = idmap.iteritems()
    with open('%s.csv' % outfile, 'w') as f:
        writer = csv.writer(f)
        writer.writerow((first_col, 'node_id'))
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

import igraph, pandas as pd

# load author, paper, venue info
author_df = pd.read_table(
        'author.csv', sep=",", header=0,
        usecols=('author_id', 'paper_id'))
paper_df = pd.read_table(
        'paper.csv', sep=",", header=0,
        usecols=('id', 'venue'))
paper_df.columns = ('paper_id', 'venue')

# filter authors down to those in LCC
lcc_author_ids = set([int(v['name']) for v in lcc.vs])
selection = author_df['author_id'].isin(lcc_author_ids)
author_df = author_df[selection]
merge_df = author_df.merge(paper_df)
del merge_df['paper_id']

# assign each venue an id and save the assignment
unique_venues = merge_df['venue'].unique()
unique_venues.sort()
venue_map = {venue: vnum for vnum, venue in enumerate(unique_venues)}
rows = ((vnum, venue) for venue, vnum in venue_map.iteritems())
with open('lcc-venue-id-map.csv', 'w') as wf:
    venue_writer = csv.writer(wf)
    venue_writer.writerow(('venue_id', 'venue_name'))
    venue_writer.writerows(rows)

# add venue information to LCC
for v in lcc.vs:
    v['venues'] = set()

for rownum, row in merge_df.iterrows():
    author_id, venue = row
    node_id = lcc_idmap[str(author_id)]
    venue_id = venue_map[venue]
    lcc.vs[node_id]['venues'].add(venue_id)

for v in lcc.vs:
    v['venues'] = tuple(v['venues'])

# save a copy of the graph with venue info
lcc.write_picklez('author-cocitation-graph-lcc.pickle.gz')

# build ground truth communities
communities = {venue_id: [] for venue_id in venue_map.itervalues()}
for v in lcc.vs:
    for venue_id in v['venues']:
        communities[venue_id].append(v.index)

# save ground truth communities
fname = 'lcc-ground-truth-by-venue.txt'
comms = sorted(communities.items())
rows = (' '.join(map(str, comm)) for comm_num, comm in comms)
with open(fname, 'w') as f:
    f.write('\n'.join(rows))

# save venue info for each author separately
fname = 'lcc-author-venues.txt'
records = sorted([(v.index, v['venues']) for v in lcc.vs])
rows = (' '.join(map(str, venues)) for node_id, venues in records)
with open(fname, 'w') as f:
    f.write('\n'.join(rows))

# ---------------------------------------------------------
# convert author repdocs to tf/tfidf corpuses
# ---------------------------------------------------------

import gensim, sys, csv, igraph

# filter authors down to those in LCC
df = pd.read_csv('lcc-author-id-to-node-id-map.csv', header=0, usecols=(0,))
lcc_author_ids = df['author_id'].values

# build dictionary of terms from repdocs
with open('repdoc-by-author-vectors.csv') as f:
    csv.field_size_limit(sys.maxint)
    reader = csv.reader(f)
    reader.next()
    corpus = (doc.split('|') for author_id, doc in reader
              if int(author_id) in lcc_author_ids)
    dictionary = gensim.corpora.Dictionary(corpus)

# save dictionary and term id mapping
dictionary.save('lcc-repdoc-corpus.dict')
rows = ((term_id, term.encode('utf-8'))
        for term, term_id in dictionary.token2id.iteritems())
with open('lcc-repdoc-corpus-term-id-map.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(('term_id', 'term'))
    writer.writerows(rows)

# write term frequency corpus
fname = 'lcc-repdoc-corpus-tf.mm'
with open('repdoc-by-author-vectors.csv') as f:
    reader = csv.reader(f)
    reader.next()
    corpus = (doc.split('|') for author_id, doc in reader)
    bow_corpus = (dictionary.doc2bow(doc) for doc in corpus)
    gensim.corpora.MmCorpus.serialize(fname, bow_corpus)

# write tfidf corpus
bow_corpus = gensim.corpora.MmCorpus(fname)
tfidf = gensim.models.TfidfModel(bow_corpus)
tfidf_corpus = tfidf[bow_corpus]
fname = 'lcc-repdoc-corpus-tfidf.mm'
gensim.corpora.MmCorpus.serialize(fname, tfidf_corpus)

