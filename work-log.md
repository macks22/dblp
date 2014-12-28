2014-12-21
----------

1.  Found that only 1 records in papers.csv has an abstract
2.  Wrote a script to use crossref.org to search papers by title. Search results
    include a doi link which can be used to retrieve abstracts.
3.  Wrote a proof of concept abstract retreival tool.

2014-12-22
----------

Goals:

1.  Update papers.csv with abstracts from publications.txt. It seems evident
    that some were missed during the first round of parsing. Figure out why this
    happened, then enrich the existing database. Repopulate the papers.csv file
    from the database.
2.  Write a general-purpose tool for converting to MMF from one or more csv
    files. This will involve specifying the rows to be used for (1) row id (2)
    col id and (3) cell value.
3.  Review documentation for doctovec library, improving where ambiguous. Bring
    yourself back up to speed on how to use it.
4.  Incorporate gensim dictionary tools more tightly with doctovec in order to
    reduce re-learning overhead next time you do not use it for a while.
5.  Create an initial SENC-ready dataset from the DBLP data.

Notes:

1.  Only one abstract is present in publications.txt. So it seems the processing
    was fine, but the dataset itself seems limited. The v6 dataset actually has
    quite a few more abstracts than the v7 one. We can pull abstracts from there
    to fill in many of the missing ones in the v7 dataset.
3.  Complete
5.  First we need to construct the co-citation graph. This can be broken down
    into the following steps:
    1.  for each author, get a list of his papers (can be node attrs)
    2.  for each paper, construct a repdoc (use gensim and MM corpus)
    3.  for each paper authored by an author, get a list of references
        1.  for each reference, make an edge to all authors of that reference

    Construct a graph for the first item, with all author ids as nodes. Next,
    get the list of papers and make those attributes. After that we need to
    connect authors based on the following link structure:

    author -> paper -> references -> paper -> author

    Build a graph of papers, with edges being references. For each node, add the
    list of authors as attributes. Then for each node, visit immediate nieghbors
    and get their author lists. Make an edge from every author in the first list
    to every author in the second.

2014-12-23
----------

    Today was spent building the co-citation graph and the reference graph.

    The igraph package was used to accomplish this. Igraph uses internal ids
    which cannot be made to be the same as the author/paper ids. So when
    constructing the paper reference graph, the paper ids are included as 'name'
    attributes on the nodes, which are assigned contiguous integer IDs. However,
    when adding edges, one must have the igraph-assigned IDs of both nodes to
    connect. Therefore, it is very useful to have a dictionary mapping between
    the two.

    Perhaps the most efficient way to build such a dictionary is before the
    graph is even constructed. Consider a list of all ids to be added.
    Consequently, the indices of these ids are equivalent to the IDs igraph will
    assign during a call to `add_vertices`. Therefore, the mapping can be made
    from this same array via enumeration. Since the utility of the map will be
    to lookup indices by ids, constructing a dictionary with val: index is best.

    The situation is simpler for authors, since the author ids were assigned
    from a contiguous sequence starting from 0. This was possible because author
    indices were assigned by Mack during the data parsing. It was not possible
    for paper ids because these were assigned by the arnetminer folks.

    I initially had some trouble adding edges to the author graph, since it has
    quite a large number of edges. However, it turns out the igraph implemention
    of `add_edges` is almost entirely in C. According to the docs, it has
    O(|V|+|E|) complexity. So all edges should be added at the same time.

    mapfile = open('paper-id-map.csv')
    mapreader = csv.reader(mapfile)
    idmap = {int(r[0]): int(r[1]) for r in mapreader}

    def lookup_node_id(paper_id):
        return idmap[paper_id]

    f = open('authors.csv')
    reader = csv.reader(f)
    records = ((int(paper), int(person)) for paper, person in reader)
    rows = ((lookup_node_id(paper), person) for paper, person in records)

    def get_edges(rows):
        while True:
            edges = get_paper_edges(*rows.next())
            for edge in edges:
                yield edge

    def get_paper_edges(paper, person):
        node = refg.vs[paper]
        neighbors = node.neighbors()
        author_lists = [n['authors'] for n in neighbors]
        if not author_lists:
            return []

        authors = reduce(lambda x,y: x+y, author_lists)
        return zip([person]*len(authors), authors)

2014-12-24
----------

Status check. We now have:

1.  repdocs for each paper as BoW with preprocessing done
2.  the co-citation graph
3.  the edge list for the co-citation graph
4.  the paper reference graph

What's left:

1.  repdocs per author (node attributes for co-citation graph)
2.  MMF representation of node features
3.  Feature id/name mapping file

Good news! We've now got all the things. They're on ARGO. You can get them after
Christmas.

2014-12-25
----------

The things have been retrieved. I'm now training an LDA model on ARGO. Hopefully
it doesn't time out.

2014-12-26
----------

Let's now better understand our data set. For that purpose, we should get some
key graph metrics from the full graph.

Next big step: LCC reduced dataset. The graph for this has been transferred to
ARGO.

2014-12-27
----------

Disaster! Or rather a bunch of wasted time. It seems that the igraph.Graph
method `write_graphmlz` changes node attributes. In particular, it messed up the
node 'name' attribute, which is quite important.

I have submitted a bug report for this. For now, we can circumvent the issue by
converting the paper and author IDs to strings before writing.
