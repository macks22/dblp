import csv

import pandas as pd
import igraph
import luigi

import util
import aminer
import filtering
import config


class YearFilterableTask(util.YearFilterableTask):
    @property
    def base_dir(self):
        return config.graph_dir


class BuildPaperCitationGraph(YearFilterableTask):
    """Build the citation graph for all papers. For each paper, a link is drawn
    between it and all of its references. The venue of its publication and its
    list of authors are added as node attributes. The graph is saved in both
    pickle.gz and graphml.gz format. Both formats are used because pickle allows
    inclusion of the `author_ids` attributes of the nodes, which consists of a
    list of all authors of the paper. Graphml on the other hand, is a simpler
    and more space-efficient format, which will only save the venues as node
    attributes.
    """

    def requires(self):
        # TODO: consider using single dependency on FilterPapersToYearRange
        return (filtering.FilteredCSVPapers(self.start, self.end),
                filtering.FilteredCSVRefs(self.start, self.end),
                filtering.FilterAuthorshipsToYearRange(self.start, self.end))

    @property
    def papers_file(self):
        return self.input()[0]

    @property
    def refs_file(self):
        return self.input()[1]

    @property
    def author_file(self):
        return self.input()[2]

    @property
    def base_paths(self):
        return ('paper-citation-graph.pickle.gz',
                'paper-citation-graph.graphml.gz',
                'paper-id-to-node-id-map.csv')

    @property
    def pickle_output_file(self):
        return self.output()[0]

    @property
    def graphml_output_file(self):
        return self.output()[1]

    @property
    def idmap_output_file(self):
        return self.output()[2]

    def read_paper_vertices(self):
        """Iterate through paper IDs from the paper csv file."""
        with self.papers_file.open() as papers_file:
            papers_df = pd.read_csv(papers_file, header=0, usecols=(0,))
            return papers_df['id'].values

    def read_paper_venues(self):
        """Iterate through (paper_id, venue) pairs from the paper csv file."""
        for record in util.iter_csv_fwrapper(self.papers_file):
            yield (record[0], record[2])

    def read_paper_references(self, idmap):
        """Filter out references to papers outside dataset."""
        for paper_id, ref_id in util.iter_csv_fwrapper(self.refs_file):
            try: yield (idmap[paper_id], idmap[ref_id])
            except: pass

    def run(self):
        refg = igraph.Graph()
        nodes = self.read_paper_vertices()
        refg.add_vertices(nodes)

        # Build and save paper id to node id mapping
        idmap = {str(v['name']): v.index for v in refg.vs}
        rows = sorted(idmap.items())
        util.write_csv_to_fwrapper(
            self.idmap_output_file, ('paper_id', 'node_id'), rows)

        # Now add venues to nodes as paper attributes
        for paper_id, venue in self.read_paper_venues():
            node_id = idmap[paper_id]
            refg.vs[node_id]['venue'] = venue

        # next add author ids
        for v in refg.vs:
            v['author_ids'] = []

        for author_id, paper_id in util.iter_csv_fwrapper(self.author_file):
            node_id = idmap[paper_id]
            refg.vs[node_id]['author_ids'].append(author_id)

        # Finally add edges from citation records
        citation_links = self.read_paper_references(idmap)
        refg.add_edges(citation_links)

        # Save in both pickle and graphml formats
        refg.write_picklez(self.pickle_output_file.path)
        refg.write_graphmlz(self.graphml_output_file.path)
        return refg


class PickledPaperCitationGraph(YearFilterableTask):
    def requires(self):
        return BuildPaperCitationGraph(self.start, self.end)

    def output(self):
        pickle_file = self.input()[0]
        return luigi.LocalTarget(pickle_file.path)


class PaperCitationGraphIdmap(YearFilterableTask):
    def requires(self):
        return BuildPaperCitationGraph(self.start, self.end)

    def output(self):
        idmap_file = self.input()[2]
        return luigi.LocalTarget(idmap_file.path)


class BuildAuthorCitationGraph(YearFilterableTask):
    """Build the author citation graph from the paper citation graph and the
    authorship csv records.
    """

    def requires(self):
        return (filtering.FilterAuthorshipsToYearRange(self.start, self.end),
                PaperCitationGraphIdmap(self.start, self.end),
                PickledPaperCitationGraph(self.start, self.end))

    @property
    def author_file(self):
        return self.input()[0]

    @property
    def paper_idmap_file(self):
        return self.input()[1]

    @property
    def paper_graph_file(self):
        return self.input()[2]

    @property
    def base_paths(self):
        return ('author-citation-graph.graphml.gz',
                'author-id-to-node-id-map.csv')

    def read_author_ids(self):
        """Read author ids from author file and return as strings (for easy
        reference when adding edges).
        """
        with self.author_file.open() as f:
            df = pd.read_csv(f, header=0, usecols=(0,))
            return df['author_id'].astype(str).values

    def get_edges(self):
        """Return all edges from a file in which each line contains an (author,
        paper) pair."""
        records = util.iter_csv_fwrapper(self.paper_idmap_file)
        idmap = {record[0]: int(record[1]) for record in records}
        refg = igraph.Graph.Read_Picklez(self.paper_graph_file.open())
        records = util.iter_csv_fwrapper(self.author_file)
        rows = ((refg, idmap[paper_id], author_id)
                for author_id, paper_id in records)

        while True:
            edges = self.get_paper_edges(*rows.next())
            for edge in edges:
                yield edge

    def get_paper_edges(self, refg, paper_id, author_id):
        """Return a list of author-to-author edges for each paper."""
        node = refg.vs[paper_id]
        neighbors = node.neighbors()
        author_lists = [n['author_ids'] for n in neighbors]
        if not author_lists: return []
        authors = reduce(lambda x,y: x+y, author_lists)
        return zip([author_id]*len(authors), authors)

    def run(self):
        nodes = self.read_author_ids()
        edges = self.get_edges()
        authorg = util.build_undirected_graph(nodes, edges)

        # Now write the graph to gzipped graphml file.
        graph_output_file, idmap_output_file = self.output()
        authorg.write_graphmlz(graph_output_file.path)

        # Finally, build and save the ID map.
        idmap = {v['name']: v.index for v in authorg.vs}
        rows = sorted(idmap.items())
        util.write_csv_to_fwrapper(
            idmap_output_file, ('author_id', 'node_id'), rows)


class WriteLCCAuthorCitationGraph(YearFilterableTask):
    """Find the largest connected component in the author citation graph."""

    def requires(self):
        return BuildAuthorCitationGraph(self.start, self.end)

    @property
    def base_paths(self):
        return ('lcc-author-citation-graph.graphml.gz',
                'lcc-author-citation-graph.edgelist.txt',
                'lcc-author-id-to-node-id-map.csv')

    def run(self):
        graphml_outfile, edgelist_outfile, idmap_outfile = self.output()
        author_graph_file, _ = self.input()

        # Read graph, find LCC, and save as graphml and edgelist
        authorg = igraph.Graph.Read_GraphMLz(author_graph_file.path)
        components = authorg.components()
        lcc = components.giant()
        lcc.write_graphmlz(graphml_outfile.path)
        lcc.write_edgelist(edgelist_outfile.path)

        # Build and save id map.
        idmap = {v['name']: v.index for v in lcc.vs}
        rows = sorted(idmap.items())
        util.write_csv_to_fwrapper(
            idmap_outfile, ('author_id', 'node_id'), rows)


class AuthorCitationGraphLCCGraphml(YearFilterableTask):
    def requires(self):
        return WriteLCCAuthorCitationGraph(self.start, self.end)

    def output(self):
        return self.input()[0]


class AuthorCitationGraphLCCIdmap(YearFilterableTask):
    def requires(self):
        return WriteLCCAuthorCitationGraph(self.start, self.end)

    def output(self):
        return self.input()[2]


class AddVenuesToAuthorCitationGraph(YearFilterableTask):
    """Build up ground truth communities using venue info for LCC."""

    def requires(self):
        return (AuthorCitationGraphLCCGraphml(self.start, self.end),
                AuthorCitationGraphLCCIdmap(self.start, self.end),
                filtering.FilteredCSVPapers(self.start, self.end),
                filtering.FilterAuthorshipsToYearRange(self.start, self.end))

    @property
    def base_paths(self):
        return ('lcc-author-citation-graph.pickle.gz',
                'lcc-venue-id-map.csv')

    def build_linked_venue_frame(self):
        """Join the author and paper data records in order to map authors to
        venues."""
        _, idmap_file, paper_file, author_file = self.input()

        # Read in authorship and venue records, with common paper_id for join
        with author_file.open() as author_fd, paper_file.open() as paper_fd:
            author_df = pd.read_table(
                    author_fd, sep=",", header=0,
                    usecols=('author_id', 'paper_id'))
            paper_df = pd.read_table(
                    paper_fd, sep=",", header=0,
                    usecols=('id', 'venue'))
            paper_df.columns = ('paper_id', 'venue')

        # filter authors down to those in LCC
        with idmap_file.open() as author_fd:
            lcc_author_df = pd.read_csv(author_fd, header=0, usecols=(0,))
            lcc_author_ids = lcc_author_df['author_id'].values

        # Filter based on LCC author ids
        selection = author_df['author_id'].isin(lcc_author_ids)
        author_df = author_df[selection]
        merge_df = author_df.merge(paper_df)
        del merge_df['paper_id']  # only need (author_id, venue) pairs
        return merge_df

    def assign_venue_ids(self, author_venue_df):
        """Assign each venue an id and save the assignment."""
        _, venue_map_file = self.output()
        unique_venues = author_venue_df['venue'].unique()
        unique_venues.sort()
        venue_map = {venue: vnum for vnum, venue in enumerate(unique_venues)}
        return venue_map

    def run(self):
        graph_file, idmap_file, paper_file, author_file = self.input()

        # Read in dependencies
        lcc = igraph.Graph.Read_GraphMLz(graph_file.path)
        author_venue_df = self.build_linked_venue_frame()
        venue_map = self.assign_venue_ids(author_venue_df)

        records = util.iter_csv_fwrapper(idmap_file)
        lcc_idmap = {record[0]: int(record[1]) for record in records}

        # Use sets in order to ensure uniqueness.
        for v in lcc.vs:
            v['venues'] = set()

        # Add the venue IDs to the node venue sets.
        for rownum, (author_id, venue) in author_venue_df.iterrows():
            node_id = lcc_idmap[str(author_id)]
            venue_id = venue_map[venue]
            lcc.vs[node_id]['venues'].add(venue_id)

        # Convert the sets to tuples.
        for v in lcc.vs:
            v['venues'] = tuple(v['venues'])

        # save a copy of the graph with venue info
        pickle_outfile, venue_map_outfile = self.output()
        lcc.write_picklez(pickle_outfile.path)  # lcc-author-citation-graph

        rows = ((vnum, venue) for venue, vnum in venue_map.iteritems())
        util.write_csv_to_fwrapper(
            venue_map_outfile, ('venue_id', 'venue_name'), rows)


class BuildGroundTruthCommunities(YearFilterableTask):
    """Build ground truth communities from the graph using the venue id
    mapping.
    """

    def requires(self):
        return AddVenuesToAuthorCitationGraph(self.start, self.end)

    @property
    def base_paths(self):
        return ('lcc-ground-truth-by-venue.txt',
                'lcc-author-venues.txt')

    def run(self):
        lcc_pickle_file, venue_map_file = self.input()

        # Read in the LCC graph
        lcc = igraph.Graph.Read_Picklez(lcc_pickle_file.path)

        # Build the community mapping:
        # each venue id is mapped to one or more node ids (the community)
        records = util.iter_csv_fwrapper(venue_map_file)
        communities = {int(venue_id): [] for venue_id, _ in records}
        for v in lcc.vs:
            for venue_id in v['venues']:
                communities[venue_id].append(v.index)

        # retrieve output files
        by_venue_file, by_author_file = self.output()

        # save ground truth communities
        comms = sorted(communities.items())
        rows = (' '.join(map(str, comm)) for comm_num, comm in comms)
        with by_venue_file.open('w') as f:
            f.write('\n'.join(rows))

        # save venue info for each author separately
        records = sorted([(v.index, v['venues']) for v in lcc.vs])
        rows = (' '.join(map(str, venues)) for node_id, venues in records)
        with by_author_file.open('w') as f:
            f.write('\n'.join(rows))


class BuildAllGraphData(luigi.Task):
    """Build all the graph data with one single task."""
    start = luigi.IntParameter(default=None)
    end = luigi.IntParameter(default=None)

    def requires(self):
        yield BuildGroundTruthCommunities(self.start, self.end)


if __name__ == "__main__":
    luigi.run()
