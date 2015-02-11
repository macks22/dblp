"""
This module provides a data processing pipeline for the DBLP dataset.
The results include a paper citation graph, an author citation graph,
and several useful node attributes, including venues for both papers and
authors and term frequencies for author documents.
"""
import luigi

import config
import repdocs


class BuildDataset(luigi.Task):
    """This task triggers a complete build of the dataset. This will produce the
    repdocs, repdoc vectors, paper corpus, paper citation graph, author citation
    graph, venue ground truth communities, author repdocs, and the author repdoc
    corpus (both tf & tfidf).
    """
    start = luigi.IntParameter(default=None)
    end = luigi.IntParameter(default=None)

    def requires(self):
        yield repdocs.BuildLCCAuthorRepdocCorpusTfidf(self.start, self.end)


if __name__ == "__main__":
    luigi.run()
