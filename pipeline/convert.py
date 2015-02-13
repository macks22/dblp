"""
Produce CESNA/CODA/EDCAR formatted data files.
"""

import os

import gensim
import luigi

import util
import build_graphs
import repdocs
import config


class WriteCodaFiles(build_graphs.YearFilterableTask):
    """CODA requiers only a tsv edgelist."""

    def requires(self):
        return build_graphs.AuthorCitationGraphLCCEdgelist(self.start, self.end)

    @property
    def base_paths(self):
        return 'lcc-author-citation-graph.edgelist.tsv'

    def run(self):
        util.swap_file_delim(self.input(), ' ', self.output(), '\t')


class WriteTermIdMap(repdocs.YearFilterableTask):
    """Use the paper repdoc dictionary to write a term_id to term mapping."""

    def requires(self):
        return repdocs.BuildPaperRepdocDictionary(self.start, self.end)

    @property
    def base_paths(self):
        return 'lcc-repdoc-corpus-term-id-map.tsv'

    def run(self):
        dict = gensim.corpora.Dictionary.load(self.input().path)
        lines = ('\t'.join([unicode(term_id), term]).encode('utf-8')
                 for term, term_id in dict.token2id.items())
        with self.output().open('w') as outfile:
            outfile.write('\n'.join(lines))


class WriteLCCAuthorBinaryTerms(repdocs.YearFilterableTask):
    """Write SNAP-formatted feature file: binary terms per node (author)."""

    def requires(self):
        return repdocs.BuildLCCAuthorRepdocCorpusTf(self.start, self.end)

    @property
    def base_paths(self):
        return 'lcc-repdoc-corpus-author-term-presence.tsv'

    def run(self):
        corpus = gensim.corpora.MmCorpus(self.input().path)
        with self.output().open('w') as wf:
            docs_with_tf = (
                (docnum, corpus.docbyoffset(offset))
                for docnum, offset in enumerate(corpus.index))
            docs_as_pairs = (
                zip([docnum] * len(doc), [term_id for term_id, _ in doc])
                for docnum, doc in docs_with_tf)
            docs_as_lines = (
                ['%s\t%s' % (docnum, termid) for docnum, termid in pairs]
                for pairs in docs_as_pairs)
            docs = ('\n'.join(lines) for lines in docs_as_lines)

            for doc in docs:
                wf.write('\n'.join(doc))


class WriteCesnaFiles(repdocs.YearFilterableTask):
    """CESNA requires the same edgelist as CODA, but also requires (1) (node_id
    \t term_id) pairs for all term features (2) (term_id \t term) pairs for all
    terms in the corpus. Note that because this is mm format, we need to subtract
    1 from all ids.
    """

    def requires(self):
        yield WriteCodaFiles(self.start, self.end)
        yield WriteTermIdMap(self.start, self.end)
        yield WriteLCCAuthorBinaryTerms(self.start, self.end)


if __name__ == "__main__":
    luigi.run()
