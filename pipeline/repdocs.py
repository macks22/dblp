import os
import sys
import csv
import logging

import pandas as pd
import gensim
import luigi

import doctovec
import util
import aminer
import filtering
import config


class YearFilterableTask(util.YearFilterableTask):
    @property
    def base_dir(self):
        return config.repdoc_dir


class BuildPaperRepdocs(YearFilterableTask):
    """Build representative documents for each paper in the DBLP corpus."""

    def requires(self):
        return filtering.FilteredCSVPapers(self.start, self.end)

    @property
    def base_paths(self):
        return 'repdoc-by-paper.csv'

    def read_paper_repdocs(self):
        paper_file = self.input()
        for record in util.iter_csv_fwrapper(paper_file):
            repdoc = '%s %s' % (record[1], record[4])
            yield (record[0], repdoc.decode('utf-8'))

    def run(self):
        """The repdoc for a single paper consists of its title and abstract,
        concatenated with space between. The paper records are read from a csv
        file and written out as (paper_id, repdoc) pairs.
        """
        docs = self.read_paper_repdocs()
        rows = ((docid, doc.encode('utf-8')) for docid, doc in docs)
        util.write_csv_to_fwrapper(self.output(), ('paper_id', 'doc'), rows)


class BuildPaperRepdocVectors(YearFilterableTask):
    """Vectorize paper repdocs, preprocessing terms."""

    def requires(self):
        return BuildPaperRepdocs(self.start, self.end)

    @property
    def base_paths(self):
        return 'repdoc-by-paper-vectors.csv'

    def run(self):
        repdocs = util.iter_csv_fwrapper(self.input())
        docs = ((docid, doc.decode('utf-8')) for docid, doc in repdocs)
        vecs = ((docid, doctovec.vectorize(doc)) for docid, doc in docs)
        rows = ((docid, '|'.join(doc).encode('utf-8')) for docid, doc in vecs)
        util.write_csv_to_fwrapper(self.output(), ('paper_id', 'doc'), rows)


class BuildPaperRepdocDictionary(YearFilterableTask):
    """Build a dictionary to index the paper repdoc corpus."""

    def requires(self):
        return BuildPaperRepdocVectors(self.start, self.end)

    @property
    def base_paths(self):
        return 'repdoc-by-paper-corpus.dict'

    def read_repdocs(self):
        records = util.iter_csv_fwrapper(self.input())
        return (doc.split('|') for _, doc in records)

    def run(self):
        repdocs = self.read_repdocs()
        dictionary = gensim.corpora.Dictionary(repdocs)
        logging.info('term count in paper repdoc corpus pre-filtering: %d' %
                     len(dictionary))
        dictionary.filter_extremes(2, 1, len(dictionary))
        logging.info('term count in paper repdoc corpus post-filtering: %d' %
                     len(dictionary))
        dictionary.save(self.output().path)


class BuildPaperRepdocCorpus(YearFilterableTask):
    """Build BoW representation and save it in MM format."""

    def requires(self):
        return (BuildPaperRepdocDictionary(self.start, self.end),
                BuildPaperRepdocVectors(self.start, self.end))

    @property
    def base_paths(self):
        return 'repdoc-by-paper-corpus.mm'

    def run(self):
        dict_file, vecs_file = self.input()
        dictionary = gensim.corpora.Dictionary.load(dict_file.path)
        records = util.iter_csv_fwrapper(vecs_file)
        repdoc_corpus = (doc.decode('utf-8').split('|') for _, doc in records)
        bow_corpus = (dictionary.doc2bow(doc) for doc in repdoc_corpus)
        gensim.corpora.MmCorpus.serialize(self.output().path, bow_corpus)


class WritePaperToRepdocIdMap(YearFilterableTask):
    """Map paper ids to contiguous indices. These can be used as document
    indices in the MM files, as well as node ids in the paper citation graph.
    """

    def requires(self):
        return BuildPaperRepdocVectors(self.start, self.end)

    @property
    def base_paths(self):
        return 'paper-id-to-repdoc-id-map.csv'

    def run(self):
        with self.input().open() as paper_file:
            paper_df = pd.read_csv(paper_file, header=0, usecols=(0,))
            paper_df.sort()

        with self.output().open('w') as outfile:
            paper_df['repdoc_id'] = paper_df.index
            paper_df.to_csv(outfile, index=False)


# ---------------------------------------------------------
# convert author repdocs to tf/tfidf corpuses
# ---------------------------------------------------------

def read_lcc_author_repdocs(author_repdoc_fname, lcc_idmap_fname):
    """Read and return an iterator over the author repdoc corpus, which excludes
    the authors not in the LCC.

    :param str author_repdoc_fname: Filename of csv author repdoc records. The
        repdocs should be a string of terms, where each term is separated by a
        '|' character.
    :param str lcc_idmap_fname: Filename of LCC author id to node id mapping.
    """
    lcc_author_df = pd.read_csv(lcc_idmap_fname, header=0, usecols=(0,))
    lcc_author_ids = df['author_id'].values
    csv.field_size_limit(sys.maxint)
    records = util.yield_csv_records(author_repdoc_fname)
    return (doc.split('|') for author_id, doc in records
            if int(author_id) in lcc_author_ids)


def build_author_repdoc_dictionary(author_repdoc_fname, lcc_idmap_fname,
                                   outfile='lcc-repdoc-corpus.dict'):
    corpus = read_lcc_author_repdocs(author_repdoc_fname, lcc_idmap_fname)
    dictionary = gensim.corpora.Dictionary(corpus)

    # save dictionary and term id mapping
    dictionary.save(outfile)
    rows = [(term_id, term.encode('utf-8'))
            for term, term_id in dictionary.token2id.iteritems()]
    rows = sorted(rows)  # put ids in order
    idmap_fname = '%s-term-id-map' % os.path.splitext(outfile)[0]
    util.write_csv(idmap_fname, ('term_id', 'term'), rows)


def build_author_tf_corpus(author_repdoc_fname, lcc_idmap_fname, dictionary,
                           outfile='lcc-repdoc-corpus-tf.mm'):
    # write term frequency corpus
    corpus = read_lcc_author_repdocs(author_repdoc_fname, lcc_idmap_fname)
    bow_corpus = (dictionary.doc2bow(doc) for doc in corpus)
    gensim.corpora.MmCorpus.serialize(outfile, bow_corpus)


def build_author_tfidf_corpus(bow_corpus_fname,
                              outfile='lcc-repdoc-corpus-tfidf.mm'):
    bow_corpus = gensim.corpora.MmCorpus(bow_corpus_fname)
    tfidf = gensim.models.TfidfModel(bow_corpus)
    tfidf_corpus = tfidf[bow_corpus]
    gensim.corpora.MmCorpus.serialize(outfile, tfidf_corpus)


if __name__ == "__main__":
    luigi.run()
