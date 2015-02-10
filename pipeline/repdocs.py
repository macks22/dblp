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
import config


class BuildPaperRepdocs(luigi.Task):
    """Build representative documents for each paper in the DBLP corpus."""

    def requires(self):
        return aminer.ParsePapersToCSV()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(config.data_dir, 'repdoc-by-paper.csv'))

    def read_paper_repdocs(self):
        paper_file = self.input()[0]
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


class BuildPaperRepdocVectors(luigi.Task):
    """Vectorize paper repdocs, preprocessing terms."""

    def requires(self):
        return BuildPaperRepdocs()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(config.data_dir, 'repdoc-by-paper-vectors.csv'))

    def run(self):
        repdocs = util.iter_csv_fwrapper(self.input())
        docs = ((docid, doc.decode('utf-8')) for docid, doc in repdocs)
        vecs = ((docid, doctovec.vectorize(doc)) for docid, doc in docs)
        rows = ((docid, '|'.join(doc).encode('utf-8')) for docid, doc in vecs)
        util.write_csv_to_fwrapper(self.input(), ('paper_id', 'doc'), rows)


class BuildPaperRepdocDictionary(luigi.Task):
    """Build a dictionary to index the paper repdoc corpus."""

    def requires(self):
        return BuildPaperRepdocVectors()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(config.data_dir, 'repdoc-by-paper-corpus.dict'))

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


class BuildPaperRepdocCorpus(luigi.Task):
    """Build BoW representation and save it in MM format."""

    def requires(self):
        return [BuildPaperRepdocDictionary(),
                BuildpaperRepdocVectors()]

    def output(self):
        return luigi.LocalTarget(
                os.path.join(config.data_dir, 'repdoc-by-paper-corpus.mm'))

    def run(self):
        dict_file, vecs_file = self.input()
        dictionary = gensim.corpora.Dictionary(dict_file.path)
        records = util.iter_csv_fwrapper(vecs_file)
        docs = ((docid, doc.decode('utf-8').split('|'))
                for docid, doc in records)
        bow_corpus = (dictionary.doc2bow(doc) for doc in repdoc_corpus)
        gensim.corpora.MmCorpus.serialize(self.output().path, bow_corpus)


class WritePaperToRepdocIdMap(luigi.Task):
    """Map paper ids to contiguous mm document indices."""

    def requires(self):
        return BuildPaperRepdocVectors()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(config.data_dir, 'paper-id-to-repdoc-id-map.csv'))

    def run(self):
        records = util.iter_csv_fwrapper(self.input())
        rows = ((paper_id, docid)
                for docid, (paper_id, _) in enumerate(records))
        util.write_csv_to_fwrapper(self.output(), ('paper_id', 'doc_id'), rows)


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
