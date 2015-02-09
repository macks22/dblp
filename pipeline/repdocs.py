import sys
import csv
import logging
import pandas as pd
import gensim
import doctovec
import util


def read_paper_repdocs(papers_file):
    for record in util.yield_csv_records(papers_file):
        repdoc = '%s %s' % (record[1], record[4])
        yield (record[0], repdoc.decode('utf-8'))


def write_paper_repdocs(paper_file, outfile='repdoc-by-paper.csv'):
    """The repdoc for a single paper consists of its title and abstract,
    concatenated with space between. The paper records are read from a csv file
    and written out as (paper_id, repdoc) pairs.

    :param str paper_file: csv paper records filename.
    :param str outfile: csv paper repdoc output filename.
    """
    docs = read_paper_repdocs(paper_file)
    with open(outfile, 'w') as doc_file:
        doc_writer = csv.writer(doc_file)
        doc_writer.writerow(('paper_id', 'doc'))
        for docid, doc in docs:
            doc_writer.writerow((docid, doc.encode('utf-8')))


def write_paper_repdoc_vectors(paper_file,
                               outfile='repdoc-by-paper-vectors.csv'):
    """The repdoc for a single paper consists of its title and abstract,
    concatenated with space between. The paper records are read from a csv file
    and written out as (paper_id, repdoc) pairs.

    :param str paper_file: csv paper records filename.
    :param str outfile: csv paper repdoc output filename.
    """
    docs = read_paper_repdocs(paper_file)
    with open(outfile, 'w') as vec_file:
        vec_writer = csv.writer(vec_file)
        vec_writer.writerow(('paper_id', 'doc'))
        for docid, doc in docs:
            vector = doctovec.doctovec(doc)
            concat = '|'.join(vector).encode('utf-8')
            vec_writer.writerow((docid, concat))


def build_paper_repdoc_corpus(
        paper_vectors_file,
        outfile='repdoc-by-paper-corpus.mm'):
        filter_terms=True, no_below=2, no_above=1, feature_cap=None):
    """Build a BoW representation of the paper repdoc corpus and save it in
    Matrix Market (mm) format.

    :param str paper_vectors_file: Name of file to read paper repdocs from; the
        assumed format is (docid, repdoc_vector), where repdoc_vector is a
        string of terms, with each term separated by '|' characters.
    :param str outfile: Filename for .mm corpus output.
    :param bool filter_terms: If True, filter out terms from the corpus.
    :param int no_below: Terms are removed if they appear in less documents
        than this (minimum number for remaining term frequencies).
    :param float no_above: Terms are removed if they appear in more than this
        proportion of documents.
    :param feature_cap: Maximum number of terms to keep. If there are more than
        this many terms, the least frequent are removed until this cap is
        reached.
    """
    def read_repdocs(fname):
        records = util.yield_csv_records(fname)
        repdocs = (term_string.split('|') for _, term_string in records)

    # Build the dictionary of terms.
    repdocs = read_repdocs(paper_vectors_file)
    dictionary = gensim.corpora.Dictionary(repdocs)

    # Filter out terms if requested.
    if filter_terms:
        logging.info('term count in paper repdoc corpus pre-filtering: %d' %
                     len(dictionary))
        feature_cap = len(dictionary) if feature_cap is None else feature_cap
        dictionary.filter_extremes(no_below, no_above, feature_cap)
        logging.info('term count in paper repdoc corpus post-filtering: %d' %
                     len(dictionary))

    # Save the dictionary.
    fname = '%s.dict' % os.path.splitext(outfile)[0]
    dictionary.save(fname)

    # Now build the MM corpus.
    repdoc_corpus = read_repdocs(paper_vectors_file)
    bow_corpus = (dictionary.doc2bow(doc) for doc in repdoc_corpus)

    # And save it to the specified output file.
    if not outfile.endswith('.mm'):
        outfile = '%s.mm' % os.path.splitext(outfile)[0]
    gensim.corpora.MmCorpus.serialize(fname, bow_corpus)


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
