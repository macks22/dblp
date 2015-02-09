"""
Produce CESNA/CODA/EDCAR formatted data files.
"""

import os
import gensim
import util


def write_coda_files(edgelist_fname):
    """CODA requires only a tsv edgelist."""
    outfile = '%s.tsv' % os.path.splitext(edgelist_fname)[0]
    util.swap_file_delim(edgelist_fname, ' ', outfile, '\t')


def write_cesna_files(
        term_idmap_fname, tf_corpus_fname,
        feat_outfile='lcc-repdoc-corpus-author-term-presence.tsv'):
    """CESNA requires the same edgelist as CODA, but also requires (1) (node_id
    \t term_id) pairs for all term features (2) (term_id \t term) pairs for all
    terms in the corpus note that because this is mm format, we need to subtract
    1 from all ids.
    """
    outfile = '%s.tsv' % os.path.splitext(term_idmap_fname)[0]
    util.swap_file_delim(term_idmap_fname, ',', outfile, '\t')

    # Write author features file (binary - term id indicates node has term)
    corpus = gensim.corpora.MmCorpus(tf_corpus_fname)
    with open(feat_outfile, 'w') as wf:
        docs_with_tf = (
            docnum, corpus.docbyoffset(offset)
            for docnum, offset in enumerate(corpus.index))
        docs_as_pairs = (
            zip([docnum] * len(doc), [term_id for term_id, _ in doc])
            for docnum, doc in docs_with_tf)
        docs_as_lines = (
            ['%s\t%s' % (docid, termid) for docid, termid in pairs]
            for pairs in docs_as_pairs)
        docs = ('\n'.join(lines) for lines in docs_as_lines)

        for doc in docs:
            wf.write('\n'.join(doc))
