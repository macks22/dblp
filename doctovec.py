"""
This module contains functions for parsing documents to vectors.
Functions for word cleaning and stemming are also included.

The main workhorse in here is the `vectorize` function. This will convert a
document directly to a list of preprocessed words. If you wish to skip some
preprocessing steps or introduce additional ones, see the `preprocess` and
`doctovec` functions.

:var set PUNCT:  The set of punctuation to be filtered from documents.
:var set STOPWORDS: The set of stopwords to be filtered from documents.

:type STEMMER: L{nltk.PorterStemmer}
:var  STEMMER: stemmer instance used to stem words in documents.

"""
import string
import itertools as it
import operator as op

import nltk.corpus
from nltk.tokenize import RegexpTokenizer
import numpy

try:
    from porterstemmer import Stemmer
    stem_word = Stemmer()
except ImportError:
    from nltk import PorterStemmer
    stem_word = PorterStemmer().stem_word

STOPWORDS_FILE = 'stopwords.txt'

# table to delete all punctuation characters using `translate`
PUNCT = set(string.punctuation)
TRANSLATION_TABLE = {ord(c): None for c in PUNCT}

# nltk has a list of 123 english stopwords
# STOPWORDS = set(nltk.corpus.stopwords.words('english'))
# but I had to expand on it to include contractions
# this list includes all nltk stopwords plus contractions plus a few extras
with open(STOPWORDS_FILE, 'r') as f:
    STOPWORDS = set(f.read().split())
STOPWORDS.add('br')  # get rid of </br> html tags (hackish)
STOPWORDS.add('')  # makes the pipeline squeaky clean (ass-covering)

"""
this tokenizer also removes all punctuation except apostrophes and hyphens
these should be filtered out afterwards so cases where hyphens are left
out are marked the same, such as:
    multi-processor and multiprocessor
    on-line and online
and so contractions can be handled appropriately.
"""
TOKENIZER = RegexpTokenizer("\w+[-']?\w*(-?\w*)*")
_word_tokenize = TOKENIZER.tokenize

strip = op.methodcaller('strip')
remove_hyphens = lambda word: word.replace('-', '')
simple_punct_remove = lambda word: word.replace('-', '').replace("'", '')


def word_tokenize(doc):
    return [remove_hyphens(word) for word in _word_tokenize(doc)]

def remove_punctuation_from_word(word):
    return word.translate(TRANSLATION_TABLE)

def is_stopword(word):
    return word in STOPWORDS

def starts_with_digits(word):
    return word[0].isdigit()

def clean_word(word):
    """Remove punctuation, lowercase, and strip whitespace from the word.

    :param str word: The word to clean.
    :return: The cleaned word.
    """
    return remove_punctuation_from_word(word).lower().strip()

def word_is_not_junk(word):
    """Applies a set of conditions to filter out junk words.

    :param str word: The word to test.
    :param int min_length: Drop words shorter than this; set to 0 to disable.
    :return: False if the word is junk, else True.

    """
    return not (is_stopword(word) or
                starts_with_digits(word) or
                (len(word) < 2))

def preprocess(wordlist, stopwords=True, digits=True, stem=True):
    """Perform preprocessing on a list of words.

    :param bool stopwords: If True, remove stopwords.
    :param bool digits: If True, remove words that start with digits.
    :param bool stem: If True, stem words using a Porter stemmer.
    """
    if stopwords: wordlist = it.ifilterfalse(is_stopword, wordlist)
    if digits: wordlist = it.ifilterfalse(starts_with_digits, wordlist)
    if stem: wordlist = it.imap(stem_word, wordlist)
    return list(wordlist)

def doctovec(doc, *args):
    """See `preprocess` for keyword args."""
    return preprocess(word_tokenize(doc.lower()), *args)

def clean_word_list(word_list):
    """The following cleaning operations are performed:

        1. punctuation removed
        2. word lowercased
        3. whitespace stripped

    Then words meeting these filtering criteria are removed:

        1. empty or only 1 character
        2. stopword
        3. all digits
        4. starts with digit

    Finally, all words are stemmed.

    :param (list of str) word_list: The list of words to clean.
    :rtype:  list of str
    :return: The cleaned, stemmed, filtered, list of words.
    """
    cleaned_words = [clean_word(w) for w in word_list]
    filtered_words = it.ifilter(word_is_not_junk, cleaned_words)
    stemmed_words = [stem_word(word) for word in filtered_words]
    return stemmed_words

def vectorize(doc):
    """Convert a document (string/unicode) into a filtered, cleaned,
    stemmed, list of words. See `doctovec` for a function with more options.

    :param str doc: The document to vectorize.
    :rtype:  list of str
    :return: The filtered, cleaned, stemmed, list of words.
    """
    return clean_word_list(word_tokenize(doc))

def write_vec(vec, filepath):
    """ Write a list of words to a txt file, seperated by newlines.

    :param (list of str) vec: The word vector to write to a file.
    :param str filepath: The absolute path of the file to write to.
    """
    with open(filepath, 'w') as f:
        for word in vec:
            f.write('{}\n'.format(word))
