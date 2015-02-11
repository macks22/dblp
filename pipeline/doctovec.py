"""
This module contains functions for parsing documents to vectors.
Functions for word cleaning and stemming are also included.

The main workhorse in here is the `vectorize` function. This will convert a
document directly to a list of preprocessed words. See the docstring for exact
preprocessing steps. If you wish to skip some preprocessing steps or introduce
additional ones, see the `preprocess` and `doctovec` functions.

Here are some other things you may want to do:
    1.  Adjust stopword filtering: Change value of `STOPWORDS_FILE` to point to
        a different stopwords file.
    2.  Adjust punctuation filtering: Remove or add to the punctuation set
        `PUNCT`. The default is the `punctuation` list from the `string` module.
        One option might be to remove hyphens from this list, since hyphenated
        words are not tokenized as separate words. So keeping the hyphens might
        improve readability of results, if that is important.  However, it might
        also result in terms such as multi-processor and multiprocessor being
        considered different, when they are really the same.
    3.  Remove word filtering criteria: first check the `preprocess` function to
        see if you can pass an argument to make your adjustment.
    4.  Add word filtering criteria: Implement a new function and add it to
        `preprocess` with a boolean arg to enable/disable.
    5.  Change the stemmer: assign `stem_word` to the function that should do
        your word stemming. The default is the Porter Stemmer.

:var set PUNCT:  The set of punctuation to be filtered from documents.
:var set STOPWORDS: The set of stopwords to be filtered from documents.

"""
import string
import itertools as it
import operator as op

import nltk.corpus
from nltk.tokenize import RegexpTokenizer
import numpy


# Error codes
NO_PORTER_STEMMER_ERROR = -10
NO_STOPWORDS_FILE_ERROR = -11


try:
    # This is a more efficient implementation of the Porter Stemmer,
    # available on PyPi.
    from porterstemmer import Stemmer
    stem_word = Stemmer()
except ImportError:
    # Fall back to the slower stemmer available from NLTK
    try:
        from nltk import PorterStemmer
        stem_word = PorterStemmer().stem_word
    except ImportError:
        # You're out of luck; in practice, we should never get here because the
        # import from nltk.tokenize above should throw an ImportError first.
        # This is here in case someone decides to remove that.
        print 'No suitable Porter Stemmer could be found.'
        sys.exit(NO_PORTER_STEMMER_ERROR)


# punctuation removal is done using the translate method of words
# you can also add mappings to translate certain characters to certain others.
PUNCT = set(string.punctuation)
# add or remove stuff here
PUNCTUATION = ''.join(PUNCT)
# TRANSLATION_TABLE = None
TRANSLATION_TABLE = {ord(c): None for c in PUNCTUATION}

# nltk has a list of 123 english stopwords
# STOPWORDS = set(nltk.corpus.stopwords.words('english'))
# but I had to expand on it to include contractions
# this list includes all nltk stopwords plus contractions plus a few extras
STOPWORDS_FILE = 'stopwords.txt'
try:
    with open(STOPWORDS_FILE) as f:
        STOPWORDS = set(f.read().split())
except IOError:
    print 'No stopwords file found at path: %s' % STOPWORDS_FILE
    sys.exit(NO_STOPWORDS_FILE_ERROR)

# Can add additional stopword to be removed here...
# but you probably shouldn't.
STOPWORDS.add('br')  # get rid of </br> html tags (hackish)
STOPWORDS.add('')  # makes the pipeline squeaky clean (ass-covering)

"""
This tokenizer also removes all punctuation except apostrophes and hyphens,
so cases where hyphens are left out are marked the same, such as:

    multi-processor and multiprocessor
    on-line and online

so contractions can be handled appropriately. These can be filtered out
afterwards if desired.
"""
TOKENIZER = RegexpTokenizer("\w+[-']?\w*(-?\w*)*")
word_tokenize = TOKENIZER.tokenize

# someone may find these things useful
strip = op.methodcaller('strip')
remove_hyphens = lambda word: word.replace('-', '')
simple_punct_remove = lambda word: word.replace('-', '').replace("'", '')


def remove_punctuation(word):
    """Remove all punctuation from the word (unicode). Note that the `translate`
    method is used, and we assume unicode inputs. The str method has a different
    `translate` method, so if you end up working with strings, you may want to
    revisit this method.
    """
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
    return remove_punctuation(word).lower().strip()

def word_is_not_junk(word):
    """Applies a set of conditions to filter out junk words.

    :param str word: The word to test.
    :param int min_length: Drop words shorter than this; set to 0 to disable.
    :return: False if the word is junk, else True.

    """
    # Can add additional conditions here; keep in mind these will be
    # run over a potentially large list of words, so make them efficient.
    return not (
        is_stopword(word) or
        starts_with_digits(word) or
        (len(word) < 2)
    )


def preprocess(wordlist, stopwords=True, digits=True, stem=True):
    """Perform preprocessing on a list of words. The various arguments to this
    function allow one to turn off certain preprocessing steps.

    :param bool stopwords: If True, remove stopwords.
    :param bool digits: If True, remove words that start with digits.
    :param bool stem: If True, stem words using a Porter stemmer.
    """
    if stopwords: wordlist = it.ifilterfalse(is_stopword, wordlist)
    if digits: wordlist = it.ifilterfalse(starts_with_digits, wordlist)
    if stem: wordlist = it.imap(stem_word, wordlist)
    return wordlist


def doctovec(doc, *args):
    """See `preprocess` for keyword args."""
    word_list = word_tokenize(doc.lower())
    return preprocess(word_list, *args)


def vectorize(doc):
    """Convert a document (string/unicode) into a filtered, cleaned,
    stemmed, list of words. See `doctovec` for a function with more options.
    The following cleaning operations are performed:

        1.  punctuation removed
        2.  word lowercased
        3.  whitespace stripped

    Then words meeting these filtering criteria are removed:

        1.  empty or only 1 character
        2.  stopword
        3.  all digits
        4.  starts with digit

    Finally, all words are stemmed.

    :param str doc: The document to vectorize.
    :rtype:  list of str
    :return: The cleaned, stemmed, filtered, list of words.
    """
    word_list = word_tokenize(doc)
    cleaned_words = [clean_word(word) for word in word_list]
    filtered_words = it.ifilter(word_is_not_junk, cleaned_words)
    return [stem_word(word) for word in filtered_words]


def write_vector(vec, filepath, delim='\n', lazy=False):
    """ Write a list of words to a delimiter-separated text file.

    :param (list of str) vec: The word vector to write to a file.
    :param str  filepath: The absolute path of the file to write to.
    :param str  delim: The delimiter to use to separate words.
    :param bool lazy: If True, loop through words one at a time. This is useful
        when you want to avoid loading the whole list of words into memory at
        once.  However, it will take longer to write to the file, since it will
        require more calls to `write`.
    """
    with open(filepath, 'w') as f:
        if not lazy:
            f.write(delim.join(vec))
        else:
            for word in vec:
                f.write('%s%s' % (word, delim))
