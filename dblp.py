import re
import os
import sys
import logging
import argparse

title_pattern = re.compile("#\*([^\r\n]*)")
author_pattern = re.compile("#@([^\r\n]*)")
year_pattern = re.compile("#t([0-9]*)")
venue_pattern = re.compile("#c([^\r\n]*)")
id_pattern = re.compile("#index([^\r\n]*)")
refs_pattern = re.compile("#%([^\r\n]*)")
abstract_pattern = re.compile("#!([^\r\n]*)")


def match(line, pattern):
    m = pattern.match(line)
    if m: return m.groups()[0].strip().decode('utf-8')
    else: return None


def fmatch(f, pattern):
    return match(f.readline(), pattern)


def nextrecord(f):
    """Assume file pos is at beginning of record and read to end. Returns all
    components as a dict.
    """
    title = fmatch(f, title_pattern)
    if title is None:
        return None

    return {
        'title': title,
        'author': fmatch(f, author_pattern),
        'year': fmatch(f, year_pattern),
        'venue': fmatch(f, venue_pattern),
        'id': fmatch(f, id_pattern),
        'refs': fmatch(f, refs_pattern),
        'abstract': fmatch(f, abstract_pattern)
    }


def castrecord(record):
    record['id'] = int(record['id'])
    record['refs'] = [int(ref) for ref in record['refs']]
    year = record['year']
    record['year'] = int(year) if year else None
    author = record['author']
    record['author'] = author.split(',') if ',' in author else [author]
    return record


def iterdata(fpath):
    with open(fpath) as f:
        record = nextrecord(f)
        while record is not None:
            yield record
            f.readline()  # consume blank line
            record = nextrecord(f)


def iterrecords(fpath):
    return (castrecord(record) for record in iterdata(fpath))


def make_parser():
    parser = argparse.ArgumentParser(
        description="parse dblp data")
    parser.add_argument(
        'fpath', action='store',
        help='file to parse data from')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help="turn on verbose logging")
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    f = open(args.fpath)
    r = nextrecord(f)
    c = castrecord(r)

    sys.exit(0)
