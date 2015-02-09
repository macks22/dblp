"""
This module contains code to parse the AminerNetwork DBLP dataset from:

    http://arnetminer.org/AMinerNetwork/

into a set of csv files which can subsequently be ingested into a relational
database. In particular, there are 4 files provided by the arnetminer group:

    1.  AMiner-Paper.rar
    2.  AMiner-Author.zip
    3.  AMiner-Author2Paper.zip
    4.  Aminer-Coauthor.zip

The fourth file consists of a complete co-authorship network, which is not
processed by this module. For the first three, there is functionality to parse
their essential information into csv files. In particular, the CLI provides two
subcommands. Each is detailed below.

    paper:
        The single positional argument should be the AMiner-Paper records. This
        command uses regular expressions to parse the relevant paper information
        from the unique grammar used. Papers and references are written to
        separate files.

    author:
        -n: Takes the Aminer-Author records and writes a csv file of
            author_id,author_name pairs.
        -a: Takes the AMiner-Author2Paper records and writes a csv file which
            consists of author_id,paper_id pairs for each instance of
            authorship.

"""
import re
import sys
import csv
import codecs
import logging
import argparse
import cStringIO
import util


# Regexes for record parsing from papers file
title_pattern = re.compile("#\*([^\r\n]*)")
author_pattern = re.compile("#@([^\r\n]*)")
affiliations_pattern = re.compile("#o([^\r\n]*)")
year_pattern = re.compile("#t ([0-9]*)")
venue_pattern = re.compile("#c([^\r\n]*)")
id_pattern = re.compile("#index([^\r\n]*)")
refs_pattern = re.compile("#%([^\r\n]*)")
abstract_pattern = re.compile("#!([^\r\n]*)")


class UnicodeWriter(object):
    """A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        self.queue = cStringIO.StringIO()  # Redirect output to a queue
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        self.stream.write(data)  # write to the target stream
        self.queue.truncate(0)  # empty queue

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class Paper(object):
    __slots__ = ['id', 'title', 'authors', 'venue', 'refs', 'year', 'abstract']
    csv_header = ('id', 'title', 'venue', 'year', 'abstract')

    def __init__(self, id, title, authors, venue, refs, abstract, year):
        self.id = int(id)
        self.title = title
        self.venue = venue
        self.refs = [int(ref) for ref in refs]
        self.year = int(year) if year else None
        self.authors = [a for a in authors.split(',') if a]
        self.abstract = abstract if abstract else None

    @property
    def csv_attrs(self):
        attrs = [getattr(self, attr) for attr in self.csv_header]
        return [unicode(attr) if attr else u'' for attr in attrs]


def match(line, pattern):
    m = pattern.match(line)
    return m.groups()[0].decode('utf-8').strip() if m else None


def fmatch(f, pattern):
    return match(f.readline(), pattern)


def nextrecord(f):
    """Assume file pos is at beginning of record and read to end. Returns all
    components as a dict.
    """
    paperid = fmatch(f, id_pattern)
    title = fmatch(f, title_pattern)
    if title is None:
        return None

    authors = fmatch(f, author_pattern)
    f.readline()  # discard affiliation info
    year = fmatch(f, year_pattern)
    venue = fmatch(f, venue_pattern)

    # read out reference list
    refs = []
    line = f.readline()
    m = match(line, refs_pattern)
    while m is not None:
        if m:
            refs.append(m)
        line = f.readline()
        m = match(line, refs_pattern)

    abstract = match(line, abstract_pattern)
    if line.strip(): f.readline()  # consume blank line

    return Paper(
        id=paperid,
        title=title,
        authors=authors,
        year=year,
        venue=venue,
        refs=refs,
        abstract=abstract
    )


def castrecord(record):
    record['id'] = int(record['id'])
    record['refs'] = [int(ref) for ref in record['refs']]
    abstract = record['abstract']
    record['abstract'] = abstract if abstract else None
    year = record['year']
    record['year'] = int(year) if year else None
    author = record['authors']
    if ',' in author:
        record['authors'] = [a for a in author.split(',') if a]
    else:
        record['authors'] = [author]
    return record


def iterrecords(fpath):
    with open(fpath) as f:
        record = nextrecord(f)
        while record is not None:
            yield record
            record = nextrecord(f)


def write_papers_to_csv(papers, ppath='papers.csv', rpath='refs.csv'):
    """Write the records to csv files.

    :param str ppath: Path of file to write paper records to.
    :param str rpath: Path of file to write paper references to.
    """
    with open(ppath, 'w') as pf, open(rpath), 'w') as rf:
        paper_writer = UnicodeWriter(pf)  # handle titles/abstracts
        refs_writer = csv.writer(rf)

        # write csv column headers
        paper_writer.writerow(Paper.csv_header)
        refs_writer.writerow(('paper_id', 'ref_id'))

        # accumulate list of unique years and venues
        venues = set()
        years = set()

        for paper in papers:
            venues.add(paper.venue)
            years.add(paper.year)
            paper_writer.writerow(paper.csv_attrs)
            for ref in paper.refs:
                refs_writer.writerow((paper.id, ref))

    with open('venues.csv', 'w') as f:
        f.write('\n'.join(venues).encode('utf-8'))

    with open('years.csv', 'w') as f:
        f.write(u'\n'.join(map(str, years)))


def read_to_newline(f):
    line = f.readline().strip()
    while line:
        line = f.readline().strip()


def read_author_id_name_pairs(author_file):
    with open(author_file) as f:
        while True:
            try:
                index = f.readline().split()[-1]
                name_line = f.readline().split()
                name = ' '.join(name_line[1:]).decode('utf-8')
                read_to_newline(f)
                yield (index, name)
            except:
                raise StopIteration()


def write_author_names_to_csv(author_file, fpath='person.csv'):
    author_rows = read_author_id_name_pairs(author_file)
    util.write_csv(fpath, ('id', 'name'), author_rows)


def write_authorships_to_csv(afile_path, fpath='author.csv'):
    with open(afile_path) as afile:
        records = (line.split() for line in afile)
        rows = ((r[1], r[2]) for r in records)
        util.write_csv(fpath, ('author_id', 'paper_id'), rows)


def paper(args):
    papers = iterrecords(args.fpath)
    fmt_string = '%s.csv'
    try:
        write_papers_to_csv(
            papers,
            fmt_string % args.paper_out_file,
            fmt_string % args.refs_out_file)
    except IOError:
        logging.error('uanble to open paper records file: %s' % args.fpath)
        sys.exit(1)


def author(args):
    if args.names_file:
        try:
            write_author_names_to_csv(args.names_file)
        except IOError:
            logging.error('unable to open author names file: %s' %
                args.names_file)
            sys.exit(1)
    elif args.authorship_file:
        try:
            write_authorships_to_csv(args.authorship_file)
        except IOError:
            logging.error('unable to open authorship file: %s' %
                args.authorship_file)
            sys.exit(1)


def make_parser():
    parser = argparse.ArgumentParser(
        description="parse dblp data")
    subparsers = parser.add_subparsers()

    # Set up subcommand for parsing paper records.
    paper_parser = subparsers.add_parser(
        'paper',
        description='parse paper records into csv files')
    paper_parser.add_argument(
        'fpath', action='store',
        help='file to parse papers from')
    paper_parser.add_argument(
        '-p', '--paper-out-file', action='store', default='papers',
        help='name of file to output csv papers to')
    paper_parser.add_argument(
        '-r', '--refs-out-file', action='store', default='refs',
        help='name of file to output csv references to')
    paper_parser.set_defaults(func=paper)

    # Set up subcommand for parsing author records.
    author_parser = subparsers.add_parser(
        'author',
        description='parse author records into csv files')
    author_parser.add_argument(
        '-n', '--names-file', action='store',
        help='file to parse author id,name pairs from')
    author_parser.add_argument(
        '-a', '--authorship-file', action='store',
        help='file to parse author_id,paper_id authorship records from')
    author_parser.set_defaults(func=author)

    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    args.func(args)
