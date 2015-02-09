import re
import sys
import csv
import codecs
import logging
import argparse
import cStringIO


# Regexes for record parsing
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


class Record(object):
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

    return Record(
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


def write_records_to_csv(records, ppath='papers.csv', rpath='refs.csv'):
    """Write the records to csv files.
    :param str ppath: Path of file to write paper records to.
    :param str rpath: Path of file to write paper references to.
    """
    pf = open(ppath, 'w')
    rf = open(rpath, 'w')
    paper_writer = UnicodeWriter(pf)  # handle titles/abstracts
    refs_writer = csv.writer(rf)

    # write csv column headers
    paper_writer.writerow(Record.csv_header)
    refs_writer.writerow(('paper_id', 'ref_id'))

    # accumulate list of unique years and venues
    venues = set()
    years = set()

    for record in records:
        venues.add(record.venue)
        years.add(record.year)
        paper_writer.writerow(record.csv_attrs)
        for ref in record.refs:
            refs_writer.writerow((record.id, ref))

    with open('venues.csv', 'w') as f:
        f.write('\n'.join(venues).encode('utf-8'))

    with open('years.csv', 'w') as f:
        f.write(u'\n'.join(map(str, years)))


def make_parser():
    parser = argparse.ArgumentParser(
        description="parse dblp data")
    parser.add_argument(
        'fpath', action='store',
        help='file to parse data from')
    parser.add_argument(
        '-p', '--paper-out-file', action='store', default='papers',
        help='name of file to output csv papers to')
    parser.add_argument(
        '-r', '--refs-out-file', action='store', default='refs',
        help='name of file to output csv references to')
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    papers = iterrecords(args.fpath)
    fmt_string = '%s.csv'
    write_records_to_csv(
        papers,
        fmt_string % args.paper_out_file,
        fmt_string % args.refs_out_file)
