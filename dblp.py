import re
import os
import sys
import logging
import argparse
import multiprocessing as mp

import sqlalchemy as sa

import db

class Record(object):
    __slots__ = ['id', 'title', 'authors', 'venue', 'refs', 'abstract', 'year']

    def __init__(self, id, title, authors, venue, refs, abstract, year):
        self.id = int(id)
        self.title = title
        self.venue = venue
        self.refs = [int(ref) for ref in refs]
        self.abstract = abstract if abstract else None
        self.year = int(year) if year else None
        self.authors = [a for a in authors.split(',') if a]


title_pattern = re.compile("#\*([^\r\n]*)")
author_pattern = re.compile("#@([^\r\n]*)")
year_pattern = re.compile("#t([0-9]*)")
venue_pattern = re.compile("#c([^\r\n]*)")
id_pattern = re.compile("#index([^\r\n]*)")
refs_pattern = re.compile("#%([^\r\n]*)")
abstract_pattern = re.compile("#!([^\r\n]*)")


def match(line, pattern):
    m = pattern.match(line)
    if m:
        return m.groups()[0].decode('utf-8').strip()
    else:
        return None


def fmatch(f, pattern):
    return match(f.readline(), pattern)


def nextrecord(f):
    """Assume file pos is at beginning of record and read to end. Returns all
    components as a dict.
    """
    title = fmatch(f, title_pattern)
    if title is None:
        return None

    if len(title) > 255:
        title = title[0:255]

    authors = fmatch(f, author_pattern)
    year = fmatch(f, year_pattern)
    venue = fmatch(f, venue_pattern)
    paperid = fmatch(f, id_pattern)

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

    f.readline()  # consume blank line
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


def insert(conn, ins):
    """Attempt to run an insertion statement; return results, None if error."""
    try:
        ins_res = conn.execute(ins)
    except sa.exc.IntegrityError as err:
        # a paper already exists with this id
        logging.error(str(err))
        return None
    except Exception as e:
        logging.error('unexpected exception\n%s', str(e))
        return None
    else:
        return ins_res


def person_insert(conn, name):
    sel = sa.sql.text("SELECT id FROM person WHERE LOWER(name)=LOWER(:n)")
    res = conn.execute(sel, n=name)
    p = res.first()

    if p is not None:
        return p['id']

    ins = db.person.insert().values(name=name)
    try:
        res = conn.execute(ins)
    except sa.exc.IntegrityError:  # concurrency issue
        res = conn.execute(sel, n=name)
        p = res.first()
        if p is None:
            raise
        else:
            return p['id']

    return res.inserted_primary_key[0]


def process_record(record):
    """Update the database with the contents of the record."""
    logging.debug('processing record\n%s' % record);
    conn = db.engine.connect()
    paper_id = record.id

    ins = db.papers.insert().\
            values(id=paper_id, title=record.title,
                   venue=record.venue, year=record.year,
                   abstract=record.abstract)

    # attempt to insert a new paper into the db
    result = insert(conn, ins)
    if result is None:
        # since ids come from data, we've already processed this record
        conn.close()
        return False

    # make new records for each author
    for author in record.authors:
        person_id = person_insert(conn, author)
        ins = db.authors.insert().values(paper=paper_id, person=person_id)
        insert(conn, ins)  # may fail, but we don't really care

    for ref in record.refs:
        ins = db.refs.insert().values(paper=paper_id, ref=ref)
        insert(conn, ins)

    conn.close()
    return True  # success


def process_records(fpath):
    """Process all records in data file."""
    processed = 0
    successful = 0
    for record in iterrecords(fpath):

        try:
            success = process_record(record)
        except Exception as e:
            logging.info('unexpected exception in `process_record`')
            logging.error(str(e))
            success = False

        processed += 1
        if success:
            successful += 1
        if processed % 20 == 0:
            logging.info('processed:  %d records' % processed)
            logging.info('successful: %d' % successful)


def make_parser():
    parser = argparse.ArgumentParser(
        description="parse dblp data")
    parser.add_argument(
        'fpath', action='store',
        help='file to parse data from')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help="turn on verbose logging")
    parser.add_argument(
        '-vv', '--very-verbose', action='store_true',
        help='turn on very verbose logging')
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s][%(levelname)s]: %(message)s')
    elif args.very_verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(asctime)s][%(levelname)s]: %(message)s')
        db.engine.echo = True
    else:
        logging.basicConfig(level=logging.CRITICAL)
        db.engine.echo = False

    # f = open(args.fpath)
    # r = nextrecord(f)
    # c = castrecord(r)

    try:
        process_records(args.fpath)
    except Exception as err:
        logging.info('ERROR OCCURED IN `process_records`')
        logging.error(str(err))
        sys.exit(-1)

    sys.exit(0)
