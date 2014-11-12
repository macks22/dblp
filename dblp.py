import re
import os
import sys
import logging
import argparse

import sqlalchemy as sa

import db


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

    return {
        'title': title,
        'authors': authors,
        'year': year,
        'venue': venue,
        'id': paperid,
        'refs': refs,
        'abstract': abstract
    }


def castrecord(record):
    record['id'] = int(record['id'])
    record['refs'] = [int(ref) for ref in record['refs']]
    year = record['year']
    record['year'] = int(year) if year else None
    author = record['authors']
    record['authors'] = author.split(',') if ',' in author else [author]
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


def process_record(record):
    """Update the database with the contents of the record."""
    logging.debug('processing record\n%s' % record);
    conn = db.engine.connect()
    paper_id = record['id']
    ins = db.papers.insert().\
            values(id=paper_id, title=record['title'],
                   venue=record['venue'], year=record['year'],
                   abstract=record['abstract'])

    # attempt to insert a new paper into the db
    try:
        ins_res = conn.execute(ins)
    except sa.exc.IntegrityError as err:
        # a paper already exists with this id
        logging.error(str(err))
        # since ids come from data, we've already processed this record
        conn.close()
        return False
    except Exception as e:
        logging.error('unexpected exception\n%s', str(e))
    else:
        ins_res.close()

    # make new records for each author
    for author in record['authors']:
        ins = db.authors.insert().\
                values(paper=paper_id, name=author)
        try:
            ins_res = conn.execute(ins)
        except sa.exc.IntegrityError as err:
            # might happen if an author is listed twice for the same paper
            logging.error(str(err))
        except Exception as e:
            logging.error('unexpected exception\n%s', str(e))
        else:
            ins_res.close()

    for ref in record['refs']:
        ins = db.refs.insert().\
                values(paper=paper_id, ref=ref)

        try:
            ins_res = conn.execute(ins)
        except sa.exc.IntegrityError as err:
            # might happen if a paper lists the same reference multiple times
            logging.error(str(err))
        except Exception as e:
            logging.error('unexpected exception\n%s', str(e))
        else:
            ins_res.close()

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

    if args.very_verbose:
        logging.basicConfig(level=logging.DEBUG)
        db.engine.echo = True

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
