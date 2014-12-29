import sqlalchemy as sa

import db
import dblp


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

    try:
        process_records(args.fpath)
    except Exception as err:
        logging.info('ERROR OCCURED IN `process_records`')
        logging.error(str(err))
        sys.exit(-1)

    sys.exit(0)
