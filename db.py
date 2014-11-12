import os
import sys
import logging
import argparse

import sqlalchemy as sa
from sqlalchemy import (
    Table, Column, Integer, String, MetaData, ForeignKey, PrimaryKeyConstraint)

import config


# engine = sa.create_engine('sqlite:///dblp.sql', echo=False)
connection_string = 'postgresql://%s:%s@%s/%s' % (
    config.username,
    config.password,
    config.hostname,
    config.dbname
)
logging.info('db connect using connection string: %s' % connection_string)
engine = sa.create_engine(connection_string)
metadata = MetaData()

papers = Table('papers', metadata,
    Column('id', Integer, primary_key=True),
    Column('title', String(255), nullable=False),
    Column('abstract', String(255)),
    Column('venue', String(255)),
    Column('year', Integer)
)

authors = Table('authors', metadata,
    Column('name', String(255)),
    Column('paper', Integer,
           ForeignKey('papers.id', onupdate='CASCADE', ondelete='CASCADE')),
    PrimaryKeyConstraint('name', 'paper', name='authors_pk')
)

refs = Table('refs', metadata,
    Column('paper', Integer,
           ForeignKey('papers.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('ref', Integer),
    # Column('ref', Integer,
    #        ForeignKey('papers.id', onupdate='CASCADE', ondelete='CASCADE')),
    PrimaryKeyConstraint('paper', 'ref', name='refs_pk')
)


def make_parser():
    parser = argparse.ArgumentParser(
        description='create/update dblp database')
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='enable verbose logging output')
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
        engine.echo = True

    logging.info('dropping all tables');
    metadata.drop_all(engine)
    logging.info('creating database from schema');
    metadata.create_all(engine)

    sys.exit(0)
