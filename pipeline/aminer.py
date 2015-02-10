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
their essential information into csv files, using the luigi batch processing
framework.
"""
import re
import os
import sys
import csv

import pandas as pd
import luigi

import util
import config


# Regexes for record parsing from papers file
title_pattern = re.compile("#\*([^\r\n]*)")
author_pattern = re.compile("#@([^\r\n]*)")
affiliations_pattern = re.compile("#o([^\r\n]*)")
year_pattern = re.compile("#t ([0-9]*)")
venue_pattern = re.compile("#c([^\r\n]*)")
id_pattern = re.compile("#index([^\r\n]*)")
refs_pattern = re.compile("#%([^\r\n]*)")
abstract_pattern = re.compile("#!([^\r\n]*)")


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


class AminerNetworkPapers(luigi.Task):
    """External dependency on Aminer-network DBLP papers file."""
    def output(self):
        return luigi.LocalTarget(
                os.path.join(config.originals_dir, 'AMiner-Paper.txt'))


class AminerNetworkAuthorNames(luigi.Task):
    """External dependency on Aminer-network DBLP author info file."""
    def output(self):
        return luigi.LocalTarget(
                os.path.join(config.originals_dir, 'AMiner-Author.txt'))


class AminerNetworkAuthorships(luigi.Task):
    """External dependency on Aminer-network DBLP author id/name file."""
    def output(self):
        return luigi.LocalTarget(
                os.path.join(config.originals_dir, 'AMiner-Author2Paper.tsv'))


class AminerNetworkData(luigi.Task):
    """Aggregate all external dependencies on Aminer-network data."""
    def output(self):
        yield AminerNetworkPapers()
        yield AminerNetworkAuthorNames()
        yield AminerNetworkAuthorships()


class ParsePapersToCSV(luigi.Task):
    """Parse paper records from Aminer-network format to csv."""

    def requires(self):
        return AminerNetworkPapers()

    def output(self):
        return [
            luigi.LocalTarget(os.path.join(config.base_csv_dir, 'paper.csv')),
            luigi.LocalTarget(os.path.join(config.base_csv_dir, 'refs.csv'))
        ]

    def run(self):
        papers = self.iterpapers()

        papers_out_path, refs_out_path = self.output()
        with papers_out_path.open('w') as pf, refs_out_path.open('w') as rf:
            paper_writer = util.UnicodeWriter(pf)  # handle titles/abstracts
            refs_writer = csv.writer(rf)

            # write csv column headers
            paper_writer.writerow(Paper.csv_header)
            refs_writer.writerow(('paper_id', 'ref_id'))

            for paper in papers:
                paper_writer.writerow(paper.csv_attrs)
                for ref in paper.refs:
                    refs_writer.writerow((paper.id, ref))

    def match(self, line, pattern):
        """Return first group of match on line for pattern."""
        m = pattern.match(line)
        return m.groups()[0].decode('utf-8').strip() if m else None

    def fmatch(self, f, pattern):
        """Call `match` on the next line of the file."""
        return self.match(f.readline(), pattern)

    def nextrecord(self, f):
        """Assume file pos is at beginning of record and read to end. Returns all
        components as a Paper instance.
        """
        fmatch = self.fmatch
        match = self.match

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


    def iterpapers(self):
        """Return iterator over all paper records."""
        with self.input().open() as f:
            record = self.nextrecord(f)
            while record is not None:
                yield record
                record = self.nextrecord(f)


class CSVPaperRecords(luigi.Task):
    """Abstraction for depending only on paper records from original parse."""
    def requires(self):
        return ParsePapersToCSV()

    def output(self):
        papers_file = self.input()[0]
        return luigi.LocalTarget(papers_file.path)


class CSVRefsRecords(luigi.Task):
    """Abstraction for depending only on refs records from original parse."""
    def requires(self):
        return ParsePapersToCSV()

    def output(self):
        refs_file = self.input()[1]
        return luigi.LocalTarget(refs_file.path)


class ParseUniqueVenues(luigi.Task):
    """Read through paper records and write out a list of unique venues."""

    def requires(self):
        return CSVPaperRecords()

    def output(self):
        return luigi.LocalTarget(os.path.join(config.base_csv_dir, 'venue.csv'))

    def run(self):
        # find venue column
        with self.input().open() as papers_file:
            reader = csv.reader(papers_file)
            headers = reader.next()
            venue_index = headers.index('venue')

        # filter out unique venues
        with self.input().open() as papers_file:
            df = pd.read_csv(papers_file, header=0, usecols=(venue_index,))
            unique_venues = df['venue'].unique()

        # write to csv file
        with self.output().open('w') as outfile:
            outfile.write('\n'.join(map(str, unique_venues)))


class ParseUniqueYears(luigi.Task):
    """Read through paper records and write out a list of unique venues."""

    def requires(self):
        return CSVPaperRecords()

    def output(self):
        return luigi.LocalTarget(os.path.join(config.base_csv_dir, 'year.csv'))

    def run(self):
        # find year column
        with self.input().open() as papers_file:
            reader = csv.reader(papers_file)
            headers = reader.next()
            year_index = headers.index('year')

        # filter out unique years
        with self.input().open() as papers_file:
            df = pd.read_csv(papers_file, header=0, usecols=(year_index,))
            unique_years = df['year'].unique()

        # write to csv file
        with self.output().open('w') as outfile:
            outfile.write('\n'.join(map(str, unique_years)))


class ParseAuthorNamesToCSV(luigi.Task):
    """Parse author name-to-id mappings from AMiner-network data."""

    def requires(self):
        return AminerNetworkAuthorNames()

    def output(self):
        return luigi.LocalTarget(os.path.join(config.base_csv_dir, 'person.csv'))

    def run(self):
        author_rows = self.read_author_id_name_pairs()
        util.write_csv_to_fwrapper(self.output(), ('id', 'name'), author_rows)

    def read_author_id_name_pairs(self):
        def _read_single_pair(f):
            try:
                index = f.readline().split()[-1]
                name_line = f.readline().split()
                name = ' '.join(name_line[1:])
                util.read_to_newline(f)
                return (index, name)
            except:
                raise StopIteration()

        with self.input().open() as f:
            while True:
                yield _read_single_pair(f)


class ParseAuthorshipsToCSV(luigi.Task):
    """Parse authorship records from AMiner-network data to a csv file."""

    def requires(self):
        return AminerNetworkAuthorships()

    def output(self):
        return luigi.LocalTarget(os.path.join(config.base_csv_dir, 'author.csv'))

    def run(self):
        authorships = self.iter_authorships()
        util.write_csv_to_fwrapper(
            self.output(), ('author_id', 'paper_id'), authorships)

    def iter_authorships(self):
        with self.input().open() as f:
            for record in (line.split('\t') for line in f):
                yield ((record[1], record[2]))


class ParseAminerNetworkDataToCSV(luigi.Task):
    """Produce complete csv dataset from Aminer-network data."""

    def requires(self):
        """Require all parsing tasks in order to trigger everything."""
        yield ParsePapersToCSV()
        yield ParseUniqueVenues()
        yield ParseUniqueYears()
        yield ParseAuthorshipsToCSV()
        yield ParseAuthorNamesToCSV()


if __name__ == "__main__":
    luigi.run()
