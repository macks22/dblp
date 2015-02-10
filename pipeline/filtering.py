import os

import pandas as pd
import luigi

import util
import aminer
import config


class PathBuilder(object):
    def convert_path(self, fname, suffix):
        base, ext = os.path.splitext(fname)
        fname = '%s-%s' % (os.path.basename(base), suffix)
        fname = '%s.csv' if not ext else '%s%s' % (fname, ext)
        return os.path.join(config.data_dir, fname)


class RemovePapersNoVenueOrYear(luigi.Task, PathBuilder):
    """Remove papers with either no year or no venue listed."""

    def requires(self):
        return aminer.CSVPaperRecords()

    def output(self):
        papers_file = self.input()
        fpath = self.convert_path(papers_file.path, 'venue-and-year')
        return luigi.LocalTarget(fpath)

    def run(self):
        with self.input().open() as pfd:
            df = pd.read_csv(pfd)

        df = df[(~df['venue'].isnull()) & (~df['year'].isnull())]
        with self.output().open('w') as outfile:
            df.to_csv(outfile, index=False)


class RemoveUniqueVenues(luigi.Task, PathBuilder):
    """Remove papers with unique venues (occur only once in dataset)."""

    def requires(self):
        return RemovePapersNoVenueOrYear()

    def output(self):
        papers_file = self.input()
        fpath = self.convert_path(papers_file.path, 'no-unique-venues')
        return luigi.LocalTarget(fpath)

    def run(self):
        with self.input().open() as paper_file:
            df = pd.read_csv(paper_file)

        multiple = df.groupby('venue')['venue'].transform(len) > 1
        filtered = df[multiple]
        with self.output().open('w') as outfile:
            filtered.to_csv(outfile, index=False)


class YearFiltering(object):
    start = luigi.IntParameter()
    end = luigi.IntParameter()

    def get_fpath(self, fname, ext='csv'):
        fpath = os.path.join(config.data_dir, fname)
        return '%s-%d-%d.%s' % (fpath, self.start, self.end, ext)


class FilterPapersToYearRange(luigi.Task, YearFiltering):
    """Filter paper records to those published in particular range of years."""

    def requires(self):
        return [RemoveUniqueVenues(), aminer.CSVRefsRecords()]

    def output(self):
        return [luigi.LocalTarget(self.get_fpath('paper')),
                luigi.LocalTarget(self.get_fpath('refs'))]

    def run(self):
        papers_file, refs_file = self.input()
        paper_out, refs_out = self.output()

        with papers_file.open() as pfile:
            papers_df = pd.read_csv(pfile)

        # Filter based on range of years
        papers_df['year'] = papers_df['year'].astype(int)
        filtered = papers_df[(papers_df['year'] >= self.start) &
                             (papers_df['year'] <= self.end)]

        # Save filtered paper records
        with paper_out.open('w') as outfile:
            filtered.to_csv(outfile, index=False)
            paper_ids = filtered['id'].unique()

        # Filter and save references based on paper ids.
        with refs_file.open() as rfile:
            refs_df = pd.read_csv(rfile)

        filtered = refs_df[(refs_df['paper_id'].isin(paper_ids)) &
                           (refs_df['ref_id'].isin(paper_ids))]
        with refs_out.open('w') as outfile:
            filtered.to_csv(outfile, index=False)


class YearFilteringNonPaper(YearFiltering):
    """Filter data records which depend on paper ids for filtering."""
    @property
    def papers_file(self):
        for file_obj in util.flatten(self.input()):
            if 'paper' in file_obj.path:
                return file_obj

    def read_paper_ids(self):
        with self.papers_file.open() as papers_file:
            df = pd.read_csv(papers_file, header=0, usecols=(0,))
        return df['id'].unique()


class FilterAuthorshipsToYearRange(luigi.Task, YearFilteringNonPaper):
    """Filter authorship records to particular range of years."""
    def requires(self):
        return (FilterPapersToYearRange(self.start, self.end),
                aminer.ParseAuthorshipsToCSV())

    @property
    def author_file(self):
        return self.input()[1]

    def output(self):
        return luigi.LocalTarget(self.get_fpath('author'))

    def run(self):
        paper_ids = self.read_paper_ids()
        with self.author_file.open() as afile:
            author_df = pd.read_csv(afile)

        # Filter and write authorship records.
        filtered = author_df[author_df['paper_id'].isin(paper_ids)]
        with self.output().open('w') as outfile:
            filtered.to_csv(outfile, index=False)


class FilterAuthorNamesToYearRange(luigi.Task, YearFiltering):
    """Filter author name,id records to particular range of years."""

    def requires(self):
        return (FilterAuthorshipsToYearRange(self.start, self.end),
                aminer.ParseAuthorNamesToCSV())

    @property
    def author_file(self):
        return self.input()[0]

    @property
    def person_file(self):
        return self.input()[1]

    def output(self):
        return luigi.LocalTarget(self.get_fpath('person'))

    def read_author_ids(self):
        with self.author_file.open() as afile:
            author_df = pd.read_csv(afile)
            return author_df['author_id'].unique()

    def run(self):
        """Filter and write person records."""
        author_ids = self.read_author_ids()
        with self.person_file.open() as person_file:
            person_df = pd.read_csv(person_file)

        filtered = person_df[person_df['id'].isin(author_ids)]
        with self.output().open('w') as outfile:
            filtered.to_csv(outfile, index=False)


class FilterAllCSVRecordsToYearRange(luigi.Task, YearFiltering):
    def requires(self):
        """Trigger all tasks for year filtering."""
        yield FilterPapersToYearRange(self.start, self.end)
        yield FilterAuthorshipsToYearRange(self.start, self.end)
        yield FilterAuthorNamesToYearRange(self.start, self.end)


if __name__ == "__main__":
    luigi.run()
