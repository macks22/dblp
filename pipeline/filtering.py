import pandas as pd
import util


def remove_single_venues(paper_file, outfile):
    """Read paper records from a file and remove all those papers that have
    venues which only occur once in the dataset.

    :param str paper_file: File name of csv paper records to filter.
    :param str outfile: File name to write the filtered records to.
    """
    df = pd.read_csv(paper_file))
    multiple = df.groupby('venue')['venue'].transform(len) > 1
    filtered = df[multiple]
    filtered.to_csv(outfile, index=False)


def filter_by_year_range(start, end,
                         paper_file='paper.csv', author_file='author.csv',
                         person_file='person.csv', refs_file='refs.csv',
                         venue_file='venue.csv', year_file='year.csv'):
    """Filter the paper records to those occuring in a specific range of years
    (inclusive). To filter to a single year, simply make `start` and `end` the
    same. The files are written to a new directory created in the cwd with the
    name <start>-to-<end>. The *_file params are used both for reading and
    writing, with the exception of `refs_file` and `year_file`, which are only
    used for writing.

    :param int start: Beginning year of the filtering range.
    :param int end: Ending year of the filtering range.
    """
    # filter the papers by year
    df = pd.read_csv(paper_file)
    df['year'] = df['year'].astype(int)
    df = df[(df['year'] >= start) & (df['year'] <= end)]

    # load authors and refs for later filtering
    # load now in case paths are relative rather than absolute
    author_df = pd.read_csv(author_file)
    person_df = pd.read_csv(person_file)
    refs_df = pd.read_csv(refs_file)

    # change to new dir to prepare to write new set of files
    newdir = '%d-to-%d' % (start, end)
    init_wd = os.getcwd()
    try: os.mkdir(newdir)
    except OSError: pass
    os.chdir(newdir)

    def save_csv_relative(fname, df):
        out_fname = os.path.basename(fname)
        df.to_csv(out_fname, index=False)

    # write new paper.csv file
    save_csv_relative(paper_file, df)

    # write new venue listing
    rows = sorted([(venue,) for venue in df['venue'].unique()])
    util.write_csv(venue_file, None, rows)

    # write new year listing
    rows = sorted([(year,) for year in df['year'].unique()])
    util.write_csv(year_file, None, rows)

    # filter authors and refs to only those in the filtered time range
    paper_ids = df['id'].unique()
    author_df = author_df[author_df['paper_id'].isin(paper_ids)]
    author_ids = author_df['author_id'].unique()
    person_df = person_df[person_df['id'].isin(author_ids)]
    refs_df = refs_df[(refs_df['paper_id'].isin(paper_ids)) &
                      (refs_df['ref_id'].isin(paper_ids))]

    # now write the filtered records
    save_csv_relative(author_file, author_df)
    save_csv_relative(person_file, person_df)
    save_csv_relative(refs_file, refs_df)

    # all done; restore previous working directory
    os.chdir(init_wd)
