dblp
====

Parse DBLP data into a structured format for experimentation.

# Data Source

The data comes from arnetminer.org, which is maintained by a research group at
Tsinghua University in China. It exists in eight different versions. Seven of
them can be found [here](http://arnetminer.org/billboard/citation). The 8th and
newest version (as of 2015-02-11) can be found
[here](http://arnetminer.org/billboard/AMinerNetwork). It is the 8th version
which this package was built to parse. However, given a suitable module 1
replacement (see below), any of the versions can be used for the subsequent
transformations.

TODO: fill in stats about dataset here
*   4 files
*   what each contains
*   which are used
*   number of papers, authors, percent with abstract, etc.

# Pipeline Design

The entire pipeline is built using the `luigi` package, which provides a data
pipeline dependency resolution scheme based on output files. Hence, there are
many output files during all phases of processing. Often these are useful;
sometimes they are not. Overall, luigi turned out to be a very nice package to
work with. It allows each processing step to be written out as a distinct class.
Each of these inherits from `luigi.Task`. Before running, each task checks its
dependent data files. If any are absent, the tasks responsible for building them
are run first. After running, each task produces one or more output files, which
can then be specified as dependencies for later tasks. Hence, the generation of
the entire dataset is as simple as running a task which is dependent on all the
others. This task is called `BuildDataset`, and is present in the `pipeline`
module.

# Outputs

All outputs end up in the `data` directory inside the base directory, which is
specified in the `config` module by setting `base_dir`.

## Module 1: Relational Representation

Module: `aminer`
Location: `base-csv/`
Config: `base_csv_dir`

The first transformation layer involves parsing the given input files into
several CSV files which can subsequently be loaded into a relational database or
used more easily by other transformation steps. In particular, the `aminer`
module performs the following conversions:

    AMiner-Paper.txt        ->  paper.csv  (id,title,venue,year,abstract)
                            ->  refs.csv   (paper_id,ref_id)
                            ->  venue.csv  (venue -- listing of unique venues)
                            ->  year.csv   (year -- listing of unique years)

    AMiner-Author.txt       ->  person.csv (id,name)

    AMiner-Author2Paper.txt ->  author.csv (author_id,paper_id)

These six CSV files contain all the information used by subsequent processing
modules; the four original files from the Aminer dataset are not used again.

## Module 2: Filtering

Module: `filtering`
Location: `filtered-csv/`
Config: `filtered_dir`

Rather than examining the entire dataset at once, many experiments will likely
find it useful to filter to a range of years. For this purpose, the second
module provides a filtering interface which takes the six relational data files
and filters them based on the paper publication years. The `filtering` module
provides code to do this. All of the tasks involved take a `start` and `end`
year. Running the `FilterAllCSVRecordsToYearRange` task like so:

    python filtering.py FilterAllCSVRecordsToYearRange --start 1990 --end 2000 --local-scheduler

will produce the following:

    paper-1990-2000.csv
    refs-1990-2000.csv
    venue-1990-2000.csv
    person-1990-2000.csv
    author-1990-2000.csv

Notice that `year.csv` is not filtered, for obvious reasons. These files can now
be used instead of those produced from the `aminer` output.

## Module 3: Network Building

Module: `build_graphs`
Location: `graphs/`
Config: `graph_dir`

This module constructs citation networks from the relational data files. In
particular, it contains tasks for building a paper citation graph and an author
citation graph, as well as for finding and writing the largest connected
component (LCC) of the author citation graph. All tasks take an optional `start`
and `end` year. If none is passed, the entire dataset is used; otherwise the
specified subset is parsed (if not already present in `filtered-csv/`) and used
instead. All graph data can be built by running the `BuildAllGraphData` task
like so:

    python build_graphs.py BuildAllGraphData --start 2000 --end 2005 --local-scheduler

This will produce the following output files:

    paper-citation-graph-2000-2005.pickle.gz
    paper-citation-graph-2000-2005.graphml.gz
    paper-id-to-node-id-map-2000-2005.csv
    author-citation-graph-2000-2005.graphml.gz
    author-id-to-node-id-map-2000-20005.csv
    lcc-author-citation-graph-2000-2005.csv
    lcc-author-citation-graph-2000-2005.edgelist.txt
    lcc-author-citation-graph-2000-2005.pickle.gz
    lcc-author-id-to-node-id-map-2000-2005.csv
    lcc-venue-id-map-2000-2005.csv
    lcc-ground-truth-by-venue-2000-2005.txt
    lcc-author-venues-2000-2005.txt

Note that the dates will be absent when running without `start` and `end`. So
for instance, the last file would be `lcc-author-venues.txt` instead.

TODO: Add info on each data file

## Module 4: Representative Documents

Module: `repdocs`
Location: `repdocs/`
Config: `repdoc_dir`

This module creates representative documents (repdocs) for each paper by
concatenating the title and abstract with a space between. Subseqeunt processing
treats these documents as a corpus to construct term frequency (tf) attributes
for each paper. Note that the tf corpus is the well-known bag-of-words (BoW)
representation.

Since experiments may also be concerned with authors as nodes in a network, such
as in the LCC author citation graph constructed by the `build_graphs` module,
repdocs are also created for each author. The repdoc for a person is built by
concatenating the repdocs of all papers authored. These are then treated in the
same manner as paper repdocs to build a tf corpus. Term-frequency
inverse-document-frequency (tfidf) weighting is also applied to produce an
additional corpus file for authors.

All data can be produced by running:

    python repdocs.py BuildAllRepdocData --start 2013 --end 2013 --local-scheduler

The following files are produced:

    repdoc-by-paper-2013-2013.csv
    repdoc-by-paper-vectors-2013-2013.csv
    repdoc-by-paper-corpus-2013-2013.dict
    repdoc-by-paper-corpus-2013-2013.mm
    repdoc-by-paper-corpus-2013-2013.mm.index
    paper-id-to-repdoc-id-map-2013-2013.csv
    repdoc-by-author-vectors-2013-2013.csv
    lcc-repdoc-corpus-tf-2013-2013.mm
    lcc-repdoc-corpus-tf-2013-2013.mm.index
    lcc-repdoc-corpus-tfidf-2013-2013.mm
    lcc-repdoc-corpus-tfidf-2013-2013.mm.index

Note that the files prefixed with `lcc-` are dependent upon the output of the
`build_graphs` module, since the author ids from the LCC author citation graph
are used to filter down the author repdocs used to build the corpus.

TODO: add explanation of data files

## Building All

To build all data files for a particular range of years, simply run:

    python pipeline.py BuildDataset --start <int> --end <int> --local-scheduler

The `start` and `end` arguments can be omitted to build all data files for the
whole dataset. In addition, a `--workers <int>` flag can be used to specify
level of multiprocessing to be used. The dependency chain will limit this in
some places throughout the processing, but it can provide a signficant speedup
overall.

# Input Data Format

## Paper Format (V8)

The papers in the dataset are represented using a custom non-tabular format
which allows for all papers to be stored in the same file in sequential blocks.
This is the specification:

    #index ---- index id of this paper
    #* ---- paper title
    #@ ---- authors (separated by semicolons)
    #o ---- affiliations (separated by semicolons, and each affiliaiton corresponds to an author in order)
    #t ---- year
    #c ---- publication venue
    #% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)
    #! ---- abstract

The following is an example:

    #index 1083734
    #* ArnetMiner: extraction and mining of academic social networks
    #@ Jie Tang;Jing Zhang;Limin Yao;Juanzi Li;Li Zhang;Zhong Su
    #o Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;Tsinghua University, Beijing, China;IBM, Beijing, China;IBM, Beijing, China
    #t 2008
    #c Proceedings of the 14th ACM SIGKDD international conference on Knowledge discovery and data mining
    #% 197394
    #% 220708
    #% 280819
    #% 387427
    #% 464434
    #% 643007
    #% 722904
    #% 760866
    #% 766409
    #% 769881
    #% 769906
    #% 788094
    #% 805885
    #% 809459
    #% 817555
    #% 874510
    #% 879570
    #% 879587
    #% 939393
    #% 956501
    #% 989621
    #% 1117023
    #% 1250184
    #! This paper addresses several key issues in the ArnetMiner system, which aims at extracting and mining academic social networks. Specifically, the system focuses on: 1) Extracting researcher profiles automatically from the Web; 2) Integrating the publication data into the network from existing digital libraries; 3) Modeling the entire academic network; and 4) Providing search services for the academic network. So far, 448,470 researcher profiles have been extracted using a unified tagging approach. We integrate publications from online Web databases and propose a probabilistic framework to deal with the name ambiguity problem. Furthermore, we propose a unified modeling approach to simultaneously model topical aspects of papers, authors, and publication venues. Search services such as expertise search and people association search have been provided based on the modeling results. In this paper, we describe the architecture and main features of the system. We also present the empirical evaluation of the proposed methods.

## Name Disambiguation

From the data, it appears the AMiner group did not perform any name
disambiguation. This has led to a dataset with quite a few duplicate author
records. This package currently does not address these issues.

The most obvious examples are those where the first or second name is
abbreviated with a single letter in one place and spelled out fully in another.
Use of dots and/or hyphens in some places also leads to different entity
mappings. Another case that is quite common is when hyphenated names are spelled
in some places with the hyphen and in some without.

There are also simple common misspellings, although these are harder to detect,
since an edit distance of 1 or 2 could just as easily be a completely different
name. One case which might be differentiated is when the edit is a deletion of a
letter in a string of one or more of that same letter. For instance, "Acharya"
vs. "Acharyya". Here it likely the second spelling simply has an extraneous y.
