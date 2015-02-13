"""
This script evaluates community detection methods against ground truth using
four metrics: precision, recall, f1-score, and the jaccard coefficient.
Found community files are searched for by default by attempting to find a
directory in the cwd called "found". The ground community file is discovered
by attempting to find a file that starts with "ground" in the current
directory. If more than one is found, the first encountered will be
used.

All metrics are calculated by computing max row-wise and column-wise
similarity, then averaging the two. Stated differently, the similarity
matrix is constructed by computing a similarity from every found to every
ground community. Then a found -> ground optimal matching is obtained by
taking an argmax across the rows; this is the row-wise maximum. The col-wise
max is obtained by taking an argmax across the columns. These matchings are
labeled as "fg" and "gf" in outputs.

The primary metrics of interest in this evaluation scheme are f1-score and
jaccard coefficient. As a results, the precision and recall are not
maximized by themselves. Instead, the optimal matching is selected to
maximize f1-score, then that same matching of found -> ground and ground ->
found communities is used to calculate the precision and recall scores.
Separate matchings are found for the jaccard coefficient in order to
maximize it separately. These matchings are available with "-m" flag.

"""
import os
import sys
import argparse
import logging
import itertools

import numpy as np
import matplotlib.pyplot as plt
from pandas import DataFrame


tableau20 = [
    (31,119,180),
    (148,103,189),
    (255,187,120),
    (44,160,44),
    (255,152,150),
    (140,86,75),
    (227,119,194),
    (127,127,127),
    (188,189,34),
    (23,190,207),
    (174,199,232),
    (255,127,14),
    (152,223,138),
    (214,39,40),
    (197,176,213),
    (196,156,148),
    (247,182,210),
    (199,199,199),
    (219,219,141),
    (158,218,229)
]
to_rgb_float = lambda num: num / 255.0
palette = [tuple(map(to_rgb_float, tup)) for tup in tableau20]
basic_colors = ['b', 'g', 'r', 'y', 'm', 'c']


def sim_matrix(found_list, ground_list, metric):
    """Calculate pairwise similarity of found/ground communities using the given
    metric.
    """
    # TODO: declare types and remove boundschecking and wrapping with Cython.
    sims = np.zeros((len(found_list), len(ground_list)))
    for i, found in enumerate(found_list):
        sims[i] = [metric(ground, found) for ground in ground_list]
    return sims

def jaccard(ground, found):
    """Cardinality of set intersection / cardinality of set union."""
    ground = set(ground)
    return len(ground.intersection(found))/float(len(ground.union(found)))

def precision(ground, found):
    """Fraction of community members found actually in ground-truth community."""
    ground = set(ground)
    return len(ground.intersection(found))/float(len(found))

def recall(ground, found):
    """Fraction of ground-truth community members found."""
    ground = set(ground)
    return len(ground.intersection(found))/float(len(ground))

def f1_score(ground, found):
    """Harmonic mean of precision and recall (2pr / (p+r))."""
    p = precision(ground, found)
    r = recall(ground, found)
    bot = float(p + r)
    if bot == 0: return bot
    return 2*p*r / bot

def fpr_matrix(found_list, ground_list):
    all_ground = np.array(list(itertools.chain.from_iterable(ground_list)))
    size_all_ground = float(len(np.unique(all_ground)))
    glens = np.array([float(len(g)) for g in ground_list])
    negatives = size_all_ground - glens
    fp_mat = np.zeros((len(found_list), len(ground_list)))

    for i, flist in enumerate(found_list):
        found = set(flist)
        fp_mat[i] = [len(found.difference(ground)) for ground in ground_list]

    return fp_mat / negatives


def similarity_matrices(found_list, ground_list):
    gsets = [set(g) for g in ground_list]
    glens = np.array([float(len(g)) for g in ground_list])
    flens = np.matrix([float(len(f)) for f in found_list])
    flens = np.array(np.tile(flens.transpose(), len(ground_list)))

    logging.info('calculating true positive values')
    tp_mat = np.zeros((len(found_list), len(ground_list)))
    for i, found in enumerate(found_list):
        tp_mat[i] = [len(ground.intersection(found)) for ground in gsets]

    logging.info('calculating found/ground set union sizes')
    union_mat = np.zeros((len(found_list), len(ground_list)))
    for i, found in enumerate(found_list):
        union_mat[i] = [len(ground.union(found)) for ground in gsets]

    logging.info('calculating recall matrix')
    recall = tp_mat / glens

    logging.info('calculating precision matrix')
    precision = tp_mat / flens

    logging.info('calculating f1-score matrix')
    f1 = 2 * recall * precision / (recall + precision)
    f1[np.isnan(f1)] = 0.0  # fill NaN values from 0 division

    logging.info('calculating jaccard coefficient matrix')
    jaccard = tp_mat / union_mat

    return (recall, precision, f1, jaccard)


def recall_matrix(found_list, ground_list):
    gsets = [set(g) for g in ground_list]
    glens = np.array([float(len(g)) for g in ground_list])
    tp_mat = np.zeros((len(found_list), len(ground_list)))
    for i, found in enumerate(found_list):
        tp_mat[i] = [len(ground.intersection(found)) for ground in gsets]
    return (tp_mat / glens)

def precision_matrix(found_list, ground_list):
    gsets = [set(g) for g in ground_list]
    flens = np.matrix([float(len(f)) for f in found_list])
    tp_mat = np.zeros((len(found_list), len(ground_list)))
    for i, found in enumerate(found_list):
        tp_mat[i] = [len(ground.intersection(found)) for ground in gsets]
    return np.array(tp_mat / np.tile(flens.transpose(), tp_mat.shape[1]))

def jaccard_matrix(found_list, ground_list):
    gsets = [set(g) for g in ground_list]
    int_mat = np.zeros((len(found_list), len(ground_list)))
    union_mat = np.zeros((len(found_list), len(ground_list)))
    for i, found in enumerate(found_list):
        int_mat[i] = [len(ground.intersection(found)) for ground in gsets]
    for i, found in enumerate(found_list):
        union_mat[i] = [len(ground.union(found)) for ground in gsets]
    return int_mat / union_mat


def read_lines(fpath):
    """Return all lines in a file as a list."""
    with open(fpath) as f:
        return [line.strip() for line in f.read().split('\n') if line.strip()]

def extract_comms(lines, delim):
    return [line.split(delim) for line in lines]


def get_edcar_comm(row, num_features=55545):
    """Each line in the EDCAR found community file consists of a 0 or 1 for each
    feature, followed by an integer that represents the number of communities,
    followed by the node IDs in the found community. This function takes the
    number of features, looks up the number of nodes in the community, and uses
    that to splice and return a list of the node IDs in the found community.
    """
    return row[-int(row[num_features]):]


def extract_edcar_comms(lines):
    """The last 2 lines in an EDCAR found community file are statistics. Each
    remaining line must be parsed to get the node IDs. See `get_edcar_comm` for
    more info.
    """
    rows = [row.split() for row in lines[:-2]]  # last 2 are stats
    return [get_edcar_comm(row) for row in rows]


def read_communities(fpath):
    """Takes the path of a found community data file and returns the name of the
    file (minus the extension), with the list of communities read from the file.

    :param str fpath: Path of the found community file. The name will be used as
        the name of the method in statistical output, and the extension will be
        used to determine how to read the file.
    :return: list of lists of strings, where each string is a node id.
    :raises Exception: if file extension is not one of {'csv', 'tsv', 'edcar'}.
    """
    lines = read_lines(fpath)
    basename = os.path.basename(fpath)
    pieces = os.path.splitext(basename)
    name, ext = pieces[0], pieces[1].replace('.','')
    if ext == 'csv':
        return (name, extract_comms(lines, ','))
    elif ext == 'tsv':
        return (name, extract_comms(lines, '\t'))
    elif ext == 'edcar':
        return (name, extract_edcar_comms(lines))
    else:
        raise Exception('Unrecognized file extension: %s' % ext)


def read_ground(fpath=None):
    """Read the ground communities.

    :param str fpath: Path of the file to read ground communities from. If none
        is given, the cwd will be searched for the first filepath that starts
        with 'ground' and that file will be used.
    """
    if fpath is None:
        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        fpath = [f for f in files if f.startswith('ground')][0]
    return read_communities(fpath)


def iterfound(fdir=None):
    """Iterate through files in `fdir`, treating each as a file of found
    communities and reading them with `read_communities`. A generator is returned that
    yields each found community list in alphabetic order by file name.

    :return: generator which yields tuples of (name, communities), where
        communities is a list of lists of strings.
    """
    fdir = os.path.join(os.getcwd(), 'found') if fdir is None else fdir
    foundfiles = sorted([os.path.join(fdir, f) for f in os.listdir(fdir)])
    logging.info(
        'discovered %d found community files to evaluate' % len(foundfiles))
    return (read_communities(f) for f in foundfiles)


# SLOWER, BUT MORE GRANULAR CONTROL - MAYBE USEFUL FOR MORE CLI OPTIONS
def old_score_communities(found, ground):
    logging.info('calculating recall matrix')
    r = recall_matrix(found, ground)
    logging.info('calculating precision matrix')
    p = precision_matrix(found, ground)
    logging.info('calculating f1-score matrix')
    f1 = 2 * r * p / (r + p)
    f1[np.isnan(f1)] = 0.0  # fill NaN values from 0 division
    logging.info('calculating jaccard coefficient matrix')
    jac = jaccard_matrix(found, ground)
    logging.info('calculating false positive rate matrix')
    fpr = fpr_matrix(found, ground)
    return {
        'recall': r,
        'precision': p,
        'f1-score': f1,
        'jaccard': jac,
        'fpr': fpr
    }

def score_communities(found, ground):
    r, p, f1, jac = similarity_matrices(found, ground)
    logging.info('calculating false positive rate matrix')
    fpr = fpr_matrix(found, ground)
    return {
        'recall': r,
        'precision': p,
        'f1-score': f1,
        'jaccard': jac,
        'fpr': fpr
    }


def find_lengths(community):
    return np.array([len(c) for c in community])

def best_matching(mat, direction='row'):
    axis = 1 if direction == 'row' else 0
    return mat.argmax(axis)

def write_matching(fpath, matching, scores, order='row'):
    """Take an array of values and interpet it either as row indices or column
    indices. It is assumed the matching was in order from 0 to len(matching).

    :param str fpath: Path of file to write matches to.
    :param arr matching: Iterable of ints, with each int being a row or col
        index, depending on the value of `order`.
    :param arr scores: Iterable of scores (floats) for each matching.
    :param str order: One of {'row','col'}. If 'row', the indices of `matching`
        are used as the column values, otherwise the indices are used as row
        values.
    """
    with open(fpath, 'w') as f:
        if order == 'row':
            matchings = ["%s,%s,%f" % (row, col, scores[col])
                         for col, row in enumerate(matching)]
        else:
            matchings = ["%s,%s,%f" % (row, col, scores[row])
                         for row, col in enumerate(matching)]
        f.write('\n'.join(matchings))


def calculate_stats(found, ground, write_matchings=False, match_prefix=''):
    r, p, f1, j = similarity_matrices(found, ground)
    logging.info('calculating false positive rate matrix')
    fpr = fpr_matrix(found, ground)

    sim_matrices = (r, p, f1)

    logging.info('finding best matches')
    fg_match = best_matching(f1, 'row')
    gf_match = best_matching(f1, 'col')
    fg_jac_match = best_matching(j, 'row')
    gf_jac_match = best_matching(j, 'col')
    fg_fpr_match = best_matching(fpr, 'row')
    gf_fpr_match = best_matching(fpr, 'col')

    fgmask = lambda mat: mat[np.arange(len(fg_match)), fg_match]
    fg_jac = j[np.arange(j.shape[0]), fg_jac_match]
    fg_fpr = fpr[np.arange(fpr.shape[0]), fg_fpr_match]
    fg_matrices = map(fgmask, sim_matrices)
    fg_matrices += [fg_jac, fg_fpr]

    gfmask = lambda mat: mat[gf_match, np.arange(len(gf_match))]
    gf_jac = j[gf_jac_match, np.arange(j.shape[1])]
    gf_fpr = fpr[gf_fpr_match, np.arange(fpr.shape[1])]
    gf_matrices = map(gfmask, sim_matrices)
    gf_matrices += [gf_jac, gf_fpr]

    if write_matchings:
        logging.info('writing matches')
        mdir = os.path.abspath('matchings')
        try: os.mkdir(mdir)
        except OSError: pass

        name_file = lambda s: os.path.join(mdir, "%s-%s" % (match_prefix, s))
        write_matching(name_file('fg-f1-matches.csv'),
                       fg_match, fg_matrices[-1], 'col')
        write_matching(name_file('gf-f1-matches.csv'),
                       gf_match, gf_matrices[-1], 'row')
        write_matching(name_file('fg-jaccard-matches.csv'),
                       fg_jac_match, fg_jac, 'col')
        write_matching(name_file('gf-jaccard-matches.csv'),
                       gf_jac_match, gf_jac, 'row')
        write_matching(name_file('fg-fpr-matches.csv'),
                       fg_fpr_match, fg_fpr, 'col')
        write_matching(name_file('gf-fpr-matches.csv'),
                       gf_fpr_match, gf_fpr, 'row')

    labels = ('fg-r','fg-p','fg-f1','fg-j', 'fg-fpr',
              'gf-r','gf-p','gf-f1','gf-j', 'gf-fpr',
              'r','p','f1','j', 'fpr')

    logging.info('calculating evaluation statistics')
    fgvals = map(np.mean, fg_matrices)
    gfvals = map(np.mean, gf_matrices)
    vals = list((np.array(fgvals) + np.array(gfvals)) / 2)
    stats = zip(labels, fgvals + gfvals + vals)
    return dict(stats)


def check_extension(fname, ext_fill='pdf'):
    name, ext = os.path.splitext(fname)
    if not ext: fname = "%s.%s" % (name, ext_fill)
    return fname


def write_barplot(fname, df, labels, sortby='f1', title=''):
    """Write a barplot from the DataFrame given.

    :param str fname: Path of file to write barplot to. If no extension is
        given, pdf will be used.
    :param list labels: Ordering of bars to use in each column.
    :param str sortby: Column to sort methods by (greatest -> least).
    :param str title: Title of the barplot.
    """
    plt.cla()
    fig, ax = plt.subplots()
    frame = df[labels].reindex_axis(labels, 1).sort(sortby, ascending=False)
    frame.plot(kind='bar', color=palette[:len(labels)], width=0.85,
               title=title, figsize=(24,16), ax=ax)

    ncols = len(labels)/2
    ncols = 4 if ncols < 4 else ncols
    ax.legend(loc='upper center', ncol=ncols,
              fancybox=True, shadow=True)
    ax.xaxis.grid(False)

    fname = check_extension(fname)
    fig.savefig(fname, bbox_inches='tight', dpi=300)
    fig.clear()


def write_precision_recall(fname, df):
    plt.cla()
    fig, ax = plt.subplots()

    ax.scatter(df['p'], df['r'], color=palette[0])
    ax.set_title('Recall vs. Precision')
    ax.set_xlim(1.0)
    ax.set_ylim(1.0)
    ax.set_xlabel('Precision')
    ax.set_ylabel('Recall')

    fname = check_extension(fname)
    fig.savefig(fname, bbox_inches='tight', dpi=300)
    fig.clear()


def write_roc_curve(fname, df):
    plt.cla()
    fig, ax = plt.subplots()

    frame = df[['fpr','r']].sort('fpr')
    ax.plot(frame['fpr'], frame['r'], color=palette[0])
    ax.set_title('ROC Curve')
    ax.set_ylim(1.0)
    ax.set_ylabel('TPR')
    ax.set_xlabel('FPR')

    fname = check_extension(fname)
    fig.savefig(fname, bbox_inches='tight', dpi=300)
    fig.clear()


def write_boxplot(fname, data, labels, title=''):
    plt.cla()
    fig, ax = plt.subplots()
    ax.boxplot(data)

    ax.set_xticklabels(labels)
    if title: ax.set_title(title)
    fig.set_size_inches(24,16)

    fname = check_extension(fname)
    fig.savefig(fname, bbox_inches='tight', dpi=300)
    fig.clear()


def write_histogram(fname, data, title=''):
    plt.cla()
    fig, ax = plt.subplots(figsize=(24,16))
    drange = data.max() - data.min()
    num_bins = drange / 3
    num_bins = 10 if num_bins < 10 else num_bins
    n, bins, patches = ax.hist(data, num_bins, color='b', alpha=0.8)

    ax.set_xticks(bins.round(1))
    if title: ax.set_title(title)
    fig.set_size_inches(24,16)

    fname = check_extension(fname)
    fig.savefig(fname, bbox_inches='tight', dpi=300)
    fig.clear()


def plot_roc_curve(csvfile):
    """`csvfile` should be the csvfile of the evaluation statistics from the
    SENC parameter sweep.
    """
    df = DataFrame.from_csv(csvfile)
    fig, ax = plt.subplots()
    frame = df[['fpr', 'r']].sort('fpr')
    ax.plot(frame['fpr'], frame['r'], color='black', alpha=0.85,
            linewidth=3)
    ax.set_title('ROC Curve')
    ax.set_ylabel('TPR')
    ax.set_xlabel('FPR')
    ax.set_ylim(0,1)

    # x values are FPR and y values are RECALL
    # metrics obtained from comparison on connected components ground truth
    cesna_x = 0.0040484809285604717
    cesna_y = 0.49224591663028472
    coda_x = 0.0032235301223333371
    coda_y = 0.41731067592697141
    edcar_x = 0.0013878215161439689
    edcar_y = 0.20758049657661148

    #ax.plot(cesna_x, cesna_y, color='r', alpha=0.8, marker='o', ms=10)
    ax.plot(cesna_x, cesna_y, color='black', marker='o', ms=10)
    plt.annotate('CESNA', xy=(cesna_x, cesna_y),
            ha='center', va='bottom', textcoords='offset points',
            xytext=(0, 3.5))

    #ax.plot(coda_x, coda_y, color='g', alpha=0.8, marker='o', ms=10)
    ax.plot(coda_x, coda_y, color='black', marker='o', ms=10)
    plt.annotate('CODA', xy=(coda_x, coda_y),
            ha='center', va='bottom', textcoords='offset points',
            xytext=(0, 3))

    #ax.plot(edcar_x, edcar_y, color=palette[0], alpha=0.8, marker='o', ms=10)
    ax.plot(edcar_x, edcar_y, color='black', marker='o', ms=10)
    plt.annotate('EDCAR', xy=(edcar_x, edcar_y),
            ha='center', va='bottom', textcoords='offset points',
            xytext=(0, 3.5))

    fig.savefig('roc-curve-with-competitor-points', bbox_inches='tight', dpi=300)
    fig.clear()


def make_parser():
    parser = argparse.ArgumentParser(
        description="Community detection evaluation.")
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='enable verbose output')
    parser.add_argument(
        '-f', '--found-dir', action='store',
        default=os.path.join(os.getcwd(), 'found'),
        help='specify directory of found community files')
    parser.add_argument(
        '-g', '--ground-file', action='store', default=None,
        help='specify the file of ground communities to compare against')
    parser.add_argument(
        '-s', '--summary', action='store_true',
        help='write a summary bar plot with final metrics for each method')
    parser.add_argument(
        '-d', '--detail', action='store_true',
        help='write detailed bar plot with all metrics for each method')
    parser.add_argument(
        '-r', '--roc', action='store_true',
        help='plot a ROC curve from the stats obtained')
    parser.add_argument(
        '-p', '--precision-recall', action='store_true',
        help='plot precision vs. recall from stats obtained')
    parser.add_argument(
        '-b', '--boxplot-sizes', action='store_true',
        help='write boxplot of community sizes')
    parser.add_argument(
        '-i', '--histogram-sizes', action='store_true',
        help='write histogram of community sizes')
    parser.add_argument(
        '-m', '--matchings', action='store_true',
        help='write the maximal f1 and jaccard matchings to a file')
    parser.add_argument(
        '-t', '--table', action='store_true',
        help='output the evaluation stats to a table')
    parser.add_argument(
        '-a', '--all', action='store_true',
        help='output all files/graphs (eclipses other args)')
    parser.add_argument(
        '--plot-roc-from-csv', action='store', default='',
        help='give a CSV file with FPR/RECALL values to plot in ROC space')
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()

    if args.plot_roc_from_csv:
        plot_roc_curve(args.plot_roc_from_csv)
        sys.exit(0)

    if args.verbose:
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(message)s',
            datefmt='%H:%M:%S')

    if args.all:
        get_stats = True
        get_lengths = True
        args.matchings = True
        logging.info('all graphs and stats files will be output')
    else:
        get_stats = (args.detail or args.summary or args.table or args.roc
                     or args.precision_recall)
        get_lengths = args.boxplot_sizes or args.histogram_sizes

    if not (get_stats or get_lengths):
        print "no output selected; no calculations performed"
        sys.exit(0)

    founditer = iterfound(args.found_dir)

    if get_stats:
        logging.info('found community evaluation stats will be calculated')
        stats = {}
        ground_name, ground = read_ground(args.ground_file)
        logging.info('using ground-truths from: %s' % ground_name)

    if get_lengths:
        logging.info('community length stats will be calculated')
        lengths = {}

    for method_name, found in founditer:
        logging.info('processing found communities for %s' % method_name)
        if get_lengths:
            lengths[method_name] = find_lengths(found)
        if get_stats:
            logging.info('evaluating %s' % method_name)
            stats[method_name] = calculate_stats(
                found, ground, args.matchings, method_name)

    if get_stats:
        df = DataFrame.from_dict(stats).transpose()
        all_labels = ('fg-r','fg-p','fg-f1','fg-j', 'fg-fpr',
                      'gf-r','gf-p','gf-f1','gf-j', 'gf-fpr',
                      'r','p','f1','j', 'fpr')
        df = df.reindex_axis(all_labels, 1).sort('f1', ascending=False)

    if args.all or args.detail:
        logging.info('writing detailed barplot for community evaluations')
        labels = ['fg-r','fg-p','fg-f1', 'gf-r','gf-p','gf-f1',
                  'r','p','f1', 'fg-j', 'gf-j', 'j']
        write_barplot(
            'detailed-metrics-barplot', df, labels, 'f1',
            'Detailed evaluation of community detection methods')

    if args.all or args.summary:
        logging.info('writing summary barplot for community evaluations')
        summary_labels = ['r','p','f1','j']
        write_barplot(
            'summary-metrics-barplot', df, summary_labels, 'f1',
            'Summary evaluation of community detection methods')

    if args.all or args.roc:
        logging.info('plotting ROC curve from evaluation stats')
        write_roc_curve('roc-curve', df)

    if args.all or args.precision_recall:
        logging.info('plotting precision vs. recall from evaluation stats')
        write_precision_recall('precision-v-recall', df)

    if args.all or args.boxplot_sizes:
        logging.info('writing boxplot for found community sizes')
        write_boxplot(
            'boxplot-sizes-found-communities.pdf',
            lengths.values(), lengths.keys(),
            'Lengths of Found Communities')

    if args.all or args.histogram_sizes:
        logging.info('writing histograms for found community sizes')
        hdir = os.path.abspath('histograms')
        try: os.mkdir(hdir)
        except OSError: pass
        for method in lengths:
            fname = "%s-comm-sizes-histogram.pdf" % method
            fpath = os.path.join(hdir, fname)
            write_histogram(
                fpath, lengths[method],
                '%s: Lengths of Found Communities' % method)

    if args.all or args.table:
        logging.info('\n' + str(df))
        df.to_csv('evaluation-stats.csv')
