import os
import csv
import codecs
import cStringIO

import igraph
import luigi

import config


class YearFilterableTask(luigi.Task):
    start = luigi.IntParameter(default=None)
    end = luigi.IntParameter(default=None)

    @property
    def base_paths(self):
        return 'year-filteable-data.csv'

    @property
    def base_dir(self):
        return config.repdoc_dir

    def build_path(self, base_path):
        if self.start is None or self.end is None:
            return base_path
        else:
            try:
                ext_index = base_path.index('.')
                base = base_path[:ext_index]
                ext = base_path[ext_index:]
            except ValueError:
                base = base_path
                ext = ''
            return '%s-%d-%d%s' % (base, self.start, self.end, ext)

    def output(self):
        if isinstance(self.base_paths, basestring):
            base_path = self.build_path(self.base_paths)
            return luigi.LocalTarget(os.path.join(self.base_dir, base_path))
        else:
            base_paths = [self.build_path(path) for path in self.base_paths]
            return [luigi.LocalTarget(os.path.join(self.base_dir, path))
                    for path in base_paths]


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


def read_to_newline(f):
    line = f.readline().strip()
    while line:
        line = f.readline().strip()


def write_csv_to_fwrapper(fwrapper, header, rows):
    """Write csv records to already opened file handle."""
    with fwrapper.open('w') as f:
        writer = csv.writer(f)
        if header: writer.writerow(header)
        writer.writerows(rows)


def iter_csv_fwrapper(csv_fwrapper):
    with csv_fwrapper.open() as f:
        reader = csv.reader(f)
        reader.next()
        for record in reader:
            yield record


def write_csv(fname, header, rows):
    """Write an iterable of records to a csv file with optional header."""
    if not fname.endswith('.csv'):
        fname = '%s.csv' % os.path.splitext(fname)[0]

    with open(fname, 'w') as f:
        writer = csv.writer(f)
        if header: writer.writerow(header)
        writer.writerows(rows)


def yield_csv_records(csv_file):
    """Iterate over csv records, returning each as a list of strings."""
    f = csv_file if isinstance(csv_file, file) else open(csv_file)
    reader = csv.reader(f)
    reader.next()
    for record in reader:
        yield record
    f.close()


def smart_open(file_like, mode='r'):
    try:
        return open(file_like, mode)
    except TypeError:
        try:
            return file_like.open(mode)
        except (TypeError, AttributeError):
            return file_like


def swap_file_delim(infile, indelim, outfile, outdelim):
    """Swap out every instance of `indelim` in the input file for `outdelim` and
    write to `outfile`. `infile` can be either a filename or file descriptor,
    but `outfile` must be a filename.
    """
    rf = smart_open(infile, 'r')
    wf = smart_open(outfile, 'w')

    in_lines = (l.strip().split(indelim) for l in rf)
    out_lines = (outdelim.join(l) for l in in_lines)
    wf.write('\n'.join(out_lines))

    rf.close()
    wf.close()


def build_and_save_idmap(graph, outfile, idname='author'):
    """Save vertex ID to vertex name mapping and then return it."""
    first_col = '%s_id' % idname
    idmap = {v['name']: v.index for v in graph.vs}
    rows = sorted(idmap.items())
    write_csv(outfile, (first_col, 'node_id'), rows)
    return idmap


def build_idmap(graph):
    idmap = {v['name']: v.index for v in graph.vs}
    return idmap


def read_idmap(idmap_fname):
    records = util.yield_csv_records(idmap_fname)
    idmap = {record[0]: int(record[1]) for record in records}
    return idmap


def build_undirected_graph(nodes, edges):
    """Build an undirected graph, removing duplicates edges."""
    graph = igraph.Graph()
    graph.add_vertices(nodes)
    graph.add_edges(edges)
    graph.simplify()
    return graph


def flatten(struct):
    """
    Creates a flat list of all all items in structured output (dicts, lists, items):
    .. code-block:: python
        >>> flatten({'a': 'foo', 'b': 'bar'})
        ['foo', 'bar']
        >>> flatten(['foo', ['bar', 'troll']])
        ['foo', 'bar', 'troll']
        >>> flatten('foo')
        ['foo']
        >>> flatten(42)
        [42]
    """
    if struct is None:
        return []
    flat = []
    if isinstance(struct, dict):
        for key, result in struct.iteritems():
            flat += flatten(result)
        return flat
    if isinstance(struct, basestring):
        return [struct]

    try:
        # if iterable
        for result in struct:
            flat += flatten(result)
        return flat
    except TypeError:
        pass

    return [struct]
