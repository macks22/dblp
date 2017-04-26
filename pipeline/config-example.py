import os
pjoin = os.path.join

base_dir = '/data/username/aminer-network'
data_dir = pjoin(base_dir, 'data')
originals_dir = pjoin(data_dir, 'original-data')
base_csv_dir = pjoin(data_dir, 'base-csv')
filtered_dir = pjoin(data_dir, 'filtered-csv')
repdoc_dir = pjoin(data_dir, 'repdocs')
graph_dir = pjoin(data_dir, 'graphs')
