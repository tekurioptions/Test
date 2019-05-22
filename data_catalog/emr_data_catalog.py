from data_catalog import get_catalog, CsvDataset, \
    ParquetDataset, PickleDataset, Pipeline

import logging

logger = logging.getLogger('data_catalog')
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.INFO)
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)
# Define datasets
# ------------------------------------------------------------------------------

# Raw iris dataset, defined from a file
CsvDataset(
    'raw_molecules', description='Raw iris dataset', relpath='raw/molecules_classification.tsv',
    read_kwargs= {'sep' : '\t'})

# def test(x):
#     return x
#
# ParquetDataset('cln_molecules2', description='Raw iris dataset', create=test, parents=['raw_molecules'])
#


# logger.info('DONE')
# CsvDataset(
#     'raw_molecules', description='EMR molecules classification', relpath='staging/01-raw/emr/molecules_classification.tsv')

# Cleaned iris dataset, defined from its generating function
@ParquetDataset.from_parents('Cleaned iris dataset')
def cln_molecules(raw_molecules):
    new_col_names = {
        name: name.lower().replace(' ', '_') for name in raw_molecules.columns}
    result = raw_molecules.rename(columns=new_col_names)
    return result

@ParquetDataset.from_parents('Base iris dataset')
def bse_molecules(cln_molecules):
    return cln_molecules


# Generate and use datasets from data catalog
# ------------------------------------------------------------------------------
# Load a catalog containing all datasets defined so far
data_catalog = get_catalog(
    's3://ucb-qb-ca-eu-west-1-data/demos/demo-carbonai/data/')
# print(data_catalog.describe())
#
# print(data_catalog['cln_molecules2'].path)
#
# print(data_catalog['raw_molecules'].path)
#
# if data_catalog['raw_molecules'].exists():
#     df1 = data_catalog['raw_molecules'].read()
#     # print(df1)
#     print('Test')

# data_catalog = get_catalog(
#     's3://ucb-qb-ca-eu-west-1-data/ca4i-fr-data/')
# data_catalog.describe()

# Generate all datasets using the pipeline
# The pipeline will not regenerate an existing file, unless
# its parents have a more recent modification time.
pipeline = Pipeline(data_catalog)
pipeline.run()

# Visualize the pipeline (requires graphviz and python-graphviz)
# pipeline.visualize('my_demo_pipeline.pdf')

# Read any of the datasets
df_bse_molecules = data_catalog['bse_molecules'].read()
df_cln_molecules = data_catalog['cln_molecules'].read()
print(df_bse_molecules.count())
print(df_cln_molecules.count())
