"""Demonstration of Data Catalog
"""
from data_catalog import get_catalog, CsvDataset, \
    ParquetDataset, PickleDataset, Pipeline


# Define datasets
# ------------------------------------------------------------------------------
# Raw iris dataset, defined from a file
CsvDataset(
    'raw_iris', description='Raw iris dataset', relpath='raw/iris.csv')

# Cleaned iris dataset, defined from its generating function
@ParquetDataset.from_parents('Cleaned iris dataset')
def cln_iris(raw_iris):
    new_col_names = {
        name: name.lower().replace(' ', '+') for name in raw_iris.columns}
    result = raw_iris.rename(columns=new_col_names)
    return result

# Base iris dataset, defined from its generating function
@ParquetDataset.from_parents('Base iris dataset')
def bse_iris(cln_iris):
    return 5 + 2 * cln_iris.select_dtypes('number')


# Generate and use datasets from data catalog
# ------------------------------------------------------------------------------
# Load a catalog containing all datasets defined so far
data_catalog = get_catalog(
    's3://ucb-qb-ca-eu-west-1-data/demos/demo-carbonai/data/')
data_catalog.describe()

# Generate all datasets using the pipeline
# The pipeline will not regenerate an existing file, unless
# its parents have a more recent modification time.
pipeline = Pipeline(data_catalog)
pipeline.run()

# Visualize the pipeline (requires graphviz and python-graphviz)
pipeline.visualize('my_demo_pipeline.pdf')

# Read any of the datasets
df = data_catalog['bse_iris'].read()
print(df)
