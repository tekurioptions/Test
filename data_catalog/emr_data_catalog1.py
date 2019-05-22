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

class EmrDataCatalog:
    CsvDataset(
        'raw_persontreatment', description='Raw Person Treatment dataset', relpath='raw/PERSON_TREATMENT.csv',
        read_kwargs={'sep': ';', 'encoding': 'ISO-8859-1'})
    def __init__(self, raw_df, desc, relpath):
       pass

    @staticmethod
    @ParquetDataset.from_parents('Cleaned person treatment dataset')
    def cln_persontreatment(raw_persontreatment):
        new_col_names = {
            name: name.lower().replace(' ', '_') for name in raw_persontreatment.columns}
        result = raw_persontreatment.rename(columns=new_col_names)
        return result

    @staticmethod
    @ParquetDataset.from_parents('Base person treatment dataset')
    def bse_persontreatment(cln_persontreatment):
        return cln_persontreatment

    @staticmethod
    def run_pipeline():
        data_catalog = get_catalog(
            's3://ucb-qb-ca-eu-west-1-data/demos/demo-carbonai/data/')
        pipeline = Pipeline(data_catalog)
        pipeline.run()
        df_bse_person_treatment = data_catalog['bse_persontreatment'].read()
        df_cln_person_treatment = data_catalog['cln_persontreatment'].read()
        print(df_bse_person_treatment.count())
        print(df_cln_person_treatment.count())


# Define datasets
# ------------------------------------------------------------------------------
# import sys
# import pandas as pd
# # df = pd.read_csv('/home/kumar.tekurinagendra/PERSON_TREATMENT.csv', sep=';', encoding='ISO-8859-1')
#
# # df = pd.read_csv('s3://ucb-qb-ca-eu-west-1-data/demos/demo-carbonai/data/raw/PERSON_TREATMENT.csv', sep=';', encoding='ISO-8859-1')
# import s3fs
# path = 's3://ucb-qb-ca-eu-west-1-data/demos/demo-carbonai/data/raw/PERSON_TREATMENT.csv'
# file_system = s3fs.S3FileSystem()
# with file_system.open(path, mode='r', encoding='ISO-8859-1') as f:
#     df = pd.read_csv(f, sep=';', encoding='ISO-8859-1', low_memory=False)
#
# print(df.head())
# print(df.dtypes)
# sys.exit()

# Raw iris dataset, defined from a file
# CsvDataset(
#     'raw_persontreatment', description='Raw Person Treatment dataset', relpath='raw/PERSON_TREATMENT.csv',
#     read_kwargs= {'sep' : ';', 'encoding': 'ISO-8859-1'}) # , 'low_memory': 'False'

# def test(x):
#     return x
#
# ParquetDataset('cln_molecules2', description='Raw iris dataset', create=test, parents=['raw_molecules'])
#


# logger.info('DONE')
# CsvDataset(
#     'raw_molecules', description='EMR molecules classification', relpath='staging/01-raw/emr/molecules_classification.tsv')
#

# @ParquetDataset.from_parents('Cleaned person treatment dataset')
# def cln_persontreatment(raw_persontreatment):
#     new_col_names = {
#         name: name.lower().replace(' ', '_') for name in raw_persontreatment.columns}
#     result = raw_persontreatment.rename(columns=new_col_names)
#     return result
#
# @ParquetDataset.from_parents('Base person treatment dataset')
# def bse_persontreatment(cln_persontreatment):
#     return cln_persontreatment


# Generate and use datasets from data catalog
# ------------------------------------------------------------------------------
# Load a catalog containing all datasets defined so far
# data_catalog = get_catalog(
#     's3://ucb-qb-ca-eu-west-1-data/demos/demo-carbonai/data/')
# print(data_catalog.describe())
# print(data_catalog['raw_persontreatment'].path)
# df = data_catalog['raw_persontreatment'].read()
# print(df.head())
# print(data_catalog['bse_persontreatment'].path)
# print(data_catalog['cln_persontreatment'].path)
#
# if data_catalog['bse_persontreatment'].exists():
#     df1 = data_catalog['bse_persontreatment'].read()
#     # print(df1)
#     print('Test')

# data_catalog = get_catalog(
#     's3://ucb-qb-ca-eu-west-1-data/ca4i-fr-data/')
# data_catalog.describe()

# Generate all datasets using the pipeline
# The pipeline will not regenerate an existing file, unless
# its parents have a more recent modification time.
# pipeline = Pipeline(data_catalog)
# pipeline.run()

# Visualize the pipeline (requires graphviz and python-graphviz)
# pipeline.visualize('my_demo_pipeline.pdf')

# Read any of the datasets
# df_bse_person_treatment = data_catalog['bse_persontreatment'].read()
# df_cln_person_treatment = data_catalog['cln_persontreatment'].read()
# print(df_bse_person_treatment.count())
# print(df_cln_person_treatment.count())
