import pandas as pd
from airflow_core.airflow_helper import read_csv_from_s3

author_columns = ['author_id', 'first_name', 'middle_name', 'lastname', 'coauthor_lastnames',
                      'topics', 'cities', 'countries', 'orcid']
investigator_columns = ['investigator_id', 'first_name', 'middle_name', 'lastname', 'coinvestigator_lastnames',
                        'topics', 'cities', 'countries', 'orcid']
output_columns = ['author_id', 'investigator_id']

def get_matched_by_orc_id(authors, investigators):
    result = pd.merge(authors, investigators, on='orcid')
    result = result[output_columns]
    return result

def read_files_from_s3(bucket_name, key1, key2):
    df1 = read_csv_from_s3(bucket_name, key1, author_columns)
    df2 = read_csv_from_s3(bucket_name, key2, investigator_columns)
    return get_matched_by_orc_id(df1[(df1['orcid'].notnull()) & (df1['orcid'] != '')],
                                 df2[(df2['orcid'].notnull()) & (df2['orcid'] != '')])