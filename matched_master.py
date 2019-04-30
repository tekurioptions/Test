import pandas as pd
from boto3_helper import read_csv_from_s3, save_df_as_csv_to_s3

author_columns = ['author_id', 'first_name', 'middle_name', 'lastname', 'coauthor_lastnames',
                      'topics', 'cities', 'countries', 'orcid']
investigator_columns = ['investigator_id', 'first_name', 'middle_name', 'lastname', 'coinvestigator_lastnames',
                        'topics', 'cities', 'countries', 'orcid']
output_columns = ['author_id', 'investigator_id']

def get_matched_by_orc_id(authors, investigators):
    print('Test3')
    result = pd.merge(authors, investigators, on='orcid')
    print('Test4')
    result = result[output_columns]
    print('Test5')
    return result

def join_authors_investigators(bucket_name, authors_clean_key, investigators_clean_key):
    print('Test1')
    df1 = read_csv_from_s3(bucket_name, authors_clean_key, author_columns)
    print('Test2')
    df2 = read_csv_from_s3(bucket_name, investigators_clean_key, investigator_columns)
    # return get_matched_by_orc_id(df1[(df1['orcid'].notnull()) & (df1['orcid'] != '')],
    #                              df2[(df2['orcid'].notnull()) & (df2['orcid'] != '')])
    return get_matched_by_orc_id(df1, df2)

def create_master_matched(bucket_name, authors_clean_key, investigators_clean_key, output_key):
    output = join_authors_investigators(bucket_name, authors_clean_key, investigators_clean_key)
    save_df_as_csv_to_s3(output, bucket_name, output_key)
    return output