import pandas as pd
from boto3.boto3_helper import read_csv_from_s3, save_df_as_csv_to_s3

author_columns = ['author_id', 'first_name', 'middle_name', 'lastname', 'coauthor_lastnames',
                      'topics', 'cities', 'countries', 'orcid']
investigator_columns = ['investigator_id', 'first_name', 'middle_name', 'lastname', 'coinvestigator_lastnames',
                        'topics', 'cities', 'countries', 'orcid']
def clean_authors(bucket_name, authors_raw_key, authors_clean_key):
    df_authors = read_csv_from_s3(bucket_name, authors_raw_key, author_columns)
    df_authors = df_authors.fillna('')
    save_df_as_csv_to_s3(df_authors, bucket_name, authors_clean_key)

def clean_investigators(bucket_name, investigators_raw_key, investigators_clean_key):
    df_investigators = read_csv_from_s3(bucket_name, investigators_raw_key, author_columns)
    df_investigators = df_investigators.fillna('')
    save_df_as_csv_to_s3(df_investigators, bucket_name, investigators_clean_key)