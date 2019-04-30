import pandas as pd
from sqlalchemy import create_engine

def create_engine_psql():
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres" , echo=False)
    return engine

def insert_to_db(engine, df, table_name):
    df.to_sql(table_name, con=engine, if_exists='append')