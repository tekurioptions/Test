from enum import Enum

class Source(Enum):
    Authors = "/home/kumar.tekurinagendra/airflow_files/input/authors1.csv"
    Investigators = "/home/kumar.tekurinagendra/airflow_files/input/investigators1.csv"

class Raw(Enum):
    Authors = "ca4i-fr-data/airflow/mvp/raw/authors.csv"
    Investigators = "ca4i-fr-data/airflow/mvp/raw/investigators.csv"

class Cleaned(Enum):
    Authors = "ca4i-fr-data/airflow/mvp/clean/authors.csv"
    Investigators = "ca4i-fr-data/airflow/mvp/clean/investigators.csv"

class Master(Enum):
    Matched = "ca4i-fr-data/airflow/mvp/master/matched.csv"

class Common(Enum):
    Bucket = "ucb-qb-ca-eu-west-1-data"
