from enum import Enum

class Raw(Enum):
    Authors = "ca4i-fr-data/airflow/mvp/raw/authors.csv"
    Investigators = "ca4i-fr-data/airflow/mvp/raw/investigators.csv"

class Cleaned(Enum):
    Authors = "ca4i-fr-data/airflow/mvp/clean/authors.csv"
    Investigators = "ca4i-fr-data/airflow/mvp/clean/investigators.csv"

class Master(Enum):
    Matched = "ca4i-fr-data/airflow/mvp/master/matched.csv"
