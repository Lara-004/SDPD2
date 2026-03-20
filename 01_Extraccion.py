import json
import pathlib
import csv
#import pendulum
import requests
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.providers.standard.operators.python import PythonOperator
#from airflow.sdk import DAG
from requests.exceptions import ConnectionError, MissingSchema

def get_df():
    with open('reviews.csv', newline='') as csvfile:
        df = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in df:
            