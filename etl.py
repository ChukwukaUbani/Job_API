import pandas as pd 
from util import get_database_conn
from datetime import datetime 


def extract_data():
    data = pd.read_csv('covid_19_data.csv')
    print('Covid_19 data extracted sucessfully')

def transform_data():
    data = pd.read_csv('covid_19_data.csv')
    data = data.rename(columns={'ObservationDate':'date'})
    data['date'] = pd.to_datetime(data['date'])
    data.columns = data.columns.str.lower()
    data.to_csv('transformed.csv', index=False)
    print('data successfully transformed')

def load_data():
    data = pd.read_csv('transformed.csv')
    engine = get_database_conn()
    data.to_sql('health', con=engine, if_exists='replace', index=False)
    print('Covid_19 data loaded successfully')