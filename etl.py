from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import requests
from util import get_database_conn



def extract_data():
    url = 'https://afx.kwayisi.org/ngx/'
    get_url = requests.get(url)
    url_content = get_url.content 
    soup = BeautifulSoup(url_content, 'lxml')
    table_soup = str(soup.find_all('table')[3])
    pd_table = pd.read_html(table_soup)[0]
    #print(pd_table)

    url1 = 'https://afx.kwayisi.org/ngx/?page=2'
    get_url1 = requests.get(url1)
    url1_content = get_url1.content 
    soup1 = BeautifulSoup(url1_content, 'lxml')
    table_soup1 = str(soup1.find_all('table')[3])
    pd_table1 = pd.read_html(table_soup1)[0]
    #print(pd_table1)
    new_table = pd.concat([pd_table,pd_table1], axis=0)
    #print(new_table)
    new_table.to_csv('combined.csv', index=False)
    print('Table successfully concatenated and created')

extract_data()


def transform_data():
    file = pd.read_csv('combined.csv')
    file['Date'] = datetime.now().date()
    mean_volume = file['Volume'].mean()
    file['Volume'] = file['Volume'].fillna(mean_volume)
    file.to_csv('tranformed.csv', index=False)
    print ('File transformed successfully')

transform_data()

def load_data ():
    file = pd.read_csv('tranformed.csv')
    engine = get_database_conn ()
    file.to_sql('stockcompany', con=engine, if_exists='append', index=False)
    print ('file successfully written to Database')

load_data()
