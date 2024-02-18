from sqlalchemy import create_engine
import psycopg2
from dotenv import dotenv_values
dotenv_values()


config = dict(dotenv_values('.env'))

def get_redshift_connection(config):
    iam_role = config.get('IAM_ROLE')
    user = config.get('USER')
    password = config.get('PASSWORD')
    host = config.get('HOST')
    database_name = config.get('DATABASE_NAME')
    port = config.get('PORT')
    # create redshift connection usingf'dbname={database_name} host={host} port={port} user={user} password={password}')
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    return conn


def get_database_conn():
    config = dict(dotenv_values('.env'))
    db_user_name = config.get('DB_USER_NAME')
    db_password = config.get('DB_PASSWORD')
    host = config.get('HOST')
    port = config.get('PORT')
    db_name = config.get('DB_NAME')
    return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')
