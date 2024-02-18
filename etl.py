import pandas as pd 
import requests
import ast
import boto3 
from io import StringIO
import io
import json
from datetime import datetime
from include.util import get_redshift_connection
from dotenv import dotenv_values
dotenv_values()


s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

bucket_name = 'ken39193'
path = 'raw'


config = dotenv_values()



def extract_data():
    url = config.get('url')
    headers = ast.literal_eval(config.get('headers'))
    querystring = ast.literal_eval(config.get('querystring'))
    response = requests.get(url, headers=headers, params=querystring).json()
    raw_job_data = response.get('data')   
    filtered_jobs_data = [
        job for job in raw_job_data if (
            job.get('job_job_title') in ['Data engineer', 'Data analyst'] and 
            job.get('job_country') in ['GB']
        )
    ]   
    print('data extracted from API')
    return filtered_jobs_data
    
df = extract_data()
Job_dataframe = pd.DataFrame(df)

def write_to_s3(Job_dataframe):
    csv_buffer = Job_dataframe.to_csv(index=False)
    file_name = f"filtered_job_data_{datetime.now().strftime('%Y%m%d')}.csv"
    s3_client.put_object(Bucket=bucket_name, Key=f'{path}/{file_name}', Body=csv_buffer) 
    print('File successfully written to S3 bucket')

write_to_s3(Job_dataframe)

def read_from_s3():
    objects_list = s3_client.list_objects(Bucket=bucket_name, Prefix=path)

    if 'Contents' not in objects_list:
        print('No objects found in S3 bucket.')
        return None

    file = objects_list['Contents'][1]
    key = file.get('Key')
    print(f"S3 Object Key: {key}")

    obj = s3_client.get_object(Bucket=bucket_name, Key=key)

    if obj['ContentLength'] == 0:
        print('The file in S3 is empty.')
        return None

    try:
        # Attempt to read the CSV file into a DataFrame
        data = pd.read_csv(io.BytesIO(obj['Body'].read()))
        print('Data successfully read from S3:')
        # print(data)
        return data
    except pd.errors.EmptyDataError:
        print('The CSV file is empty.')
        return None

data_from_s3 = read_from_s3()


def transform_raw_job_data():
    rata = pd.DataFrame(data_from_s3)
    rata = rata[["employer_name", "employer_website", "job_id", "job_employment_type", 
                 "job_title", "job_apply_link", "job_city", "job_posted_at_timestamp", 
                 "job_country", "employer_company_type"]]
    rata.drop_duplicates(inplace=True)
    rata['job_posted_at_timestamp'] = pd.to_datetime(rata['job_posted_at_timestamp'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    rata.fillna(value={'job_employment_type': 'Unknown', 'job_city': 'Unknown', 'job_country': 'Unknown'}, inplace=True)
    rata['job_title'] = rata['job_title'].str.lower()
    print('transformations done')
    return rata

transformed_data = transform_raw_job_data()

def write_transformed_data_to_s3(transformed_data):
    csv_buffer = StringIO()
    transformed_data.to_csv(csv_buffer, index=False)
    file_name = "transformed_data.csv"
    s3_client.put_object(Bucket=bucket_name, Key=f'transformed/{file_name}', Body=csv_buffer.getvalue())
    print('Transformed file successfully written to S3 bucket')

write_transformed_data_to_s3(transformed_data)

def generate_schema(transformed_data, table_name='chuka1', exclude_columns=None):
    if exclude_columns is None:
        exclude_columns = []

    create_table_statement = f'CREATE TABLE IF NOT EXISTS {table_name}(\n'
    column_type_query = ''

    types_checker = {
        'INT': pd.api.types.is_integer_dtype,
        'VARCHAR': pd.api.types.is_string_dtype,
        'FLOAT': pd.api.types.is_float_dtype,
        'TIMESTAMP': pd.api.types.is_datetime64_any_dtype,
    }

    varchar_lengths = {
        'job_city': 255,  # Set the desired length for job_city
    }

    for column in transformed_data.columns:
        if column in exclude_columns:
            continue  # Skip columns in the exclusion list

        last_column = transformed_data.columns[-1]
        mapped = False

        for type_ in types_checker:
            if types_checker[type_](transformed_data[column]):
                mapped = True
                if type_ == 'VARCHAR' and column in varchar_lengths:
                    length = varchar_lengths[column]
                    column_type_query += f'"{column}" {type_}({length}),\n'
                elif column != last_column:
                    column_type_query += f'"{column}" {type_},\n'
                else:
                    column_type_query += f'"{column}" {type_}\n'
                break

        if not mapped:
            # If type not found, use a fallback type (in this case, VARCHAR)
            column_type_query += f'"{column}" VARCHAR,\n'

    # Remove the trailing comma and newline from the last line
    column_type_query = column_type_query.rstrip(',\n')
    column_type_query += ');'

    output_query = create_table_statement + column_type_query
    return output_query

# Example usage with exclusion list
exclude_columns = ['extra_column1', 'extra_column2']
generate_schema(transformed_data, table_name='chuka1', exclude_columns=exclude_columns)





def create_table_from_schema(table_name='chuka1'):
    conn = get_redshift_connection(config)
    schema_query = generate_schema(transformed_data, table_name)

    try:
        with conn.cursor() as cur:
            cur.execute(schema_query)
            conn.commit()
        print(f'Table {table_name} created successfully')
    except Exception as e:
        conn.rollback()
        print(f"Error creating table: {e}")
    finally:
        conn.close()

create_table_from_schema()


def load_to_redshift(table_name='chuka1'):
    s3_path = 's3://ken39193/transformed/transformed_data.csv'
    iam_role = config.get('IAM_ROLE')
    conn = get_redshift_connection(config)

    try:
        with conn.cursor() as cur:
            # Corrected copy query
            copy_query = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            CSV
            IGNOREHEADER 1;
            """
            cur.execute(copy_query)
            conn.commit()
        print('Data successfully loaded to Redshift')
    except Exception as e:
        conn.rollback()
        print(f"Error loading data to Redshift: {e}")
    finally:
        conn.close()

load_to_redshift(table_name='chuka1')




