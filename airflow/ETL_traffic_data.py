__author__ = "Miriela Velazquez"
__version__ = "1.1"

'''
Python script to define a DAG that performs ETL.
Data Acquisition and Management Course.
Final Project - Part A.
Data source: https://elasticbeanstalk-us-east-2-340729127361.s3.us-east-2.amazonaws.com/trafficdata.tgz
'''

# imports
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# file-related variables
data_dir = '/opt/airflow/dags/data'
archive_file = data_dir + '/trafficdata.tgz'
traffic_data_csv = data_dir + '/vehicle-data.csv'
traffic_data_tsv = data_dir + '/tollplaza-data.tsv'
traffic_data_fixed = data_dir + '/payment-data.txt'

temp_csv_data = data_dir + '/csv_d.csv'
temp_tsv_data = data_dir + '/tsv_d.csv'
temp_fixed_data = data_dir + '/fixed_width_d.csv'

extracted_data_file = data_dir + '/extracted_d.csv'
transformed_data_file = data_dir + '/transformed_d.csv'

# DAG arguments
default_args = {
    'owner': 'mvelazquez',
    'start_date': datetime(2022, 2, 12), # notice that static dates are recommended, dynamic shcedules such as now() or days_ago(0) is advised against
    'email': ['mvelazquez@analytics.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='etl_traffic_data',
    default_args = default_args,
    description = 'Airflow Project',
    schedule_interval = '@daily'
)

# Tasks definition
# task to unzip
unzip = BashOperator(task_id='unzip', bash_command='tar xvf %s -C %s' %(archive_file, data_dir), dag=dag)

# task to extract data from the csv file
# extract Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type --> columns 1, 2, 3, 4
extract_csv = BashOperator(task_id='extract_csv', bash_command='cut -d"," -f1-4 %s > %s' %(traffic_data_csv, temp_csv_data), dag=dag,)

# task to extract data from the tsv file
# extract Number of axles, Tollplaza id, and Tollplaza code --> columns 5, 6, 7
extract_tsv = BashOperator(task_id='extract_tsv', bash_command='cut -f5-7 %s | tr "\t" "," | sed "s/\\r//g" > %s' %(traffic_data_tsv, temp_tsv_data), dag=dag,)

# task to extract data from the fixed width file
# extract Type of Payment code, and Vehicle Code --> columns 6, 7
extract_fixed = BashOperator(task_id='extract_fixed', bash_command='tr -s " " < %s | rev |  cut -d" " -f1,2 | rev | tr " " "," > %s' %(traffic_data_fixed, temp_fixed_data), dag=dag,)

# task to combine all extracted data into a single file
combine_data = BashOperator(task_id='combine_data', bash_command='paste -d"," %s %s %s > %s' %(temp_csv_data, temp_tsv_data, temp_fixed_data, extracted_data_file), dag=dag,)

# task to transform the data
# transform the vehicle_type field in extracted_d.csv into capital letters --> column 4
transform_data = BashOperator( task_id='transform_data', bash_command='''awk 'BEGIN {FS=","; OFS=","}  $4=toupper($4)' %s >  %s''' %(extracted_data_file, transformed_data_file), dag=dag,)

# task pipeline
unzip >> [extract_csv, extract_tsv, extract_fixed] >> combine_data >> transform_data