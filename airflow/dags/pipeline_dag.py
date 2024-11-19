from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Path to the CSV file
csv_path = '/opt/airflow/dags/data/your_data.csv'

# Extract Function
def extract_data():
    df = pd.read_csv(csv_path)
    df.to_pickle('/opt/airflow/dags/data/extracted_data.pkl')  # Save the extracted data for next steps

# Transform Function
def transform_data():
    df = pd.read_pickle('/opt/airflow/dags/data/extracted_data.pkl')

    # Example Transformations:
    # 1. Handling missing data
    df.fillna({'quantity': 1, 'price_per_unit': 0}, inplace=True)

    # 2. Adding new columns
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year'] = df['order_date'].dt.year
    df['month'] = df['order_date'].dt.month

    # 3. Extracting state from shipping address
    df['state'] = df['shipping_address'].apply(lambda x: x.split(',')[-1].strip().split(' ')[0])

    # 4. Normalizing product categories
    df['category'] = df['category'].str.lower()

    df.to_pickle('/opt/airflow/dags/data/transformed_data.pkl')  # Save the transformed data

# Load Function
def load_data():
    df = pd.read_pickle('/opt/airflow/dags/data/transformed_data.pkl')

    # Example Load: Save the transformed data to a new CSV file
    df.to_csv('/opt/airflow/dags/data/loaded_data.csv', index=False)

# Define DAG
with DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Extract data
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Task 3: Load data
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task
