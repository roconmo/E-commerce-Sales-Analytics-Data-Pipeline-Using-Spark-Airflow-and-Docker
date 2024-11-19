from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths to files
csv_path = '/opt/airflow/dags/data/ecommerce_sales_data.csv'
extracted_path = '/opt/airflow/dags/data/extracted_data.pkl'
transformed_path = '/opt/airflow/dags/data/transformed_data.pkl'
loaded_path = '/opt/airflow/dags/data/loaded_data.csv'

###################################



def copy_file_from_container(container_name, container_path, local_path):
    try:
        # Construct the docker cp command
        command = ["docker", "cp", f"{container_name}:{container_path}", local_path]
        # Run the command using subprocess
        subprocess.run(command, check=True)
        print(f"Successfully copied {container_path} from {container_name} to {local_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while copying file: {e}")

# Parameters
container_name = "airflow-webserver"  # Replace with your container name or ID
container_path = "/opt/airflow/dags/data/loaded_data.csv"  # The path inside the container
local_path = "./loaded_data.csv"  # The path on your local machine

# Execute the function
copy_file_from_container(container_name, container_path, local_path)

###################################


# Extract Function
def extract_data():
    try:
        df = pd.read_csv(csv_path)
        df.to_pickle(extracted_path)
        logger.info("Extracted data successfully saved.")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")


# Transform Function
def transform_data():
    try:
        df = pd.read_pickle(extracted_path)

        # Handling missing data
        df.fillna({'quantity': 1, 'price_per_unit': 0}, inplace=True)

        # Adding new columns
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['year'] = df['order_date'].dt.year
        df['month'] = df['order_date'].dt.month

        # Extracting state from shipping address
        df['state'] = df['shipping_address'].apply(lambda x: x.split(',')[-1].strip().split(' ')[0])

        # Normalizing product categories
        df['category'] = df['category'].str.lower()

        df.to_pickle(transformed_path)
        logger.info("Transformed data successfully saved.")
    except Exception as e:
        logger.error(f"Error occurred during data transformation: {e}")


# Load Function
def load_data():
    try:
        df = pd.read_pickle(transformed_path)
        df.to_csv(loaded_path, index=False)
        logger.info("Loaded data successfully saved.")
    except Exception as e:
        logger.error(f"Error occurred during data loading: {e}")


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
