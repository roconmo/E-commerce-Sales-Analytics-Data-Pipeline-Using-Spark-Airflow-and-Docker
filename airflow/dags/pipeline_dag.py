from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import logging

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
csv_path = '/opt/airflow/dags/data/bad_ecommerce_sales_data.csv'
extracted_path = '/opt/airflow/dags/data/extracted_data.pkl'
transformed_path = '/opt/airflow/dags/data/transformed_data.pkl'
loaded_path = '/opt/airflow/dags/data/loaded_data.csv'


# Extract Function
def extract_data():
    try:
        df = pd.read_csv(csv_path)
        df.to_pickle(extracted_path)
        logger.info("Extracted data successfully saved.")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")


# Individual Transformation Functions
def handle_missing_data():
    try:
        df = pd.read_pickle(extracted_path)
        df.fillna({'quantity': 1, 'price_per_unit': 0}, inplace=True)
        df.to_pickle(extracted_path)  # Save intermediate step
        logger.info("Handled missing data successfully.")
    except Exception as e:
        logger.error(f"Error occurred while handling missing data: {e}")


def extract_date_features():
    try:
        df = pd.read_pickle(extracted_path)
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df['year'] = df['order_date'].dt.year
        df['month'] = df['order_date'].dt.month
        df.to_pickle(extracted_path)  # Save intermediate step
        logger.info("Extracted date features successfully.")
    except Exception as e:
        logger.error(f"Error occurred while extracting date features: {e}")


def normalize_product_categories():
    try:
        df = pd.read_pickle(extracted_path)
        df['category'] = df['category'].str.lower()
        df.to_pickle(extracted_path)  # Save intermediate step
        logger.info("Normalized product categories successfully.")
    except Exception as e:
        logger.error(f"Error occurred while normalizing product categories: {e}")


def extract_state_from_address():
    try:
        df = pd.read_pickle(extracted_path)
        df['state'] = df['shipping_address'].apply(
            lambda x: x.split(',')[-1].strip().split(' ')[0] if pd.notna(x) else 'Unknown')
        df.to_pickle(transformed_path)  # Save transformed data
        logger.info("Extracted state from shipping address successfully.")
    except Exception as e:
        logger.error(f"Error occurred while extracting state from shipping address: {e}")


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

    # Task Group: Transformations
    with TaskGroup('data_transformation') as data_transformation:
        # Task 2.1: Handle Missing Data
        handle_missing_data_task = PythonOperator(
            task_id='handle_missing_data',
            python_callable=handle_missing_data,
        )

        # Task 2.2: Extract Date Features
        extract_date_features_task = PythonOperator(
            task_id='extract_date_features',
            python_callable=extract_date_features,
        )

        # Task 2.3: Normalize Product Categories
        normalize_product_categories_task = PythonOperator(
            task_id='normalize_product_categories',
            python_callable=normalize_product_categories,
        )

        # Task 2.4: Extract State from Address
        extract_state_from_address_task = PythonOperator(
            task_id='extract_state_from_address',
            python_callable=extract_state_from_address,
        )

        # Set dependencies within the transformations TaskGroup
        handle_missing_data_task >> extract_date_features_task >> normalize_product_categories_task >> extract_state_from_address_task

    # Task 3: Load data
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Set task dependencies
    extract_task >> data_transformation >> load_task
