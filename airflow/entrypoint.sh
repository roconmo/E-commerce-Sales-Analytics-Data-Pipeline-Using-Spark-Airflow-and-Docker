#!/bin/bash
# Initialize the database
airflow db init

# Start the Airflow webserver
exec airflow webserver
