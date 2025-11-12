"""
Olist ELT Pipeline DAG

This DAG orchestrates the complete ELT (Extract, Load, Transform) pipeline
for the Olist e-commerce dataset using Apache Airflow.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import SQLITE_BD_ABSOLUTE_PATH
from src.extract import extract
from src.load import load
from src.transform import run_queries

# Default arguments for the DAG
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
def extract_task():
    """Task 1: Extract data from CSV files and public holidays API"""
    print("Starting extraction task...")
    
    # Import config values
    from src.config import DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL, get_csv_to_table_mapping
    
    # Call extract with required parameters
    data_frames = extract(
        csv_folder=DATASET_ROOT_PATH,
        csv_table_mapping=get_csv_to_table_mapping(),
        public_holidays_url=PUBLIC_HOLIDAYS_URL
    )
    
    print(f"Extracted {len(data_frames)} datasets successfully")
    return data_frames


def load_task(**context):
    """Task 2: Load extracted data into SQLite database"""
    print("Starting load task...")
    
    # Retrieve data from previous task using XCom
    ti = context["ti"]
    data_frames = ti.xcom_pull(task_ids="extract")
    
    if not data_frames:
        raise ValueError("No data received from extract task")
    
    # Create database engine
    engine = create_engine(f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}")
    
    # Load data into database
    load(data_frames, engine)
    
    print(f"Loaded {len(data_frames)} datasets into database successfully")


def transform_task():
    """Task 3: Execute SQL queries and generate analytical results"""
    print("Starting transform task...")
    
    # Create database engine
    engine = create_engine(f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}")
    
    # Run all queries
    query_results = run_queries(engine)
    
    print(f"Executed {len(query_results)} queries successfully")
    return query_results

# Define the DAG
with DAG(
    dag_id="olist_etl_pipeline",
    default_args=default_args,
    description="ELT pipeline for Olist e-commerce data",
    schedule_interval="@daily",  # Run daily at midnight
    start_date=datetime(2025, 11, 1),
    catchup=False,  # Don't run for past dates
    tags=["olist", "etl", "analytics"],
) as dag:
    
    # Task 1: Extract data
    extract_op = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )
    
    # Task 2: Load data into database
    load_op = PythonOperator(
        task_id="load",
        python_callable=load_task,
        provide_context=True,
    )
    
    # Task 3: Transform data (run queries)
    transform_op = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )
    
    # Define task dependencies: Extract → Load → Transform
    extract_op >> load_op >> transform_op