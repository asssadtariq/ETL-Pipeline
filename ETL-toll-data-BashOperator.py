from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime as dt

default_args = {
    "owner": "me",
    "start_date": dt.datetime.today(),
    "retries": 1,
    "retry_delay": dt.datetime(minutes=5),
    "email": ["asadtariq.at66@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

dag = DAG(
    dag_id="ETL_toll_data",
    description="Apache Airflow Final Assignment",
    schedule=dt.timedelta(days=1),
    default_args=default_args,
)

# task 1 - unzip the file
unzip_files = BashOperator(
    task_id="unzip_files",
    bash_command="tar -xvf /home/project/airflow/dags/finalassignment/tolldata.tgz",
    dag=dag,
)

# task 2 - extract from csv
extract_from_csv = BashOperator(
    task_id="extract_from_csv",
    bash_command="cat /home/project/airflow/dags/finalassignment/vehicle-data.csv | cut -d ',' -f1-4 > /home/project/airflow/dags/finalassignment/csv_data.csv",
    dag=dag,
)

# task 3 - extract from tsv
extract_from_tsv = BashOperator(
    task_id="extract_from_tsv",
    bash_command="cat /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | cut -d $'\t' -f5-7 | tr $'\t' ',' > /home/project/airflow/dags/finalassignment/tsv_data.csv",
    dag=dag,
)

# task 4 - extract_data_from_fixed_width
extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command="cat /home/project/airflow/dags/finalassignment/payment-data.txt | cut -c 59-68 | tr ' ' ',' > /home/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag=dag,
)

# task 5 - consolidate_data
base_path = "/home/project/airflow/dags/finalassignment"
consolidate_data = BashOperator(
    task_id="combine_data",
    bash_command=f"paste -d ',' {base_path}/csv_data.csv {base_path}/tsv_data.csv {base_path}/fixed_width_data.csv > {base_path}/extracted_data.csv ",
    dag=dag,
)

# task 6 - transform data
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="awk -F',' '{ $4 = toupper($4); print }'/home/project/airflow/dags/finalassignment/extracted_data.csv > transformed_datas.csv",
    dag=dag,
)

# task 7 - tasks definations
(
    unzip_files
    >> extract_from_csv
    >> extract_from_tsv
    >> extract_data_from_fixed_width
    >> consolidate_data
    >> transform_data
)
