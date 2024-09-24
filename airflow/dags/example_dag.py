from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime


# Function to push values to XCom
def push_xcom(**kwargs):
    kwargs['ti'].xcom_push(key='test', value='Hello from XCom!')
    kwargs['ti'].xcom_push(key='test1', value='XCom is working')


# Function to pull values from XCom and print them
def pull_xcom(**kwargs):
    ti = kwargs['ti']
    test = ti.xcom_pull(key='test', task_ids='push_task')
    test1 = ti.xcom_pull(key='test1', task_ids='push_task')
    print(f"Value is 'test': {test}")
    print(f"Value for 'test1': {test1}")


default_args = {
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
}

with DAG('example_xcom_dag', default_args=default_args, catchup=False, schedule_interval='@daily') as dag:

    # Task to push values to XCom
    push_task = PythonOperator(
        task_id='push_task',
        provide_context=True,
        python_callable=push_xcom
    )

    # Task to pull values from XCom
    pull_task = PythonOperator(
        task_id='pull_task',
        provide_context=True,
        python_callable=pull_xcom
    )

    # Task to print XCom values using bash
    print_task = BashOperator(
        task_id='print_task',
        bash_command='echo "{{ ti.xcom_pull(task_ids=\'push_task\', key=\'test\') }}" and "{{ ti.xcom_pull(task_ids=\'push_task\', key=\'test1\') }}"'
    )

    # Defining the task dependencies
    push_task >> pull_task >> print_task
