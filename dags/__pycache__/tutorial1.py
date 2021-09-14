# first step
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

# second step
default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'email': ['vlasbelyaev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)

}
# third step
dag = DAG(
    'tutorial1',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1)
)

# fourth step
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)
t1.doc_md = """### TASK Documentation"""
dag.doc_md = __doc__

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag
)
# language=BASH
# templated_command = """
# {% for i in range(5) %}
#     echo "{{ params.my_param }}"
# {% endfor %}
# """

# t3 = BashOperator(
#     task_id='templated',
#     depends_on_past=False,
#     bash_command=templated_command,
#     params={'my_param', 'I passed a param'},
#     dag=dag
# )

def someCallable(ds, **kwargs):
    print(kwargs)
    print(ds)
    return "This message will be printed into console logs"

t3 = PythonOperator(
    task_id = "python",
    python_callable=someCallable,
    dag=dag
)

SNOWFLAKE_SCHEMA = 'public'
SNOWFLAKE_ROLE = 'SYSADMIN'
SNOWFLAKE_WAREHOUSE = 'TEST_WH'
SNOWFLAKE_CONN_ID = 'snowflake'
SQL_SELECT_STATEMENT = 'SELECT * FROM BILLING WHERE SHOP_ID = 1 LIMIT 10'
SNOWFLAKE_DATABASE = 'test_db'
t4 = SnowflakeOperator(
    task_id='snowflake_op_with_params',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_SELECT_STATEMENT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)

# t2 will depend on t1
# running successfully to run
# t1.set_downstream(t2)
# t1 >> t2 # similar

t1 >> [t2, t3]

t4 >> t3

# similar to above where t3 will depend on t1
# t3.set_downstream(t1)
# t3 >> t1 # similar
