import datetime as dt
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'LEONID',
    'start_date': dt.datetime(2024, 2, 25),
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
    'retries': 1,
}


def format_message(**kwargs):
    flat_message = kwargs['ti'].xcom_pull(task_ids='show_avg_fare', key='return_value')
    formatted = flat_message.replace(';', '\n')
    print(format_message)
    kwargs['ti'].xcom_push(key='telegram_message', value=formatted)


def prepare_namefile(ti):
    file_name = 'titanic-' + str(int(datetime.now().timestamp())) + '.csv'
    ti.xcom_push(key='file_name', value=file_name)


with DAG(
        dag_id='titanic_to_hdfs_to_hive_to_telegram',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    with TaskGroup(group_id='prepare_data', prefix_group_id=False) as prepare_data:
        prepare_file_name = PythonOperator(
            task_id='prepare_file_name',
            python_callable=prepare_namefile,
        )

        download_titanic_dataset = BashOperator(
            task_id='download_titanic_dataset',
            bash_command=''' wget -q https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv -O {{ ti.xcom_pull(task_ids=\'prepare_file_name\', key=\'file_name\') }}  && \
            mv `realpath {{ ti.xcom_pull(task_ids=\'prepare_file_name\', key=\'file_name\') }}` /home/hduser/ ''',
        )

        dataset_to_hdfs = BashOperator(
            task_id='dataset_to_hdfs',
            bash_command=''' hdfs dfs -mkdir -p /datasets/ && \
            hdfs dfs -put ~/{{ ti.xcom_pull(task_ids=\'prepare_file_name\', key=\'file_name\') }} /datasets/ && \
            rm ~/{{ ti.xcom_pull(task_ids=\'prepare_file_name\', key=\'file_name\') }} ''',
        )

        prepare_file_name >> download_titanic_dataset >> dataset_to_hdfs

    with TaskGroup("prepare_table") as prepare_table:
        drop_hivi_table_managed = HiveOperator(
            task_id='drop_hivi_table_managed',
            hql='DROP TABLE titanic_data;',
        )

        create_hive_table_managed = HiveOperator(
            task_id='create_hive_table_managed',
            hql='''CREATE TABLE IF NOT EXISTS titanic_data(Survived INT,Pclass INT,Name STRING,Sex STRING,Age INT,SibSp INT,ParCh INT,Fare DOUBLE)
            ROW FORMAT DELIMITED
            fields terminated by ','
            STORED AS TEXTFILE
            location 'hdfs://localhost:9000/datasets/'
            tblproperties("skip.header.line.count"="1");''',
        )

        drop_hivi_table_managed >> create_hive_table_managed

    with TaskGroup("prepare_table_part") as prepare_table_part:
        drop_hivi_table_part = HiveOperator(
            task_id='drop_hivi_table_part',
            hql='DROP TABLE IF EXISTS titanic_data_part;',
        )

        create_hive_table_part = HiveOperator(
            task_id='create_hive_table_part',
            hql='''CREATE TABLE IF NOT EXISTS titanic_data_part(Survived INT,Name STRING,Sex STRING,Age INT,SibSp INT,ParCh INT,Fare DOUBLE)
            partitioned by (Pclass INT)
            stored as TEXTFILE;''',
        )

        load_titanic_data_part = HiveOperator(
            task_id='load_titanic_data_part',
            hql='''set hive.exec.dynamic.partition.mode=nonstrict;
            INSERT OVERWRITE TABLE titanic_data_part PARTITION (`Pclass`)
            SELECT Survived, Name, Sex, Age, SibSp, ParCh, Fare, Pclass
            FROM titanic_data;''',
        )

        drop_hivi_table_managed = HiveOperator(
            task_id='drop_hivi_table_managed',
            hql='DROP TABLE titanic_data;',
        )

        drop_hivi_table_part >> create_hive_table_part >> load_titanic_data_part >> drop_hivi_table_managed

    show_avg_fare = BashOperator(
        task_id='show_avg_fare',
        bash_command=''' beeline -u jdbc:hive2://localhost:10000 -e "SELECT Pclass, avg(Fare) FROM titanic_data_part GROUP BY Pclass" \
         | tr "\n" ";" ''',
    )

    prepare_message = PythonOperator(
        task_id='prepare_message',
        python_callable=format_message,
    )

    send_result_telegram = TelegramOperator(
        task_id='send_result_telegram',
        telegram_conn_id='telegram_conn_id',
        chat_id='417953408',
        text='''Pipeline {{execution_date.int_timestamp}} is done. Result:
        {{ ti.xcom_pull(task_ids='prepare_message', key='telegram_message')}}''',
    )

    prepare_data >> prepare_table >> prepare_table_part >> show_avg_fare

    show_avg_fare >> prepare_message >> send_result_telegram
