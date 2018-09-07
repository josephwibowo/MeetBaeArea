import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from meetup.models import Base, BaseProd, Transaction
from operators.meetup_to_postgres_operator import MeetupToPostgresOperator

class SQLTemplatedPythonOperator(PythonOperator):
    # Allows sql files to be found
    template_ext = ('.sql',)

def db_init():
    engine.execute('CREATE SCHEMA IF NOT EXISTS source;')
    engine.execute('CREATE SCHEMA IF NOT EXISTS staging;')
    engine.execute('CREATE SCHEMA IF NOT EXISTS prod;')
    Base.metadata.create_all(engine)
    BaseProd.metadata.create_all(engine)
    # Create dim date table if not already created
    if not engine.execute('''SELECT to_regclass('prod.dim_date')''').first()[0]:
        fd = open('./dags/sql/create_date_dim.sql', 'r')
        sqlfile = fd.read()
        fd.close()
        engine.execute(sqlfile)

def stage_data(**context):
    session = Session()
    # Get last job date
    last_job_date = session.execute(context['templates_dict']['query']).first()[0]
    # Stage
    sql_commands = context['templates_dict']['load'].split(';')
    for command in sql_commands:
        if command:
            session.execute(command.format(last_job_date))
            session.commit()
    session.close()

def load_data(**kwargs):
    exec_date = kwargs['execution_date']
    session = Session()
    # Stage
    fd = open('./dags/sql/load_data.sql', 'r')
    sqlfile = fd.read()
    fd.close()
    sql_commands = sqlfile.split(';')
    try:
        for command in sql_commands:
            if command:
                session.execute(command)
        t = Transaction(loaded_until=exec_date, status='Success')
        session.add(t)
        session.commit()
    except:
        t = Transaction(loaded_until=exec_date, status='Failed')
        session.execute(t)
        session.commit()
    session.close()

# Get Engine Settings
def get_engine(conn_id):
    connection = BaseHook.get_connection(conn_id)
    connection_uri = '{c.conn_type}+psycopg2://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}'.format(c=connection)
    return create_engine(connection_uri)
engine = get_engine('meetup')
Session = sessionmaker(bind=engine)

default_args = {
    'owner': 'joseph.wibowo',
    'start_date': datetime.datetime(2008, 1, 1),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False
}

dag = DAG('meetup_to_postgres',
          default_args=default_args,
          schedule_interval='@daily',
          template_searchpath=['dags/sql'],
          max_active_runs=1,
          catchup=True
         )

initialize_db = PythonOperator(task_id='initialize_db',
                               python_callable=db_init,
                               dag=dag)

start = '{{(execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00.000")}}'
end = '{{execution_date.strftime("%Y-%m-%dT00:00:00.000")}}'
call_meetup_api = MeetupToPostgresOperator(task_id='call_meetup_api',
                                           engine=engine,
                                           start=start,
                                           end=end,
                                           dag=dag)

stage_the_data = SQLTemplatedPythonOperator(task_id='stage_data',
                                            templates_dict={'query': 'get_last_job_date.sql', 'load': 'staging.sql'},
                                            python_callable=stage_data,
                                            provide_context=True,
                                            dag=dag)

load_the_data = SQLTemplatedPythonOperator(task_id='load_data',
                               python_callable=load_data,
                               provide_context=True,
                               dag=dag)

initialize_db >> call_meetup_api >> stage_the_data >> load_the_data