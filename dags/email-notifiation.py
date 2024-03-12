
'''
 # Code to run the Salesforce Sales Volume Task
 # Importing required libraries
'''

from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.email import send_email
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow_master',
    'depends_on_past': False,
    'start_date': datetime(2023,7,4),
    'email': ['abc.def@xyz.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Number of retries
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'email_to': ['abc.def@xyz.com'],  # Add the 'to' email address here
    'email_cc': ['abc.def@xyz.com'],  # Add the 'cc' email address here
}

def send_status_email(context):
    task_status = context['task_instance'].current_state()

    subject = f"Airflow Task- {context['task_instance'].task_id}: {task_status}"
    body = f"<html> <head> <body><br>  Hi App Support Team <br> The task <b> {context['task_instance'].task_id} </b> finished with status: <b>{task_status}</b> <br><br> Task execution date: {context['execution_date']} <br> <p>Log URL: {context['task_instance'].log_url} <br> Regards<br> Dev Team </p></body> </head> </html>"
    
    to_email = ["abc.def@xyz.com"]  # Replace with the primary recipient email address
    cc_email = ["abc.def@xyz.com"]  # Replace with the CC recipient email address

    send_email(to=to_email, cc=cc_email, subject=subject, html_content=body)

def send_failure_status_email(context):
    #task_status = context['task_instance'].current_state()
    task_instance = context['task_instance']
    task_status = task_instance.current_state()
    
    subject = f"Airflow Task- {context['task_instance'].task_id}: {task_status}"
    body = f"<html> <head> <body><br>  Hi App Support Team <br> The task <b> {context['task_instance'].task_id} </b> finished with status: <b>{task_status}</b> <br><br> Task execution date: {context['execution_date']} <br> <p>Log URL: {context['task_instance'].log_url}<br><br> NOTE: Kindly resolve the error so as the UPSTREAM tasks can run, until then it's blocked. <br><br> Regards<br> Dev Team</p></body> </head> </html>"
    
    to_email = ["abc.def@xyz.com"]  # Replace with the primary recipient email address
    cc_email = ["abc.def@xyz.com"]  # Replace with the CC recipient email address

    send_email(to=to_email, cc=cc_email, subject=subject, html_content=body)

with DAG('email_alerting', default_args=default_args, schedule_interval=None, tags=['abc','def','ijk','xyz']) as dag:

    # Define tasks
    start = DummyOperator(task_id='start')
    start.on_success_callback = send_status_email
    start.on_failure_callback = send_failure_status_email
    
    step_1 = DummyOperator(task_id='step_1')
    step_1.on_success_callback = send_status_email
    step_1.on_failure_callback = send_failure_status_email
    
    step_2 = DummyOperator(task_id='step_2')
    step_2.on_success_callback = send_status_email
    step_2.on_failure_callback = send_failure_status_email
    
    # Add other steps 

    data_valifate_file = DummyOperator(task_id='data_valifate_file')
    data_valifate_file.on_success_callback = send_status_email
    data_valifate_file.on_failure_callback = send_failure_status_email
    
    step_21 = DummyOperator(task_id='step_21')
    step_21.on_success_callback = send_status_email
    step_21.on_failure_callback = send_failure_status_email
    
    step_22 = DummyOperator(task_id='step_22')
    step_22.on_success_callback = send_status_email
    step_22.on_failure_callback = send_failure_status_email
    
    # step_23 = DummyOperator(task_id='step_23')
    step_23 = BashOperator(
        task_id='step_23',
        bash_command='exit 1',  # Command that will exit with a non-zero code
        dag=dag
        )
    
    step_23.on_success_callback = send_status_email
    step_23.on_failure_callback = send_failure_status_email
    
    end = DummyOperator(task_id='end')
    end.on_success_callback = send_status_email
    end.on_failure_callback = send_failure_status_email
    
    # Define dependencies
    start >> step_1 >> [step_6, step_7]
    step_2 >> [step_6, step_7]
    start >> [step_1, step_2, step_3, step_4, step_5, data_valifate_file] >> step_21 >> step_22 >> step_23 >> end 
    step_6 >> step_21 
    step_7 >> step_21
    start >> step_17 >> step_18
    step_17 >> step_19 >> end
    step_18 >> step_19 >> end
    start >> step_20 >> end
