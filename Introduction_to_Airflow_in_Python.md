-----------------------------------------------------
# Introduction to Airflow in Python	
------------------------------------------------------

# Chapter 1: Intro to Airflow

## Defining a simple DAG
```
# Import the DAG object
from airflow.models import DAG
```

## Troubleshooting DAG creation
in `refresh_data_workflow.py`:
```
from airflow.models import DAG
default_args = {
  'owner': 'jdoe',
  'email': 'jdoe@datacamp.com'
}
dag = DAG( 'refresh_data', default_args=default_args )
```

# Chapter 2: Implementing Airflow DAGs

## Defining a BashOperator task

```py
# Import the BashOperator
from airflow.operators.bash_operator import BashOperator

# Define the BashOperator 
cleanup = BashOperator(
    task_id='cleanup_task',
    # Define the bash_command
    bash_command= 'cleanup.sh',
    # Add the task to the dag
    dag=analytics_dag
)
```



## Multiple BashOperators
```py
# Define a second operator to run the `consolidate_data.sh` script
consolidate = BashOperator(
    task_id='consolidate_task',
    bash_command= 'consolidate_data.sh',
    dag=analytics_dag)

# Define a final operator to execute the `push_data.sh` script
push_data = BashOperator(
    task_id='pushdata_task',
    bash_command='push_data.sh',
    dag=analytics_dag)
    
   ```
   
   
  ## Define order of BashOperators
  
  ```py
   # Define a new pull_sales task
pull_sales = BashOperator(
    task_id='pullsales_task',
    bash_command= 'wget https://salestracking/latestinfo?json',
    dag=analytics_dag
)

# Set pull_sales to run prior to cleanup
pull_sales >> cleanup

# Configure consolidate to run after cleanup
consolidate << cleanup

# Set push_data to run last
consolidate >> push_data

```


## Determining the order of tasks

1. initialize_process
2. pull_data
3. clean
4. run_ml_pipeline
5. generate_reports



## Troubleshooting DAG dependencies
`task3 >> task1`


## Using the PythonOperator

### 1
```py
# Define the method
def pull_file(URL, savepath):
    r = requests.get(URL)
    with open(savepath, 'wb') as f:
        f.write(r.content)    
    # Use the print method for logging
    print(f"File pulled from {URL} and saved to {savepath}")
```


### 2
```py
def pull_file(URL, savepath):
    r = requests.get(URL)
    with open(savepath, 'wb') as f:
        f.write(r.content)   
    # Use the print method for logging
    print(f"File pulled from {URL} and saved to {savepath}")
    
# Import the PythonOperator class
from airflow.operators.python_operator import PythonOperator
```

### 3
```py
def pull_file(URL, savepath):
    r = requests.get(URL)
    with open(savepath, 'wb') as f:
        f.write(r.content)   
    # Use the print method for logging
    print(f"File pulled from {URL} and saved to {savepath}")

from airflow.operators.python_operator import PythonOperator

# Create the task
pull_file_task = PythonOperator(
    task_id ='pull_file',
    # Add the callable
    python_callable=pull_file,
    # Define the arguments
    op_kwargs={'URL':'http://dataserver/sales.json', 'savepath':'latestsales.json'},
    dag=process_sales_dag
)
```


## More PythonOperators

```py
# Add another Python task
parse_file_task = PythonOperator(
    task_id='parse_file',
    # Set the function to call
    python_callable = parse_file,
    # Add the arguments
    op_kwargs={'inputfile':'latestsales.json', 'outputfile':'parsedfile.json'},
    # Add the DAG
    dag=process_sales_dag
)
```

## EmailOperator and dependencies
```py
# Import the Operator
from airflow.operators.email_operator import EmailOperator

# Define the task
email_manager_task = EmailOperator(
    task_id='email_manager',
    to='manager@datacamp.com',
    subject='Latest sales JSON',
    html_content='Attached is the latest sales JSON file as requested.',
    files='parsedfile.json',
    dag=process_sales_dag
)

# Set the order of tasks
pull_file_task >> parse_file_task >> email_manager_task
```

##  Schedule a DAG via Python
```py
# Update the scheduling arguments as defined
default_args = {
  'owner': 'Engineering',
  'start_date': datetime(2019, 11, 1),
  'email': ['airflowresults@datacamp.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 3,
  'retry_delay': timedelta(minutes=20)
}

dag = DAG('update_dataflows', default_args=default_args, schedule_interval='30 12 * * 3')
```

## Deciphering Airflow schedules
1. `*****`
2. `timedelta(minutes=5)`
3. `@hourly`
4. `*0,12***`
5. `timedelta(days=1)`
6. `@weekly`



# Chapter 3: Maintaining and monitoring Airflow workflows

## Sensors vs operators

|     **Sensors**                    | **Both**                  | **Operators**                   |
|:----------------------------------:|:-------------------------:|--------------------------------:|
| has a `poke_interval` attribute.  | Are assigned to DGAs.    | `BashOperator`                    |
| Derives from `BaseSensorOperator`.| Have a `task_id`.       | Only runs once per DAG run.       |
| `FileSnsor`              |



## Executor implications
Step 1: in `execute_report_dag.py`, line 15:
```py
    mode='reschedule',
```
Step 2: in command line, run `airflow list_dags `



## Missing DGA
Step 1: Delete `#` in line 2, in order to convert the line from comment to code 

Step 2: save `execute_report_dag.py` in  workspace/drags

Step 3: In command line, execute `airflow list_drags` to verify that the `.py` is stored in the drags folder


## Defining a SLA


```py
# Import the timedelta object
from datetime import timedelta

# Create the dictionary entry
default_args = {
  'start_date': datetime(2020, 2, 20),
  'sla': timedelta(minutes=30)
}

# Add to the DAG
test_dag = DAG('test_workflow', default_args=default_args, schedule_interval='@None')
```

## Defining a task SLA
```py
# Import the timedelta object
from datetime import timedelta

test_dag = DAG('test_workflow', start_date=datetime(2020,2,20), schedule_interval='@None')

# Create the task with the SLA
task1 = BashOperator(task_id='first_task',
                     sla=timedelta(hours=3),
                     bash_command='initialize_data.sh',
                     dag=test_dag)
```

## Generate and email a report
```py
# Define the email task
email_report = EmailOperator(
        task_id='email_report',
        to='airflow@datacamp.com',
        subject='Airflow Monthly Report',
        html_content="""Attached is your monthly workflow report - please refer to it for more detail""",
        files=['monthly_report.pdf'],
        dag=report_dag
)

# Set the email task to run after the report is generated
email_report << generate_report
```


## Adding status email
In `execute_report_dag.py` the `default_args` should be:
```py
default_args={
    'email': ['airflowalerts@datacamp.com', 'airflowadmin@datacamp.com'],
    'email_on_failure': True,
    'email_on_success': True
}
```



# Chapter 4: Building production pipelines in Airflow

## Creating a templated BashOperator


```
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
  'start_date': datetime(2020, 4, 15),
}

cleandata_dag = DAG('cleandata',
                    default_args=default_args,
                    schedule_interval='@daily')

# Create a templated command to execute
# 'bash cleandata.sh datestring'
templated_command = """
bash cleandata.sh {{ ds_nodash }}
"""

# Modify clean_task to use the templated command
clean_task = BashOperator(task_id='cleandata_task',
                          bash_command=templated_command,
                          dag=cleandata_dag)
```


## Templates with multiple arguments


```
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
  'start_date': datetime(2020, 4, 15),
}

cleandata_dag = DAG('cleandata',
                    default_args=default_args,
                    schedule_interval='@daily')

# Create a templated command to execute
# 'bash cleandata.sh datestring'
templated_command = """
  bash cleandata.sh {{ ds_nodash }} {{ params.filename }}
"""

# Modify clean_task to use the templated command
clean_task = BashOperator(task_id='cleandata_task',
                          bash_command=templated_command,
                          params={'filename': 'salesdata.txt'},
                          dag=cleandata_dag)
                          
# Create a new BashOperator clean_task2
clean_task2 = BashOperator(task_id='cleandata_task2',
                           bash_command=templated_command,
                           params={'filename': 'supportdata.txt'},
                           dag=cleandata_dag)
                           
# Set the operator dependencies
clean_task >> clean_task2
```


## Using lists with templates

```
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

filelist = [f'file{x}.txt' for x in range(30)]

default_args = {
  'start_date': datetime(2020, 4, 15),
}

cleandata_dag = DAG('cleandata',
                    default_args=default_args,
                    schedule_interval='@daily')

# Modify the template to handle multiple files in a 
# single run.
templated_command = """
  <% for filename in params.filenames %>
  bash cleandata.sh {{ ds_nodash }} {{ filename }};
  <% endfor %>
"""

# Modify clean_task to use the templated command
clean_task = BashOperator(task_id='cleandata_task',
                          bash_command=templated_command,
                          params={'filenames': filelist},
                          dag=cleandata_dag)

```

## Sending templated emails

```
from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

# Create the string representing the html email content
html_email_str = """
Date: {{ ds }}
Username: {{ params.username }}
"""

email_dag = DAG('template_email_test',
                default_args={'start_date': datetime(2020, 4, 15)},
                schedule_interval='@weekly')
                
email_task = EmailOperator(task_id='email_task',
                           to='testuser@datacamp.com',
                           subject="{{ macros.uuid.uuid4() }}",
                           html_content=html_email_str,
                           params={'username': 'testemailuser'},
                           dag=email_dag)
```


## Define a BranchPythonOperator
```
# Create a function to determine if years are different
def year_check(**kwargs):
    current_year = int(kwargs['ds_nodash'][0:4])
    previous_year = int(kwargs['prev_ds_nodash'][0:4])
    if current_year == previous_year:
        return 'current_year_task'
    else:
        return 'new_year_task'

# Define the BranchPythonOperator
branch_task = BranchPythonOperator(task_id='branch_task', dag=branch_dag,
                                   python_callable=year_check, provide_context=True)
# Define the dependencies
branch_dag >> current_year_task
branch_dag >> new_year_task
```

## Creating a production pipeline #1
```
from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor

# Import the needed operators
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date

def process_data(**context):
  file = open("/home/repl/workspace/processed_data.tmp", "w")
  file.write(f"Data processed on {date.today()}")
  file.close()

    
dag = DAG(dag_id="etl_update", default_args={"start_date": "2020-04-01"})

sensor = FileSensor(task_id="sense_file", 
                    filepath="/home/repl/workspace/startprocess.txt",
                    poke_interval=5,
                    timeout=15,
                    dag=dag)

bash_task = BashOperator(task_id="cleanup_tempfiles", 
                         bash_command="rm -f /home/repl/*.tmp",
                         dag=dag)

python_task = PythonOperator(task_id="run_processing", 
                             python_callable=process_data,
                             dag=dag)

sensor >> bash_task >> python_task
```



## Creating a production pipeline #2

```
from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from dags.process import process_data
from datetime import timedelta

# Update the default arguments and apply them to the DAG
default_args = {
  "start_date": "2019-01-01",
  "sla": timedelta(minutes=90)
}

dag = DAG(dag_id="etl_update", default_args=default_args)

sensor = FileSensor(task_id="sense_file", 
                    filepath="/home/repl/workspace/startprocess.txt",
                    poke_interval=timedelta(seconds=45),
                    dag=dag)

bash_task = BashOperator(task_id="cleanup_tempfiles", 
                         bash_command="rm -f /home/repl/*.tmp",
                         dag=dag)

python_task = PythonOperator(task_id="run_processing", 
                             python_callable=process_data,
                             provide_context=True,
                             dag=dag)

sensor >> bash_task >> python_task
```

## Adding the final changes to your pipeline

```
from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from dags.process import process_data
from datetime import datetime, timedelta

# Update the default arguments and apply them to the DAG.

default_args = {
  "start_date": "2019-01-01",
  "sla": timedelta(minutes=90)
}
    
dag = DAG(dag_id="etl_update", default_args=default_args)

sensor = FileSensor(task_id="sense_file", 
                    filepath="/home/repl/workspace/startprocess.txt",
                    poke_interval=timedelta(seconds=45),
                    dag=dag)

bash_task = BashOperator(task_id="cleanup_tempfiles", 
                         bash_command="rm -f /home/repl/*.tmp",
                         dag=dag)

python_task = PythonOperator(task_id="run_processing", 
                             python_callable=process_data,
                             provide_context=True,
                             dag=dag)

email_subject="""
  Email report for {{ params.department }} on {{ ds_nodash }}
"""

email_report_task = EmailOperator(task_id='email_report_task',
                                  to='sales@mycompany.com',
                                  subject=email_subject,
                                  params={'department': 'Data subscription services'},
                                  dag=dag)

no_email_task = DummyOperator(task_id='no_email_task', dag=dag)

def check_weekend(**kwargs):
    dt = datetime.strptime(kwargs['execution_date'],"%Y-%m-%d")
    # If dt.weekday() is 0-4, it's Monday - Friday. If 5 or 6, it's Sat / Sun.
    if (dt.weekday() > 4):
        return 'email_report_task'
    else:
        return 'no_email_task'
    
branch_task = BranchPythonOperator(task_id="check_if_weekend",
                                   python_callable=check_weekend,
                                   provide_context=True,
                                   dag=dag)

    
sensor >> bash_task >> python_task

python_task >> branch_task >> [email_report_task, no_email_task]

```





