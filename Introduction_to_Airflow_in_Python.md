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

Available in soon, wait for it ... 
