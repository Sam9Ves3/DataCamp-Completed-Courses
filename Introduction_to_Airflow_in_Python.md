# Introduction to Airflow in Python	



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

## 
```py
x
```

## 
```py
x
```

## 
```py
x
```

## 
```py
x
```

