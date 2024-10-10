#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

# Airflow DAG
from datetime import datetime, timedelta, timezone
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator  # Import CDE operator to run CDE Jobs
from airflow.operators.dummy_operator import DummyOperator  # Import DummyOperator to define start and end tasks
import logging  # Import logging for logging information in tasks

username = "username" #copy and paste values here from Trial Manager configuration
# Set the DAG name dynamically based on the username from the config
dag_name = "BankFraudHol-" + username
logger = logging.getLogger(__name__)  # Initialize a logger for this DAG

print("Using DAG Name: {}".format(dag_name))  # Print the name of the DAG being used

# Define default arguments to be used across the DAG
default_args = {
    'owner': username,  # Set the owner of the DAG as the username from config
    'depends_on_past': False,  # Don't wait for previous task runs
    'start_date': datetime(2024, 5, 21),  # Define the start date for the DAG
}

# Initialize the DAG object
dag = DAG(
    dag_name,  # Name of the DAG
    default_args=default_args,  # Default arguments defined above
    catchup=False,  # Don't backfill previous missed runs
    schedule_interval='@once',  # Run the DAG once when triggered
    is_paused_upon_creation=False  # DAG will not be paused upon creation
)

# Dummy start task, used as an entry point for the DAG
start = DummyOperator(
    task_id="start",  # Unique task ID for this task
    dag=dag  # Reference to the DAG
)

# Task to run the 'Bronze' CDE Spark job in the CDE Jobs UI
bronze = CDEJobRunOperator(
    task_id='data-ingestion',  # Unique task ID for this task
    dag=dag,  # Reference to the DAG
    job_name='001_Lakehouse_Bronze_' + username,  # Name of the CDE job (must match job name in CDE Jobs UI)
    trigger_rule='all_success',  # This task runs only if the previous task succeeded
)

# Task to run the 'Silver' CDE Spark job in the CDE Jobs UI (Iceberg merge operation)
silver = CDEJobRunOperator(
    task_id='iceberg-merge-branch',  # Unique task ID for this task
    dag=dag,  # Reference to the DAG
    job_name='002_Lakehouse_Silver_' + username,  # Name of the CDE job (must match job name in CDE Jobs UI)
    trigger_rule='all_success',  # This task runs only if the previous task succeeded
)

# Task to run the 'Gold' CDE Spark job in the CDE Jobs UI
gold = CDEJobRunOperator(
    task_id='gold-layer',  # Unique task ID for this task
    dag=dag,  # Reference to the DAG
    job_name='003_Lakehouse_Gold_' + username,  # Name of the CDE job (must match job name in CDE Jobs UI)
    trigger_rule='all_success',  # This task runs only if the previous task succeeded
)

# Dummy end task, used as an exit point for the DAG
end = DummyOperator(
    task_id="end",  # Unique task ID for this task
    dag=dag  # Reference to the DAG
)

# Task dependencies: Start the DAG -> Run Bronze job -> Run Silver job -> Run Gold job -> End
start >> bronze >> silver >> gold >> end

