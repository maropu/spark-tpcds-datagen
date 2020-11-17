# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# A workflow script for running TPCDS benchmarks and pushing the results into GitHub
# A configuration list is as follows when triggering a workflow:
#
#  - tpcds_data:    Specify a path of generated TPCDS data
#                   (e.g., {"tpcds_data": "/home/ec2-user/tpcds-sf1-data"})
#  - checkout_date: Specify a date to check out the Spark codebase via a git command
#                   (e.g., {"checkout_date": "2020-11-10 00:00:00+00:00"})
#  - checkout_pr:   Specify a GitHub PR number to check out the Spark codebase via git command
#                   (e.g., {"checkout_pr": 30012})
#  - to_email:      Specify an email address to send TPCDS benchmark results
#                   (e.g., {"to_email": "airflow@example.com"})

from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain

# Default configurations
envs = {
    'JAVA_HOME': '/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.265.b01-1.amzn2.0.1.x86_64',
    'SPARK_TPCDS_DATAGEN_HOME': '/home/ec2-user/spark-tpcds-datagen',
    'TPCDS_DATA': '/home/ec2-user/tpcds-sf20-data',
    'HTTPS_PROXY': '172.31.16.10:8080'
}

# Default parameters for base operators
# See: https://airflow.apache.org/docs/stable/_api/airflow/operators/index.html#airflow.operators.BaseOperator
default_args = {
    'owner': 'Airflow',
    # 'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': False,
    # 'start_date': days_ago(1),
    'depends_on_past': True,
    'wait_for_downstream': True,
    # 'priority_weight': 10,
    # 'weight_rule': 'downstream'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'execution_timeout': timedelta(hours=2),
    # 'trigger_rule': 'all_success',
    'run_as_user': 'ec2-user'
}

# Default parameters for DAG
# See: https://airflow.apache.org/docs/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG
dag = DAG(
    dag_id='report-tpcds-benchmark',
    description='Spark TPC-DS Benchmark Results',
    schedule_interval='2 15 * * *',
    start_date=days_ago(1),
    end_date=None,
    user_defined_macros=None,
    default_args=default_args,
    params=None,
    concurrency=1,
    max_active_runs=1,
    dagrun_timeout=None,
    default_view='graph',
    orientation='TB',
    catchup=False,
    tags=['spark']
)

tpcds_data = BashOperator(
    task_id='tpcds_data',
    bash_command="""
    if [ -n "{{ dag_run.conf["tpcds_data"] }}" ]; then
      _TPCDS_DATA="{{ dag_run.conf["tpcds_data"] }}"
    else
      _TPCDS_DATA=${TPCDS_DATA}
    fi

    echo ${_TPCDS_DATA}
    """,
    env={ 'TPCDS_DATA': envs['TPCDS_DATA'] },
    xcom_push=True,
    dag=dag
)

checkout_date = BashOperator(
    task_id='checkout_date',
    bash_command="""
    # If `checkout_date` given, checks out a target snapshot
    if [ -n "{{ dag_run.conf["checkout_date"] }}" ]; then
      _CHECKOUT_DATE="{{ dag_run.conf["checkout_date"] }}"
    else
      _CHECKOUT_DATE="{{ dag.start_date }}"
    fi

    echo ${_CHECKOUT_DATE}
    """,
    xcom_push=True,
    dag=dag
)

checkout_pr = BashOperator(
    task_id='checkout_pr',
    bash_command='echo ${CHECKOUT_PR_PARAM}',
    env={ 'CHECKOUT_PR_PARAM': '{{ dag_run.conf["checkout_pr"] }}' },
    xcom_push=True,
    dag=dag
)

github_clone = BashOperator(
    task_id='github_clone',
    bash_command="""
    _SPARK_HOME=`mktemp -d`
    git clone https://github.com/apache/spark ${_SPARK_HOME} || exit -1
    echo ${_SPARK_HOME}
    """,
    xcom_push=True,
    dag=dag
)

checkout = BashOperator(
    task_id='checkout',
    bash_command="""
    if [ -n "${CHECKOUT_PR}" ]; then
      echo "Checking out Spark by GitHub PR number: ${CHECKOUT_PR}" 1>&2
      cd ${SPARK_HOME} && git fetch origin pull/${CHECKOUT_PR}/head:pr${CHECKOUT_PR} && \
      git checkout pr${CHECKOUT_PR} &&                                                  \
      git rebase master || exit -1
    elif [ "{{ dag_run.external_trigger }}" = "True" ] && [ -n "{{ dag_run.conf["checkout_date"] }}" ]; then
      _COMMIT_HASHV=`git -C ${SPARK_HOME} rev-list -1 --before="${CHECKOUT_DATE}" master`
      echo "Checking out Spark by date: ${CHECKOUT_DATE} (commit: ${_COMMIT_HASHV})" 1>&2
      git -C ${SPARK_HOME} checkout ${_COMMIT_HASHV} || exit -1
    fi
    """,
    env={
        'SPARK_HOME': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="github_clone") }}',
        'CHECKOUT_DATE': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="checkout_date") }}',
        'CHECKOUT_PR': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="checkout_pr") }}',
        'HTTPS_PROXY': envs['HTTPS_PROXY']
    },
    dag=dag
)

create_temp_file = BashOperator(
    task_id='create_temp_file',
    bash_command='mktemp',
    xcom_push=True,
    dag=dag
)

build_package = BashOperator(
    task_id='build_package',
    bash_command="""
    cd ${SPARK_HOME} && ./build/mvn package --also-make --projects assembly -DskipTests
    """,
    env={
        'SPARK_HOME': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="github_clone") }}',
        'JAVA_HOME': envs['JAVA_HOME']
    },
    dag=dag
)

format_results = BashOperator(
    task_id='format_results',
    bash_command="""
    _FORMATTED_DATE=`LANG=en_US.UTF-8 date -d "${CHECKOUT_DATE}" '+%Y/%m/%d %H:%M'`
    _FORMATTED_RESULTS=`mktemp`
    ${SPARK_TPCDS_DATAGEN_HOME}/bin/format-results ${TEMP_OUTPUT} "${_FORMATTED_DATE}" > ${_FORMATTED_RESULTS}
    cat ${TEMP_OUTPUT} # For logging
    echo ${_FORMATTED_RESULTS}
    """,
    env={
        'SPARK_HOME': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="github_clone") }}',
        'TEMP_OUTPUT': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="create_temp_file") }}',
        'CHECKOUT_DATE': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="checkout_date") }}',
        'SPARK_TPCDS_DATAGEN_HOME': envs['SPARK_TPCDS_DATAGEN_HOME']
    },
    xcom_push=True,
    dag=dag
)

def select_output_func(**context):
    if context['dag_run'].external_trigger and context['dag_run'].conf is not None:
        if 'to_email' in context['dag_run'].conf:
            return 'copy_results_to_file'
        else:
            return 'dump_file'

    return 'github_push'

select_output = BranchPythonOperator(
    task_id='select_output',
    provide_context=True,
    python_callable=select_output_func,
    dag=dag
)

github_push = BashOperator(
    task_id='github_push',
    bash_command="""
    # Pushs it into a GitHub repository
    cd ${SPARK_TPCDS_DATAGEN_HOME} &&                                \
      cat ${FORMATTED_RESULTS} >> ./reports/tpcds-avg-results.csv && \
      git add ./reports/tpcds-avg-results.csv &&                     \
      git commit -m "[AUTOMATICALLY GENERATED] Update TPCDS reports at ${CHECKOUT_DATE})" && \
      git push origin master
    """,
    env={
        'CHECKOUT_DATE': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="checkout_date") }}',
        'FORMATTED_RESULTS': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="format_results") }}',
        'SPARK_TPCDS_DATAGEN_HOME': envs['SPARK_TPCDS_DATAGEN_HOME']
    },
    dag=dag
)

copy_results_to_file = BashOperator(
    task_id='copy_results_to_file',
    bash_command="""
    cat ${SPARK_TPCDS_DATAGEN_HOME}/reports/tpcds-results-header.txt ${FORMATTED_RESULTS} \
      > /tmp/spark-tpcds-benchmark-report
    """,
    env={
        'FORMATTED_RESULTS': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="format_results") }}',
        'SPARK_TPCDS_DATAGEN_HOME': envs['SPARK_TPCDS_DATAGEN_HOME']
    },
    dag=dag
)

dump_file = BashOperator(
    task_id='dump_file',
    bash_command="""
    cat ${SPARK_TPCDS_DATAGEN_HOME}/reports/tpcds-results-header.txt ${FORMATTED_RESULTS}
    """,
    env={
        'FORMATTED_RESULTS': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="format_results") }}',
        'SPARK_TPCDS_DATAGEN_HOME': envs['SPARK_TPCDS_DATAGEN_HOME']
    },
    dag=dag
)

send_email = EmailOperator(
    task_id='send_email',
    to='{{ dag_run.conf["to_email"] }}',
    subject='Spark TPC-DS Benchmark Results: {{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="checkout_date") }}',
    html_content='',
    files=['/tmp/spark-tpcds-benchmark-report'],
    dag=dag
)

run_tpcds_commamd = """
# Resolves proper Scala/Spark versions
_SCALA_VERSION=`grep "<scala.binary.version>" "${SPARK_HOME}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
_SPARK_VERSION=`grep "<version>" "${SPARK_HOME}/pom.xml" | head -n2 | tail -n1 | awk -F '[<>]' '{print $3}'`

# Temporary output file for each query group
_TEMP_OUTPUT=`mktemp`

# Creates a propertye file for log4j if it does not exist
_LOG4J_PROP_FILE=${SPARK_HOME}/conf/log4j.properties
if [ ! -e ${_LOG4J_PROP_FILE} ]; then
  cat << EOF > ${_LOG4J_PROP_FILE}
log4j.rootCategory=INFO, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
EOF
fi

echo "Using \`spark-submit\` from path: $SPARK_HOME" 1>&2
${SPARK_HOME}/bin/spark-submit                                         \
  --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
  --jars "${SPARK_HOME}/core/target/spark-core_${_SCALA_VERSION}-${_SPARK_VERSION}-tests.jar,${SPARK_HOME}/sql/catalyst/target/spark-catalyst_${_SCALA_VERSION}-${_SPARK_VERSION}-tests.jar" \
  --conf spark.ui.enabled=false          \
  --conf spark.master.rest.enabled=false \
  --conf spark.master=local[1]           \
  --conf spark.driver.memory=60g         \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://${_LOG4J_PROP_FILE}"   \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://${_LOG4J_PROP_FILE}" \
  "${SPARK_HOME}/sql/core/target/spark-sql_${_SCALA_VERSION}-${_SPARK_VERSION}-tests.jar"   \
  --data-location ${TPCDS_DATA}          \
  --query-filter {{ params.QUERIES }}    \
  > ${_TEMP_OUTPUT} || exit -1

# Appends the output results into a final output file
cat ${_TEMP_OUTPUT} | tee -a ${TEMP_OUTPUT} | cat
"""

query_groups = [
    "q1,q2,q3,q4,q5,q6,q7,q8,q9,q10",
    "q11,q12,q13,q14a,q14b,q15,q16,q17,q18,q19,q20",
    "q21,q22,q23a,q23b,q24a,q24b,q25,q26,q27,q28,q29,q30",
    "q31,q32,q33,q34,q35,q36,q37,q38,q39a,q39b,q40,q41,q42,q43,q44,q45,q46,q47,q48,q49,q50,q51,q52,q53,q54,q55,q56,q57,q58,q59,q60,q61,q62,q63,q64,q65,q66,q67,q68,q69,q70",
    "q71,q72,q73,q74,q75,q76,q77,q78,q79,q80",
    "q81,q82,q83,q84,q85,q86,q87,q88,q89,q90,q91,q92,q93,q94,q95,q96,q97,q98,q99",
    "q5a-v2.7,q6-v2.7,q10a-v2.7,q11-v2.7,q12-v2.7,q14-v2.7,q14a-v2.7,q18a-v2.7,q20-v2.7,q22-v2.7,q22a-v2.7,q24-v2.7,q27a-v2.7,q34-v2.7,q35-v2.7,q35a-v2.7,q36a-v2.7",
    "q47-v2.7,q49-v2.7,q51a-v2.7,q57-v2.7,q64-v2.7",
    "q67a-v2.7,q70a-v2.7,q72-v2.7",
    "q74-v2.7,q75-v2.7,q77a-v2.7,q78-v2.7,q80a-v2.7,q86a-v2.7,q98-v2.7"
]

run_tpcds_tasks = []

for index, query_group in enumerate(query_groups):
    run_tpcds_tasks.append(BashOperator(
        task_id='run_tpcds_group_%d' % index,
        bash_command=run_tpcds_commamd,
        env={
            'SPARK_HOME': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="github_clone") }}',
            'TEMP_OUTPUT': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="create_temp_file") }}',
            'TPCDS_DATA': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="tpcds_data") }}',
            'JAVA_HOME': envs['JAVA_HOME']
        },
        params={ 'QUERIES': query_group },
        dag=dag
    ))

# Defines a workflow
[[checkout_date, checkout_pr, github_clone] >> checkout] >> build_package
[create_temp_file, tpcds_data, build_package] >> run_tpcds_tasks[0]
chain(*run_tpcds_tasks)
run_tpcds_tasks[-1] >> format_results >> select_output

select_output >> github_push
select_output >> copy_results_to_file >> send_email
select_output >> dump_file

if __name__ == "__main__":
    dag.cli()

