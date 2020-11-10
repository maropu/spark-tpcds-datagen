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

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain

from datetime import timedelta

# Configurations
envs = {
    'JAVA_HOME': '/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.265.b01-1.amzn2.0.1.x86_64',
    'SPARK_TPCDS_DATAGEN_HOME': '/home/ec2-user/spark-tpcds-datagen',
    'TPCDS_DATA': '/home/ec2-user/tpcds-sf20-data'
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
    schedule_interval='2 16 * * *',
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

checkout_date_command = """
# If `checkout_date` given, checks out a target snapshot
if [ -n "${CHECKOUT_DATE_PARAM}" ]; then
  _CHECKOUT_DATE=${CHECKOUT_DATE_PARAM}
else
  _CHECKOUT_DATE="{{ dag.start_date }}"
fi

echo ${_CHECKOUT_DATE}
"""

checkout_date = BashOperator(
    task_id='checkout_date',
    bash_command=checkout_date_command,
    env={ 'CHECKOUT_DATE_PARAM': '{{ dag_run.conf["checkout_date"] }}' },
    xcom_push=True,
    dag=dag
)

github_clone_command = """
_SPARK_HOME=`mktemp -d`
git clone https://github.com/apache/spark ${_SPARK_HOME} || exit -1
# TODO: Removes this
cp /home/ec2-user/TPCDSQueryBenchmark.scala ${_SPARK_HOME}/sql/core/src/test/scala/org/apache/spark/sql/execution/benchmark/
echo ${_SPARK_HOME}
"""

github_clone = BashOperator(
    task_id='github_clone',
    bash_command=github_clone_command,
    xcom_push=True,
    dag=dag
)

build_commamd = """
_COMMIT_HASHV=`cd ${SPARK_HOME} && git rev-list -1 --before="${CHECKOUT_DATE}" master`
echo "Checking out spark by date: ${CHECKOUT_DATE}(commit: ${_COMMIT_HASHV})" 1>&2
cd ${SPARK_HOME} && git checkout ${_COMMIT_HASHV} && \
  ./build/mvn package --also-make --projects assembly -DskipTests
"""

build_envs = {
    'SPARK_HOME': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="github_clone") }}',
    'CHECKOUT_DATE': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="checkout_date") }}'
}

build_envs.update(envs)

build_package = BashOperator(
    task_id='build_package',
    bash_command=build_commamd,
    env=build_envs,
    dag=dag
)

create_temp_file = BashOperator(
    task_id='create_temp_file',
    bash_command="mktemp",
    xcom_push=True,
    dag=dag
)

github_push_command = """
# Formats the output results and appends them into the report file
_FORMATTED_DATE=`LANG=en_US.UTF-8 date -d "${CHECKOUT_DATE}" '+%Y/%m/%d %H:%M'`
_REPORT_FILE=${SPARK_TPCDS_DATAGEN_HOME}/reports/tpcds-avg-results.csv
${SPARK_TPCDS_DATAGEN_HOME}/bin/format-results ${TEMP_OUTPUT} "${_FORMATTED_DATE}" >> ${_REPORT_FILE}

# Pushs it into git repository
cd ${SPARK_TPCDS_DATAGEN_HOME} && git add ${_REPORT_FILE} &&                               \
  git commit -m "[AUTOMATICALLY GENERATED] Update TPCDS reports at ${_FORMATTED_DATE})" && \
  git push origin master
"""

github_envs = {
    'SPARK_HOME': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="github_clone") }}',
    'TEMP_OUTPUT': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="create_temp_file") }}',
    'CHECKOUT_DATE': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="checkout_date") }}'
}

github_envs.update(envs)

github_push = BashOperator(
    task_id='github_push',
    bash_command=github_push_command,
    env=github_envs,
    dag=dag
)

run_tpcds_commamd = """
# Resolves proper Scala/Spark versions
_SCALA_VERSION=`grep "<scala.binary.version>" "${SPARK_HOME}/pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
_SPARK_VERSION=`grep "<version>" "${SPARK_HOME}/pom.xml" | head -n2 | tail -n1 | awk -F '[<>]' '{print $3}'`

# Temporary output file for each query group
_TEMP_OUTPUT=`mktemp`

if [ -n "${TPCDS_DATA_PARAM}" ]; then
  _TPCDS_DATA=${TPCDS_DATA_PARAM}
else
  _TPCDS_DATA=${TPCDS_DATA}
fi

echo "Using \`spark-submit\` from path: $SPARK_HOME" 1>&2
${SPARK_HOME}/bin/spark-submit                                         \
  --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
  --jars "${SPARK_HOME}/core/target/spark-core_${_SCALA_VERSION}-${_SPARK_VERSION}-tests.jar,${SPARK_HOME}/sql/catalyst/target/spark-catalyst_${_SCALA_VERSION}-${_SPARK_VERSION}-tests.jar" \
  --conf spark.ui.enabled=false          \
  --conf spark.master.rest.enabled=false \
  --conf spark.master=local[1]           \
  --conf spark.driver.memory=60g         \
  --conf spark.driver.extraJavaOptions="-Dlog4j.rootCategory=WARN,console" \
  --conf spark.sql.shuffle.partitions=32 \
  "${SPARK_HOME}/sql/core/target/spark-sql_${_SCALA_VERSION}-${_SPARK_VERSION}-tests.jar" \
  --data-location ${_TPCDS_DATA}         \
  --query-filter {{ params.QUERIES }}    \
  > ${_TEMP_OUTPUT}

# Appends the output results into a final output file
cat ${_TEMP_OUTPUT} | tee -a ${TEMP_OUTPUT} | cat
"""

# TODO: Needs to re-group queries to balance elapsed time
query_groups = [
    "q1,q2,q3,q4,q5,q6,q7,q8,q9,q10",
    "q11,q12,q13,q14a,q14b,q15,q16,q17,q18,q19,q20",
    "q21,q22,q23a,q23b,q24a,q24b,q25,q26,q27,q28,q29,q30",
    "q31,q32,q33,q34,q35,q36,q37,q38,q39a,q39b,q40,q41,q42,q43,q44,q45,q46,q47,q48,q49,q50,q51,q52,q53,q54,q55,q56,q57,q58,q59,q60,q61,q62,q63,q64,q65,q66,q67,q68,q69,q70",
    "q71,q72,q73,q74,q75,q76,q77,q78,q79,q80",
    "q81,q82,q83,q84,q85,q86,q87,q88,q89,q90,q91,q92,q93,q94,q95,q96,q97,q98,q99",
    "q5a-v2.7,q6-v2.7,q10a-v2.7,q11-v2.7,q12-v2.7,q14-v2.7,q14a-v2.7,q18a-v2.7,q20-v2.7,q22-v2.7,q22a-v2.7,q24-v2.7,q27a-v2.7,q34-v2.7,q35-v2.7,q35a-v2.7,q36a-v2.7",
    "q47-v2.7,q49-v2.7,q51a-v2.7,q57-v2.7,q64-v2.7,q67a-v2.7,q70a-v2.7,q72-v2.7",
    "q74-v2.7,q75-v2.7,q77a-v2.7,q78-v2.7",
    "q80a-v2.7,q86a-v2.7,q98-v2.7"
]

run_tpcds_tasks = []

runtime_envs = {
    'SPARK_HOME': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="github_clone") }}',
    'TEMP_OUTPUT': '{{ ti.xcom_pull(dag_id="report-tpcds-benchmark", task_ids="create_temp_file") }}',
    'TPCDS_DATA_PARAM': '{{ dag_run.conf["tpcds_data"] }}'
}

runtime_envs.update(envs)

for index, query_group in enumerate(query_groups):
    run_tpcds_tasks.append(BashOperator(
        task_id='run_tpcds_group_%d' % index,
        bash_command=run_tpcds_commamd,
        env=runtime_envs,
        params={ 'QUERIES': query_group },
        dag=dag
    ))

# Defines a workflow
[create_temp_file, checkout_date >> github_clone >> build_package] >> run_tpcds_tasks[0]
run_tpcds_tasks[-1] >> github_push

chain(*run_tpcds_tasks)

if __name__ == "__main__":
    dag.cli()

