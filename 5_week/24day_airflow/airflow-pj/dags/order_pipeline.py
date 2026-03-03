from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


# ── 알림 함수 ──
def send_failure_alert(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    exec_date = context['execution_date']
    logger.error(f"""
    🚨 PIPELINE FAILED!
    DAG: {dag_id} | Task: {task_id} | Date: {exec_date}
    """)


def log_pipeline_result(**context):
    logger.info(f"""
    ✅ PIPELINE COMPLETE!
    DAG: {context['dag'].dag_id}
    Date: {context['execution_date']}
    """)


# ── 기본 설정: 3번 재시도, 5분 간격, 실패 시 알림 ──
default_args = {
    'owner': 'master',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_failure_alert,
}


# ── DAG 정의: 매일 KST 15:30 (= UTC 03:30) ──
dag = DAG(
    'order_pipeline',
    default_args=default_args,
    description='S3→RDS→GE→dbt 주문 파이프라인',
    schedule_interval='50 3 * * *',
    start_date=datetime(2026, 3, 1),
    catchup=False,
)


# ── Task 1: S3 → RDS 적재 (실패 시 STOP) ──
task_s3_to_rds = BashOperator(
    task_id='s3_to_rds',
    bash_command='cd /home/ubuntu/week4_pratice && python3 s3_to_rds.py',
    dag=dag,
)

# ── Task 2: GE 데이터 검증 (실패 시 로그 + STOP) ──
task_ge = BashOperator(
    task_id='ge_validation',
    bash_command='cd /home/ubuntu/week4_pratice && python3 01_GE.py',
    dag=dag,
    # GE 실패 = exit code 1 → Airflow가 자동으로 STOP
    # 재시도 0번: 데이터 품질 문제는 재시도해도 안 고쳐짐
    retries=0,
)

# ── Task 3: dbt run (실패해도 WARN + 계속) ──
task_dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /home/ubuntu/week4_pratice/order_mart && dbt run || true',
    dag=dag,
)

# ── Task 4: dbt test (실패해도 WARN + 계속) ──
task_dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /home/ubuntu/week4_pratice/order_mart && dbt test || true',
    dag=dag,
)

# ── Task 5: 완료 로그 ──
task_log = PythonOperator(
    task_id='log_result',
    python_callable=log_pipeline_result,
    dag=dag,
    # 앞에서 뭐가 실패하든 로그는 기록
    trigger_rule='all_done',
)
task_s3_to_rds >> task_ge >> task_dbt_run >> task_dbt_test >> task_log

