DE_practice — 데이터 엔지니어링 5주 집중 훈련

SQL → ETL → 시계열 → 통계 → dbt/GE → AWS 클라우드까지
로컬 수작업에서 24/7 무인 자동화 파이프라인으로 진화한 기록

주차별 학습 내용
주차핵심 주제사용 기술1주차SQL 심화 + ETL 기초PostgreSQL · pandas · JOIN/CTE/GROUP BY2주차시계열 + 센서 데이터 처리pandas · matplotlib · 이동평균/리샘플링3주차통계 기반 데이터 검증가설검정 · t-test · 상관/회귀4주차데이터 모델링 + 품질 검증dbt · Great Expectations · Incremental/Snapshot5주차클라우드 이주 + 자동화AWS EC2 · RDS · S3 · Airflow · Linux cron

핵심 파이프라인 (5주차 최종)
S3 (원시 데이터)
      ↓
s3_to_rds.py (EC2에서 실행)
      ↓
RDS PostgreSQL
      ↓
Great Expectations (데이터 품질 검증 — 실패 시 STOP)
      ↓
dbt run / dbt test (변환 + 모델 테스트)
      ↓
Airflow DAG (매일 새벽 4시 KST 자동 실행)

dbt 모델 구조
models/
├── staging/
│   ├── stg_orders.sql       # 원시 데이터 정제 (view)
│   └── stg_customers.sql
└── marts/
    ├── fct_orders.sql            # 전체 적재
    └── fct_orders_incremental.sql # 증분 적재 (unique_key + is_incremental)
