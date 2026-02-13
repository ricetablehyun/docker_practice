# 🛒 dbt + BigQuery E-commerce Data Warehouse

## 📌 프로젝트 개요
**Google BigQuery**와 **dbt(data build tool)**를 활용하여 Northwind 이커머스 데이터의 **ELT 파이프라인**을 구축
Raw Data 적재부터 Staging, Mart(Fact/Dimension) 모델링까지의 과정을 자동화하고, 데이터 품질 이슈를 해결하여 분석 가능한 환경을 구성

* **기간:** 2026.02 (Day 17)
* **역할:** 데이터 엔지니어링 (ETL/ELT, 모델링)
* **데이터셋:** Northwind Sample Data (Customers, Orders, Order Details, Products)

---

## 🏗️ 아키텍처 (Architecture)

```mermaid
graph LR
    Local_CSV[Local CSV Files] -->|Python Script| BQ_Raw[(BigQuery: raw_data)]
    BQ_Raw -->|dbt Source| Stg[Staging Layer (Views/Tables)]
    Stg -->|dbt ref()| Mart[Marts Layer (Fact Tables)]
    Mart -->|Docs| Lineage[Lineage Graph & Docs]

🛠️ 기술 스택 (Tech Stack)Data Warehouse: Google BigQueryTransformation: dbt (Data Build Tool) Core 1.11Language: Python 3.12 (Data Loading), SQL (Modeling)Version Control: Git & GitHub🚀 주요 기능 및 모델 구조1. Data Loading (Extract & Load)문제 상황: 원본 CSV(customers.csv)의 Address 컬럼 내부에 쉼표(,)가 포함되어 있어, BigQuery Auto-detect 적재 시 스키마 밀림(Schema Mismatch) 현상 발생.해결: Python 스크립트를 작성하여 불필요한 컬럼 제거(Preprocessing) 및 수동 스키마(Manual Schema) 지정 방식으로 적재 성공.2. dbt Modeling (Transform)LayerModel NameMaterialization설명Stagingstg_customerstable컬럼명 표준화 (CamelCase -> snake_case), 필수 컬럼 추출Stagingstg_ordersview주문 데이터 정제, Null 데이터 필터링Martsfct_ordersincrementalorders와 customers를 조인하여 분석용 Fact Table 생성💾 설치 및 실행 (How to Run)1. 환경 설정Bash# 가상환경 활성화
conda activate new_en

# 필수 라이브러리 설치
pip install dbt-core dbt-bigquery pandas google-cloud-bigquery
2. dbt 연결 설정 (profiles.yml)~/.dbt/profiles.yml 파일에 GCP Service Account 인증 정보 설정.YAMLecommerce_dw:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: [GCP_PROJECT_ID]
      dataset: dbt_dev
      keyfile: [PATH_TO_JSON_KEY]
      location: asia-northeast3
3. 실행 명령어Bash# 1. 연결 테스트
dbt debug

# 2. 모델 실행 (전체)
dbt run

# 3. 문서화 및 리니지 그래프 확인
dbt docs generate
dbt docs serve
🔥 트러블슈팅 (Troubleshooting Log)🛑 Issue: CSV 파싱 및 BigQuery 적재 오류증상: Error: CSV processing encountered too many errors. 주소 데이터 내 쉼표로 인해 컬럼 개수가 불일치함.원인: Pandas 엔진과 BigQuery의 CSV 파서가 따옴표(") 처리를 다르게 해석함.해결:Python으로 Raw CSV를 읽어 문제가 되는 Address 등 불필요 컬럼 제거 (final_cut.py).BigQuery 적재 시 Schema Auto-detect를 끄고, 정확한 타입(STRING)을 지정하여 적재.dbt 모델(stg_customers.sql)에서 실제 적재된 컬럼만 SELECT 하도록 수정.📂 프로젝트 구조Bash├── dbt_project.yml
├── models
│   ├── marts
│   │   └── fct_orders.sql      # 최종 Fact 모델
│   └── staging
│       ├── sources.yml         # 소스 데이터 정의
│       ├── stg_customers.sql   # 고객 데이터 정제
│       └── stg_orders.sql      # 주문 데이터 정제
├── analyses
├── seeds
└── tests