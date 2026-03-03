import boto3
import pandas as pd
from sqlalchemy import create_engine
import io
import os 

# ==========================================
# [Master Mode] S3 -> RDS Pipeline (코드 오류 수정본)
# ==========================================

# [Fact] 설정 정보
S3_BUCKET = "master-data-pipeline-2026"
S3_FILE_KEY = "bronze/orders/dirty_orders.csv"
RDS_ENDPOINT = os.environ['RDS_ENDPOINT']
RDS_USER = os.environ['RDS_USER']
RDS_PWD = os.environ['RDS_PWD']

# [치명적 에러 수정 1] RDS_DB는 금고 건물 이름이 아니라, 금고 '내부의 방 이름'입니다.
# 아까 Additional configuration에서 만든 방 이름이 'pipeline_db' 입니다.
RDS_DB = "pipeline_db" 

# [치명적 에러 수정 2] TABLE_NAME은 데이터를 담을 '서랍 이름'입니다. 
# 방 이름(pipeline_db)을 서랍 이름으로 쓰면 논리적으로 꼬입니다.
TABLE_NAME = "raw_orders"

def s3_to_rds():
    s3 = boto3.client('s3')
    
    try:
        print("[*] 1. S3에서 데이터를 가져오는 중 (Extract)...")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_FILE_KEY)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        
        df = df.fillna('') 
        
        # [함정 방어] 네트워크 타임아웃 방지를 위해 connect_args 설정 추가 (10초면 포기하게 만듦)
        engine = create_engine(
            f'postgresql://{RDS_USER}:{RDS_PWD}@{RDS_ENDPOINT}:5432/{RDS_DB}',
            connect_args={'connect_timeout': 10}
        )        
        
        print(f"[*] 2. '{TABLE_NAME}' 테이블에 적재 시작 (Load)...")
        # 여기가 10초 이상 멈춰있다면 100% 네트워크(방화벽) 문제입니다.
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False, chunksize=500)
        
        print(f"[+] [Success] 총 {len(df)}건의 데이터가 적재되었습니다.")
        
    except Exception as e:
        print(f"[-] [Error] 파이프라인 붕괴 원인: {e}")

if __name__ == "__main__":
    s3_to_rds()