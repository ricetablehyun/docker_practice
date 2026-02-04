import pandas as pd
from sqlalchemy import create_engine

# [1] DB 연결 설정 (포트 5430 주의!)
# docker-compose.yml에 설정한 비번(postgres 혹은 pass1234) 확인 필수
# 형식: postgresql://아이디:비번@주소:포트/DB이름
db_connection_str = 'postgresql://analyst:pass1234@localhost:5430/manufacturing'
db_engine = create_engine(db_connection_str)

# 파이프 라인 함수 생성
def run_bosch_pipeline():
    print("Bosch 데이터 파이프라인 시작")
    
    # ---------------------------------------------------------
    # Step 1: 추출 : Extract & Sampling (맛보기)
    # ---------------------------------------------------------
    print("1. 데이터 읽기 (Sampling)...")
    # 실제로는 수기가가바이트지만, nrows로 일부만 읽거나 sample을 씀
    # 여기선 우리가 만든 dummy 데이터를 읽음
    df = pd.read_csv('bosch_dummy.csv',nrows=1000) # ,nrow=n을 하면 샘플링할 데이터 n개를 의미한다. 
    # Q4. 샘플링해서 일부만 했지만 이거를 전체는 그럼 언제하고 샘플을 통해서 뭘확인하는것? --> 샘플링의 목적은 제대로 코드가 돌아가는지 확인용이다. 앞부분 부터 뽑기보다는 df.sample(n=1000)이런식으로 한다. / 일단 샘플로 확인하고 적용은 실무에서는 밤에 전체적으로 한다.
    # df = pr.read_csv('bosch_dummy.csv').sample(n=1000,random_state=42)
    print(f"    -> 추출한  데이터 크기: {df.shape} (행,열)")
    
    # ---------------------------------------------------------
    # Step 2: 전처리 : Transform (썩은 귤 골라내기)
    # ---------------------------------------------------------
    print("2. 결측치 분석 및 제거 중...")
    
    # 각 센서(컬럼)별로 비어있는 값(NULL)의 비율 계산
    missing_ratio = df.isna().sum(axis=0) / len(df) * 100
    
    # 결측치가 50% 이상인 '고장난 센서 찾기'
    bad_sensor = missing_ratio[missing_ratio >= 50].index
    # Q1. .index로하면 어찌 나타나는거임? -> 인덱스만 이용해서 찾는거임. 효율적이고 빠름. 
    print(f"50% 이상 오류인 센서 발견 {list(bad_sensor)}")
    
    # 해당 나쁜 센서 delete
    df_clean = df.drop(columns=bad_sensor)
    print(f"나쁜 센서 삭제 & 남은 데이터 크기는 {df_clean.shape}")
    
    # 결측치 처리 
    df_clean = df_clean.fillna(0) 
    
    # ---------------------------------------------------------
    # Step 3: Load (창고 적재)
    # ---------------------------------------------------------
    print("3. DB 적재 중...")
    
    # 'bosch_production' 이라는 테이블로 저장
    df_clean.to_sql(name='bosch_production',con=db_engine,if_exists='replace', index=False)
    #Q2. con은 뭐임? -> 연결통로 지정임 우리는 db_engine을 만들었기에 이걸 사용하는것임. 
    print("✅ 파이프라인 성공! 'bosch_production' 테이블을 확인하세요.")
    
if __name__ == "__main__":
    run_bosch_pipeline()
    # Q3. 항상 헷갈림 --> 남이 내코드를 import할떄는 함수만 빌려주고 내가 직접실행할떄만 동잦ㄱ하게 하는것.
    
    # Q5. ML이랑 똑같은데? 파이썬 로드 - EDA,전처리 - Model / DB로드 차이. --> ETL = ML에서 "모델 학습" 빼고 "DB 저장"으로 바꾼 것
