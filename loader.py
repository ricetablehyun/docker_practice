import pandas as pd
from sqlalchemy import create_engine

# 1. [접속 정보] DB 문을 열 열쇠 (비번: postgres 확인!)
# 형식: postgresql://아이디:비번@주소:포트/DB이름
db_connection_str = 'postgresql://analyst:pass1234@localhost:5430/manufacturing'
db_connection = create_engine(db_connection_str)

try:
    # 2. [Extract] 짐(CSV) 싣기
    df = pd.read_csv('user_data.csv')
    print("1. CSV 파일 읽기 성공!")
    print(df) 

    # 3. [Load] DB 창고에 넣기
    # name='users' -> 'users'라는 테이블을 새로 만듦
    # if_exists='replace' -> 기존 거 있으면 싹 밀고 새로 만듦
    df.to_sql(name='users', con=db_connection, if_exists='replace', index=False)
    
    print("\n2. DB 적재 성공! 🚀")
    print("이제 DBeaver 가서 확인해보세요!")

except Exception as e:
    print(f"❌ 에러 발생: {e}")