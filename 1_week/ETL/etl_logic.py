import pandas as pd
from sqlalchemy import create_engine # 여러 DB지원하며 고수준 추상화. 이거를 현업에서 더많이씀. 
# create_engine으로 "엔진 : db 연결을 관리하는 중심객체"로 자동 관리해준다.
# ORM : DB테이블 행을 객체로 다루게끔해준다. 


# 1. [접속 정보] DB 문을 열 열쇠 (비번: postgres 확인!)
# 형식: postgresql://아이디:비번@주소:포트/DB이름
db_connection_str = 'postgresql://analyst:pass1234@localhost:5430/manufacturing'
db_engine = create_engine(db_connection_str)

def etl_process():
    print("=== ETL 가동 시작 ===")
    # [2] EXTRACT : DB에서 데이터 가져오기
    # SQL 은 '가져오는'역할만 시킴.
    print('1. 데이터 추출중 ')
    df = pd.read_sql("SELECT * FROM users" , db_engine ) 
    # Q1. 읽는데 어떤 DB인지 모르니까 엔진이 자동으로 맞춰줘서 넣는건가? 그리고 요청만 보냈지 가져오는건 fetch아닌가? 그리고 역할은 EXECUTE같은데 그함수는 안써있네 그게 엔진인건가? 
    # --> pd.read_sql 자체가 그냥 밀키트임. connect,execute,fetch,close의 기능을 알아서 해줌.
    # Q1-1 그럼 read_sql("SQL명령어",주소가 담긴변수(보통engine))인거? -> ㅇㅇ "쿼리문",engine임. 엔진에 db정보가 다 들어있음. 
    print(f"{len(df)}명의 데이터 수집 완료")
    
    # [3] TRANSFORM :Python으로 지지고 볶기 (logic)
    print("2. 데이터 가공 중 (등급심사)...")
    
    # 함수 정의: Python은 이런 복잡한 로직을 함수로 쉽게 만듦 
    # q2. 이거는 그냥 db에서 꺼내서 가공을 자동으로하는거인데 자동으로 수집해서 db로 만드는거도 되는거지?
    # --> ㅇㅇ 가능. “DB에서 꺼내서 가공 후 다시 DB로 적재”라서 엄밀히는 DB → Python → DB 흐름이고,외부에서 가져오면 파일/API → Python → DB 흐름이 돼. 둘 다 ETL.
    def determine_grade(age):
        if pd.isna(age):
            return "Unknown"
        elif age >= 30:
            return "VIP"
        else: return "Normal"
        
    # apply 함수 : 엑셀 함수 적용하기랑 똑같음. Q3.apply 함수란? -> 적용하기 
    df['grade'] = df['age'].apply(determine_grade)
    
    # 결과 미리보기
    print(" [가공 결과 미리보기]")
    print(df[['name', 'age', 'grade']])
    
    # [4] LOAD: 다시 DB에 넣기
    print("3. DB에 적재중 ..")
    df.to_sql(name='user_grades', con=db_engine, if_exists='replace', index=False)
    # index=False: 0,1,2 숫자 인덱스는 저장 안 함
    # if_exists='replace': 테이블 있으면 덮어쓰기 (새로 만듦)
    # Q5.  q5. 그럼 테이블은 어떤식으로 만들어? 보통 pk, 자료형부터 설정하는건데 이건 알아서 만든다면서 
    # --> 제약 조건은 보통 자동세팅이 안됌. 따라서 실무에서는 파이썬,SQL을 이용한하여 테이블을 DDL로 먼저 만들고 to_sql로 데이터만 넣는다.
    
    print("✅ 모든 작업 완료! 'user_grades' 테이블이 생성되었습니다.")
    
    
if __name__ == "__main__": # q4. 이건 뭐냐..? --> 파일을 직접 실행에서만 될아간다. 자세히는 모르겠음.
    etl_process()