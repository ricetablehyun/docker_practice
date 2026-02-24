🚀 Data Pipeline Cloud Migration: From Local to 24/7 Automation

"맥북에서만 도는 코드는 장난감이다. 시스템은 덮개를 닫아도 스스로 돌아가야 한다."

본 프로젝트는 로컬(macOS) 환경에서 수동으로 작동하던 데이터 파이프라인(dbt, Great Expectations)을 클라우드(AWS EC2 Linux)로 이주시키고, OS 종속성과 경로 하드코딩 문제를 해결하여 **완전 무인 자동화(Zero-touch Automation)**를 달성한 실전 마이그레이션 기록입니다.

🏗 1. 아키텍처 개요 (Architecture Overview)

비유 (Analogy): "가내 수공업에서 글로벌 자동화 공장으로"

AS-IS (Local): 맥북 주방. 내가 잠들거나 와이파이가 끊기면 파이프라인도 멈춤.

TO-BE (Cloud): AWS EC2. 24시간 자가발전기가 도는 임대 공장. 스케줄러(Cron)가 나 대신 매일 버튼을 누름.

인프라 스택 (Infra Stack)

Cloud Provider: AWS EC2 (t3.micro, Ubuntu 22.04 LTS)

Security: ED25519 Key Pair, SSH (Port 22, My IP Only Inbound)

Environment: Python 3.10, venv (Conda 배제)

Automation: Linux cron, Shell Script (.sh)

🧠 2. 마이그레이션 흐름 (Migration Flow)

데이터 파이프라인 이주는 단순한 '파일 복사'가 아닙니다. 환경(OS)의 재설계입니다.

Provisioning (공장 임대): AWS 콘솔에서 Ubuntu EC2 인스턴스를 띄우고, IAM 수준이 아닌 OS 수준의 보안(SSH Key) 통로를 개척함.

Code Transfer (도면 복사): scp를 활용해 로컬의 코드와 dbt 설정 파일(.dbt/profiles.yml)을 물리적으로 격리 전송. (※ 보안 원칙에 따라 .pem 키는 프로젝트에 포함하지 않고 ~/.ssh/에 격리 보관)

Environment Isolation (환경 재구축): 로컬의 무거운 Conda 대신, Linux 서버에 순정 venv를 세팅하여 OS 충돌을 방지.

Orchestration (자동화): 크론탭(crontab)에 쉘 스크립트를 등록하여 인간의 개입을 100% 제거.

🚨 3. 치명적 트러블슈팅 (The 3 Great Traps)

이주 과정에서 터진 3대 시스템 에러와 아키텍처적 해결 과정입니다.

Trap 1: "경로 하드코딩"의 붕괴 (Path Mismatch)

상황: ./run_pipeline.sh 실행 시 cd: /Users/sanghyun/...: No such file 에러 발생.

원인: macOS의 로컬 절대 경로가 Linux 환경(/home/ubuntu/)에 존재할 리 없음.

해결 (Best Practice): 스크립트 내부의 맥북 전용 경로를 삭제하고, Linux 서버의 디렉토리 구조에 맞게 수정. (궁극적으로는 $(dirname "$0") 등을 활용한 상대 경로 동적 탐색 도입 예정).

Trap 2: 의존성 지옥 (Cross-Platform Dependency Hell)

상황: pip freeze로 가져온 9KB짜리 requirements.txt 설치 시 appnope 관련 OSError 발생.

원인: "미국 집(Linux)에 이사 가면서 한국 220V 콘센트(macOS 전용 패키지)를 통째로 뜯어간 격." 로컬의 쓰레기 패키지까지 전부 덤프됨.

해결 (Best Practice): 기존 파일을 날리고, echo -e "pandas\ndbt-bigquery\ngreat_expectations" > requirements.txt로 핵심 라이브러리만 정제하여 덮어쓰기 완료.

Trap 3: 묵시적 중단 (Interactive Prompt in Automation)

상황: Ubuntu 서버 초기화 시 sudo apt update -y를 실행했으나, 보라색 GUI 창(needrestart)이 뜨면서 자동화 흐름이 멈춤.

원인: 설치 중 백그라운드 서비스 재시작 여부를 묻는 데몬의 개입.

해결 (Best Practice): DEBIAN_FRONTEND=noninteractive 환경변수를 명령어 앞에 주입하여, "화면 없는 로봇이니 질문하지 마라"고 OS에 강제 선언.

🛠 4. 실행 가이드 (Execution Manual)

이 파이프라인을 처음부터 다시 세팅하기 위한 런북(Runbook)입니다.

1) 서버 초기화 및 환경 세팅

# 무인(Non-interactive) 모드로 패키지 매니저 업데이트 및 venv 설치
sudo DEBIAN_FRONTEND=noninteractive apt update -y
sudo DEBIAN_FRONTEND=noninteractive apt install python3-venv -y

# 프로젝트 폴더 내 가상환경 생성 및 활성화
python3 -m venv venv
source venv/bin/activate

# 핵심 라이브러리만 정제하여 설치
echo -e "pandas\ndbt-bigquery\ngreat_expectations" > requirements.txt
pip install -r requirements.txt


2) 파이프라인 수동 트리거 (Manual Run)

chmod +x run_pipeline.sh
./run_pipeline.sh


(기대 결과: GE 서킷 브레이커 통과 -> dbt run -> 결과 로그 출력)

3) 24/7 자동화 스케줄링 (Cron)

crontab -e
# 매일 새벽 4시 0분 실행 및 모든 에러 로그 수집 설정 (아래 내용 추가)
0 4 * * * /home/ubuntu/week4_pratice/run_pipeline.sh >> /home/ubuntu/week4_pratice/pipeline.log 2>&1


💡 5. 설계자의 고찰 (Architect's Synthesis)

본 마이그레이션 프로젝트를 통해 '도구(Application)'와 '환경(Infrastructure)'을 분리하여 사고하는 법을 체득했습니다.

로컬에서 아무리 완벽한 Python/SQL 코드를 짰더라도, OS의 차이, 패키지의 충돌, 경로의 부재 앞에서는 무용지물이 됨을 뼈저리게 겪었습니다. 이 원시적인 '수동 마이그레이션'의 한계를 극복하기 위해, 향후 이 모든 환경(OS + Code + Dependencies)을 하나의 캡슐로 얼려버리는 Docker(도커) 컨테이너 기반 아키텍처로 고도화할 계획입니다.