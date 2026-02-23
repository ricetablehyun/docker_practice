🚀 E-commerce DW Pipeline: From Manual Judgment to Automated Logic

"수동으로 진행하던 데이터 파이프라인의 판단 과정을 자동화된 코드(Shell Script)로 이식하고, 인프라의 영속성을 설계하다."

본 프로젝트는 단순한 데이터 변환을 넘어, 운영체제(OS)와 애플리케이션 간의 상호작용을 이해하고 데이터 오염을 방지하기 위한 **'방어적 설계(Defensive Design)'**를 구축한 실습 기록입니다.

🏗 아키텍처 비유: "자동 생수 정제 시스템"

GE (Circuit Breaker): 입구에서 오염물질을 검사하는 센서. 오염 시 공장 즉시 중단 (STOP).

dbt snapshot: 원수 상태 기록 로그북. 실패 시 경고 (WARN).

dbt run: 여과 및 정수 공정. 기계 고장 시 생산 중단 (STOP).

dbt test: 최종 생산품 품질 검사 (QC).

Crontab: 정해진 시간에 공장 전원을 올리는 타이머.

🧠 설계 사고 프로세스 (4-Step Design Thinking)

1️⃣ Step 1: 작업 순서 (What)

GE(입구 검증) ➔ dbt snapshot(이력) ➔ dbt run(변환) ➔ dbt test(출구 검증) ➔ dbt docs(문서화)

2️⃣ Step 2: 실패 대응 (What If) ⭐

인간의 판단(눈)을 기계의 논리(Exit Code)로 번역했습니다.

치명적 단계 (GE, dbt run): 실패 시 exit 1로 파이프라인 즉시 중단. (GIGO 방지)

운영적 단계 (Test, Docs): 실패 시 WARN 로그만 남기고 계속 진행. (가용성 확보)

3️⃣ Step 3: 기록 및 가시성 (Record)

>> pipeline_$(date +%Y%m%d).log 2>&1: 날짜별 독립적 로그 생성으로 추적성 확보.

실행 결과 요약(PASS/WARN/STOP)을 통해 아침에 로그 파일 하나로 전체 상황 파악 가능.

4️⃣ Step 4: 환경 정하기 (Where)

source ~/anaconda/etc/profile.d/conda.sh: 환경변수 누락으로 인한 '침묵의 실패' 방지.

절대 경로의 원칙: Cron이라는 '유령 환경'에서도 멱등성(Idempotency)을 유지하도록 설계.

🧐 Engineering Contemplations (설계자의 고찰)

실습 과정에서 발생한 기술적 부채와 향후 개선 방향에 대한 기록입니다.

📍 하드코딩(Hardcoding)의 딜레마

문제: Cron 실행을 위해 /Users/sanghyun/... 식의 절대 경로를 하드코딩했습니다.

고찰: 이는 현재 환경에서는 작동하지만, 다른 서버로 이주할 때(Portability) 큰 장애물이 됩니다.

해결 방향: 향후 **환경 설정 파일(Environment Variables/Config)**을 도입하여 소스 코드와 자원의 위치 정보를 분리할 계획입니다.

📍 변수명과 버전 관리의 중요성

문제: 파일 위치나 버전이 바뀔 때마다 스크립트 내부를 일일이 수정해야 하는 번거로움을 경험했습니다.

고찰: "환경 설정이 곧 시스템의 지도"임을 깨달았습니다. Git을 통해 코드뿐만 아니라 이런 '환경의 변화' 자체를 추적해야 함을 체감했습니다.

해결 방향: 프로젝트 루트에 .env 파일을 활용하거나, 실행 시 인자(Argument)를 받아 유연하게 대처하는 구조로 고도화할 예정입니다.

🛠 실행 및 운영 (Operation)

1. 실행 권한 및 수동 트리거

chmod +x run_pipeline.sh
./run_pipeline.sh


2. 크론탭(Crontab) 자동화 설정

# 매일 정해진 시간 자동 가동 (모든 출력은 전용 로그로 수집)
31 14 * * * /Users/sanghyun/.../run_pipeline.sh >> /Users/sanghyun/.../logs/cron.log 2>&1


⚠️ 주니어의 함정 (Common Traps)

Zsh Globbing: kill -9 [PID]에서 대괄호 []를 사용하여 발생한 구문 오류 해결. (리눅스 문서의 치환 기호와 쉘의 특수 기호 혼동 주의)

Process Tracking: ps aux | grep -v grep을 통해 유령 프로세스를 식별하고 시스템 자원을 정확히 회수하는 '사격 절차' 정립.

macOS 보안 정책: '전체 디스크 접근 권한' 설정 미비로 인한 Cron 실행 실패 문제 해결.

[Fact] 이 문서는 단순한 코드 설명서가 아닙니다. 인프라의 영속성과 데이터의 품질을 동시에 책임지려는 데이터 엔지니어의 설계 철학이 담긴 리포트입니다.