#!/bin/bash
# ============================================================
# run_pipeline.sh — Week 4 이커머스 DW 파이프라인 자동화
# 
# 설계 정책:
#   GE error     → 🛑 STOP (데이터 품질 치명적)
#   dbt snapshot → ⚠️ WARN + 계속 (이력 손실, 도메인 판단)
#   dbt run      → 🛑 STOP (테이블 생성 실패)
#   dbt test     → ⚠️ WARN + 계속 (품질 경고)
#   dbt docs     → ⚠️ WARN + 계속 (문서화 실패일 뿐)
# ============================================================

# === Step 4: 환경 설정 (사람이 무의식적으로 하던 준비 작업) ===
source ~/anaconda/anaconda3/etc/profile.d/conda.sh
conda activate new_en
cd /Users/sanghyun/inha/project/혼자공부/DE_engineer/DOCKER/01_Study/4_week/week4_pj/02_dbt/week4_pratice

# === Step 3: 로그 설정 (날짜별 기록) ===
TODAY=$(date +%Y%m%d)
LOG_DIR="./logs"
LOG_FILE="${LOG_DIR}/pipeline_${TODAY}.log"
mkdir -p $LOG_DIR

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> $LOG_FILE
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"   # 화면에도 출력
}

log "=========================================="
log "파이프라인 시작"
log "=========================================="

# === Step 1-①: Data Validation (GE 서킷 브레이커) ===
log "[1/5] GE 서킷 브레이커 실행 중..."

# GE 스크립트는 프로젝트 외부에 있으므로 경로 지정
python /Users/sanghyun/inha/project/혼자공부/DE_engineer/DOCKER/01_Study/4_week/week4_pj/02_dbt/01_GE.py >> $LOG_FILE 2>&1

# Step 2: GE error → STOP
if [ $? -ne 0 ]; then
    log "🛑 [STOP] GE 서킷 브레이커 실패 — 데이터 품질 치명적. 파이프라인 중단."
    exit 1
fi
log "✅ [PASS] GE 서킷 브레이커 통과"

# === Step 1-②: State Capture (dbt snapshot) ===
log "[2/5] dbt snapshot 실행 중..."
dbt snapshot >> $LOG_FILE 2>&1

# Step 2: snapshot → WARN + 계속 (이력 손실이지만 최신 데이터가 더 중요)
if [ $? -ne 0 ]; then
    log "⚠️ [WARN] dbt snapshot 실패 — 변경 이력 미기록. 계속 진행."
else
    log "✅ [PASS] dbt snapshot 완료"
fi

# === Step 1-③: Transformation (dbt run) ===
log "[3/5] dbt run 실행 중..."
dbt run >> $LOG_FILE 2>&1

# Step 2: dbt run error → STOP
if [ $? -ne 0 ]; then
    log "🛑 [STOP] dbt run 실패 — 테이블 생성 오류. 파이프라인 중단."
    exit 1
fi
log "✅ [PASS] dbt run 완료"

# === Step 1-④: Quality Test (dbt test) ===
log "[4/5] dbt test 실행 중..."
dbt test >> $LOG_FILE 2>&1

# Step 2: dbt test → WARN + 계속
if [ $? -ne 0 ]; then
    log "⚠️ [WARN] dbt test 경고 발생 — 품질 이슈 확인 필요. 계속 진행."
else
    log "✅ [PASS] dbt test 전체 통과"
fi

# === Step 1-⑤: Cataloging (dbt docs) ===
log "[5/5] dbt docs generate 실행 중..."
dbt docs generate >> $LOG_FILE 2>&1

# Step 2: docs → WARN + 계속
if [ $? -ne 0 ]; then
    log "⚠️ [WARN] dbt docs 실패 — 문서 미갱신. 계속 진행."
else
    log "✅ [PASS] dbt docs 생성 완료"
fi

# === 최종 리포트 ===
log "=========================================="
log "파이프라인 완료"
log "로그 파일: $LOG_FILE"
log "=========================================="

# 로그에서 STOP/WARN/PASS 요약
echo ""
echo "=== 실행 요약 ==="
echo "PASS: $(grep -c 'PASS' $LOG_FILE)건"
echo "WARN: $(grep -c 'WARN' $LOG_FILE)건"
echo "STOP: $(grep -c 'STOP' $LOG_FILE)건"
