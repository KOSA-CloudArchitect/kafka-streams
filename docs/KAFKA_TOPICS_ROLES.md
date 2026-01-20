# Kafka 토픽 구성 및 역할 요약

## 📊 토픽별 역할 및 구성

| 토픽명 | 역할 | 파티션 | 복제본 | 보존 정책 | 보존 기간 | 주요 데이터 |
|--------|------|--------|--------|-----------|-----------|-------------|
| `realtime-review-collection-topic` | 원본 수집 데이터 | 1 | 1 | delete | 7일 | 수집된 리뷰 원본 |
| `realtime-review-transform-topic` | Spark 변환 결과 | 1 | 1 | delete | 7일 | 정규화/검증된 리뷰 |
| `realtime-review-analysis-topic` | LLM 분석 결과 | 1 | 1 | delete | 7일 | 요약/감정분석 결과 |
| `review-rows` | 조인된 개별 리뷰 | 1 | 1 | delete | 14일 | Transform+Analysis 조인 |
| `review-agg-by-job` | job별 집계 통계 | 1 | 1 | compact,delete | 14일 | 통합 집계 결과 |
| `job-control-topic` | 작업 완료 관리 | 1 | 1 | delete | 7일 | 단계별 완료/실패 상태 |
| `review-dlq` | 실패 처리 | 1 | 1 | delete | 14일 | 처리 실패 데이터 |
| `overall-summary-request-topic` | 전체 요약 요청 | 1 | 1 | delete | 7일 | 요약 요청 메시지 |

## 🔄 데이터 흐름

```
Collection → Transform → Analysis → Aggregation
     ↓           ↓          ↓          ↓
Control Topic ← Control Topic ← Control Topic ← Control Topic
     ↓           ↓          ↓          ↓
   DLQ ←─────── DLQ ←────── DLQ ←────── DLQ
```

## 📋 토픽별 상세 역할

### 1. 데이터 파이프라인 토픽
- **`realtime-review-collection-topic`**: 데이터 수집 서버에서 원본 리뷰 데이터 발행
- **`realtime-review-transform-topic`**: Spark가 정규화/검증/전처리한 결과 발행
- **`realtime-review-analysis-topic`**: LLM API가 요약/감정분석한 결과 발행

### 2. 집계 및 조인 토픽
- **`review-rows`**: Transform과 Analysis를 `job_id|review_id`로 조인한 개별 리뷰 데이터
- **`review-agg-by-job`**: job_id별 통합 집계 통계 (별점 분포, 키워드 분석, 감정 분포)

### 3. 제어 및 관리 토픽
- **`job-control-topic`**: 각 단계별 완료/실패 상태 관리 (Airflow 연동)
- **`review-dlq`**: 처리 실패한 데이터 격리 및 재처리 대기
- **`overall-summary-request-topic`**: 전체 요약 요청 처리

## 🎯 주요 특징

### 보존 정책
- **delete**: 일반적인 데이터 토픽 (7-14일 보존)
- **compact,delete**: 집계 토픽 (최신 상태 유지 + 오래된 데이터 삭제)

### 파티션 전략
- **단일 파티션**: 순서 보장 및 단순한 운영 (확장 시 파티션 수 증가 고려)

### 복제 전략
- **단일 복제**: 개발 환경 기준 (운영 환경에서는 3+ 복제 권장)

## 🔧 운영 고려사항

### 확장성
- 처리량 증가 시 파티션 수 증가 필요
- 복제본 수를 3개 이상으로 설정하여 가용성 확보

### 모니터링
- 각 토픽별 메시지 수, 지연시간, 오프셋 lag 모니터링
- DLQ 토픽 모니터링으로 실패 패턴 파악

### 보안
- 토픽별 접근 권한 설정 (ACL)
- 민감한 데이터는 암호화 고려
