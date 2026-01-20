# Kafka Streams 리뷰 집계 프로젝트 요약

## 📊 프로젝트 개요

### 목표
변환(Spark)·분석(LLM) 결과를 조인/집계하여 per-review 행 데이터와 job 단위 통합 통계를 실시간으로 생성하는 Kafka Streams 애플리케이션 구축

### 핵심 가치
- **실시간성**: 변환/분석 스트림을 조인해 즉시 결과 발행
- **정확성**: Control 토픽 기반 완료 검증과 통합 통계 산출
- **탄력성**: 직렬화/버전/배포 이슈에 강한 운영 패턴 확립
- **확장성**: 키워드/별점/감정 분포 등 통계 확장 구조

---

## 🏗️ 시스템 아키텍처

### 파이프라인 흐름
```
Collection → Transform(Spark) → Analysis(LLM) → Aggregation(Kafka Streams)
        ↓             ↓                 ↓                 ↓
   Control Topic  Control Topic    Control Topic     Control Topic
```

### 토픽 구성(핵심)
- 입력: `realtime-review-transform-topic`, `realtime-review-analysis-topic`, `job-control-topic`
- 출력: `review-rows`(per-review), `review-agg-by-job`(job 통합), `job-control-topic`(상태 업데이트)

---

## 🚀 주요 성과

1) **통합 조인/집계 구현**: 변환·분석 스트림을 `job_id|review_id` 키로 조인하여 행 데이터 생성, `job_id`로 최종 통계 집계
2) **완료 관리 체계화**: Control 토픽으로 expected/actual 검증 및 단계별 완료/실패/타임아웃 관리
3) **통계 고도화**: 별점 분포, 평균/구분별 평균, 키워드 태그 분포(체험단/일반 구분), 감정 비율 제공
4) **운영 안정화**: 직렬화 충돌, JDK 버전, 이미지 캐시 이슈 해결 패턴 정립(재파티션/Always pull/앱 ID 리셋)

---

## 💻 핵심 작업 내용

### 1. 스트림 토폴로지
- Transform 스트림: reviews 배열 평탄화 → per-review JSON String 변환 → 키 `job_id|review_id` → 명시적 `repartition(String,String)`
- Analysis 스트림: results 평탄화 → per-review JSON String → 동일한 키/재파티션 적용
- 윈도우 조인: inactivity gap 90초로 per-review 조인 → `review-rows` 발행
- 최종 집계: Transform/Analysis 집계를 `job_id`로 조인 → `review-agg-by-job` 발행

### 2. Control 기반 완료/실패 관리
- Control 스키마: `job_id, status(done|failed|timeout), step, expected_count, actual_count, timestamp, error_message`
- 완료 판정: 단계별로 actual=expected 일치 시 완료, 미일치/타임아웃은 실패 처리 권장

### 3. 확장 통계
- Transform 통계: 유효/빈/체험단 개수, 전체/구분별 평균, 별점 분포(all/체험단/일반/빈)
- 키워드 통계: 상위 키워드 10개, 태그별 개수/비율(전체/체험단/일반)
- Analysis 통계: 감정 분포(전체/체험단/일반) 및 비율

---

## 🔧 문제 해결 과정(핵심 이슈)

1) **`UnsupportedClassVersionError`**
- 원인: 컨테이너 JRE 11과 클래스 파일 버전 불일치
- 해결: 런타임 이미지를 Temurin JRE 21로 통일 및 리빌드

2) **`ClassCastException (ObjectNode → StringSerializer)`**
- 원인: repartition 경로에서 value가 ObjectNode 상태 유지
- 해결: selectKey 전 String 변환 + selectKey 후 `repartition(Repartitioned.with(String,String))` 명시, sink/aggregate 모두 String Serde 사용

3) **이미지 캐시로 수정 미반영**
- 해결: 이미지 태그 증가(예: 0.1.2), `imagePullPolicy: Always`로 강제 갱신, `APPLICATION_ID` 변경로 내부 토픽 리셋

---

## 📈 성과 지표(예시)
- **조인 지연**: 수초 내 (윈도우 90초, 입력 지연에 비례)
- **정확도**: Control 기반 expected/actual 일치율로 검증 가능
- **안정성**: 재파티션/Serde 정책 표준화로 직렬화 오류 제거

---

## 🔮 향후 개선 계획
- RocksDB 메모리 최적화 및 state store 압축 설정 튜닝
- 키/파티션 전략 개선으로 스케일 아웃 및 핫파티션 완화
- 상세 메트릭/알람(조인 지연, state store hit/miss, drop률) 강화

---

## 🧭 배포/운영 스냅샷
- 애플리케이션 ID: `review-aggregator-v3`(기본) 또는 `review-aggregator-enhanced-v1`(확장 사양)
- 이미지: `hahxowns/review-aggregator:0.1.2` 또는 `hahxowns/review-aggregator-enhanced:0.2.0`
- 배포 가이드: `kafka-streams/docs/KAFKA_STREAMS_DEPLOYMENT.md`, `kafka_stream/deploy-enhanced.sh`

---

## 📚 참고 문서/코드
- `kafka-streams/docs/ENHANCED_KAFKA_STREAMS_SPECIFICATION.md`
- `kafka-streams/docs/KAFKA_STREAMS_DEPLOYMENT.md`
- `kafka-streams/docs/CONTROL_TOPIC_SPECIFICATION.md`
- `kafka-streams/docs/CONTROL_TOPIC_IMPLEMENTATION_STATUS.md`
- `kafka-streams/aggregator/src/main/java/com/aggregator/*.java`

