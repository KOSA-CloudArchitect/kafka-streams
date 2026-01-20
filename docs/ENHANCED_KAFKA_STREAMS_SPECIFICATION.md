# 향상된 Kafka Streams 리뷰 집계 시스템 명세서

## 개요

기존 Kafka Streams 리뷰 집계 애플리케이션을 확장하여 작업 완료 관리, 향상된 데이터 조인, 그리고 상세한 집계 통계를 제공하는 시스템입니다.

## 시스템 아키텍처

### 전체 파이프라인 흐름
```
Data Collection → Transform (Spark) → Analysis (LLM) → Aggregation (Kafka Streams)
     ↓                ↓                    ↓                    ↓
Control Topic ← Control Topic ← Control Topic ← Control Topic
```

### 토픽 구성
- **입력 토픽**: 
  - `realtime-review-transform-topic`: Spark 처리 결과
  - `realtime-review-analysis-topic`: LLM 분석 결과
  - `job-control-topic`: 작업 완료 관리 (신규)
- **출력 토픽**:
  - `review-rows`: 조인된 개별 리뷰 데이터
  - `review-agg-by-job`: job_id별 집계 통계
  - `job-control-topic`: 작업 상태 업데이트 (신규)
  - `review-dlq`: 실패 처리 (기존)

## 주요 기능

### 1. 작업 단위 완료 관리

#### Control 토픽 스키마
```json
{
  "job_id": "string",
  "status": "string",  // "done", "failed", "timeout"
  "step": "string",    // "collection", "transform", "analysis", "aggregation"
  "expected_count": "integer",
  "actual_count": "integer",
  "timestamp": "string",
  "error_message": "string"  // 실패 시에만
}
```

#### 완료 조건 검증
1. **Collection 완료**: 데이터 수집 서버에서 발행
2. **Transform 완료**: Transform 토픽의 job_id별 리뷰 수 = expected_count
3. **Analysis 완료**: Analysis 토픽의 job_id별 결과 수 = expected_count
4. **Aggregation 완료**: 집계 처리 완료 후 발행

#### 실패 처리 기준 (권장)
- **타임아웃**: 각 단계별 30분 내 완료되지 않으면 실패
- **수량 불일치**: expected_count와 actual_count가 일치하지 않으면 실패
- **데이터 품질**: 필수 필드 누락 시 실패

### 2. Raw Data 조인 및 발행

#### 조인 로직
- **키**: `job_id|review_id` (기존과 동일)
- **윈도우**: 90초 inactivity gap (기존과 동일)
- **매핑**: Transform의 모든 필드 + Analysis의 summary, sentiment

#### 조인된 데이터 스키마
```json
{
  "job_id": "string",
  "review_id": "string",
  "product_id": "string",
  "title": "string",
  "tag": "string",
  "review_count": "integer",
  "sales_price": "integer",
  "final_price": "integer",
  "rating": "number",
  "review_date": "string",
  "review_text": "string",
  "clean_text": "string",
  "keywords": "object",
  "review_help_count": "integer",
  "crawled_at": "string",
  "is_coupang_trial": "integer",
  "is_empty_review": "integer",
  "is_valid_rating": "integer",
  "is_valid_date": "integer",
  "has_content": "integer",
  "is_valid": "integer",
  "invalid_reason": "array",
  "year": "integer",
  "month": "integer",
  "day": "integer",
  "quarter": "integer",
  "yyyymm": "string",
  "yyyymmdd": "string",
  "weekday": "string",
  "summary": "string",
  "sentiment": "string"
}
```

### 3. Transform 토픽 집계

#### 기본 집계 통계
```json
{
  "job_id": "string",
  "transform_stats": {
    "total_reviews": "integer",
    "valid_reviews": "integer",  // is_valid=1
    "empty_reviews": "integer",  // is_empty_review=1
    "coupang_trial_reviews": "integer",  // is_coupang_trial=1
    "avg_rating": "number",
    "avg_rating_excluding_empty": "number",
    "avg_rating_coupang_trial": "number",
    "avg_rating_regular": "number"
  }
}
```

#### 별점 분포 집계
```json
{
  "rating_distribution": {
    "all": {
      "1": "integer",
      "2": "integer", 
      "3": "integer",
      "4": "integer",
      "5": "integer"
    },
    "coupang_trial": {
      "1": "integer",
      "2": "integer",
      "3": "integer", 
      "4": "integer",
      "5": "integer"
    },
    "regular": {
      "1": "integer",
      "2": "integer",
      "3": "integer",
      "4": "integer", 
      "5": "integer"
    },
    "empty_review": {
      "1": "integer",
      "2": "integer",
      "3": "integer",
      "4": "integer",
      "5": "integer"
    }
  }
}
```

#### 키워드 분석 집계 (쿠팡체험단 구분)
```json
{
  "keyword_analysis": [
    {
      "keyword": "string",  // 키워드명 (예: "품질", "배송", "가격")
      "all_tags": [         // 전체 태그 분석
        {
          "tag": "string",  // 태그명 (예: "긍정", "부정", "중립")
          "count": "integer",
          "percentage": "number"
        }
      ],
      "coupang_tags": [     // 쿠팡체험단 태그 분석
        {
          "tag": "string",
          "count": "integer", 
          "percentage": "number"
        }
      ],
      "regular_tags": [     // 일반 구매자 태그 분석
        {
          "tag": "string",
          "count": "integer",
          "percentage": "number"
        }
      ]
    }
  ]
}
```

**키워드 분석 특징:**
- **빈 리뷰 제외**: `is_empty_review = 1`인 리뷰는 키워드 분석에서 제외
- **쿠팡체험단 구분**: `is_coupang_trial` 필드를 활용하여 구분 집계
- **상위 키워드**: 전체 태그 개수가 많은 키워드 상위 10개만 선별
- **태그별 정렬**: 각 구분별로 태그는 개수 기준 내림차순 정렬
- **비율 계산**: 각 태그의 해당 구분 내 비율을 백분율로 계산

### 4. Analysis 토픽 집계

#### 감정 분석 집계
```json
{
  "job_id": "string",
  "analysis_stats": {
    "total_analyzed": "integer",
    "all_sentiment": {
      "positive": "integer",
      "negative": "integer", 
      "neutral": "integer",
      "positivePct": "number",
      "negativePct": "number",
      "neutralPct": "number"
    },
    "coupang_trial_sentiment": {
      "positive": "integer",
      "negative": "integer",
      "neutral": "integer",
      "positivePct": "number", 
      "negativePct": "number",
      "neutralPct": "number"
    },
    "regular_sentiment": {
      "positive": "integer",
      "negative": "integer",
      "neutral": "integer",
      "positivePct": "number",
      "negativePct": "number", 
      "neutralPct": "number"
    }
  }
}
```

### 5. 통합 집계 및 주기적 발행

#### 최종 집계 스키마 (review-agg-by-job 토픽)
```json
{
  "job_id": "string",
  "transform_stats": {
    "total_reviews": "integer",
    "valid_reviews": "integer",
    "empty_reviews": "integer", 
    "coupang_trial_reviews": "integer",
    "avg_rating": "number",
    "avg_rating_excluding_empty": "number",
    "avg_rating_coupang_trial": "number",
    "avg_rating_regular": "number",
    "rating_distribution": {
      "all": {"1": "integer", "2": "integer", "3": "integer", "4": "integer", "5": "integer"},
      "coupang_trial": {"1": "integer", "2": "integer", "3": "integer", "4": "integer", "5": "integer"},
      "regular": {"1": "integer", "2": "integer", "3": "integer", "4": "integer", "5": "integer"},
      "empty_review": {"1": "integer", "2": "integer", "3": "integer", "4": "integer", "5": "integer"}
    },
    "keyword_analysis": [
      {
        "keyword": "string",
        "all_tags": [
          {
            "tag": "string",
            "count": "integer",
            "percentage": "number"
          }
        ],
        "coupang_tags": [
          {
            "tag": "string", 
            "count": "integer",
            "percentage": "number"
          }
        ],
        "regular_tags": [
          {
            "tag": "string",
            "count": "integer", 
            "percentage": "number"
          }
        ]
      }
    ]
  },
  "analysis_stats": {
    "total_analyzed": "integer",
    "all_sentiment": {
      "positive": "integer", "negative": "integer", "neutral": "integer",
      "positivePct": "number", "negativePct": "number", "neutralPct": "number"
    },
    "coupang_trial_sentiment": {
      "positive": "integer", "negative": "integer", "neutral": "integer",
      "positivePct": "number", "negativePct": "number", "neutralPct": "number"
    },
    "regular_sentiment": {
      "positive": "integer", "negative": "integer", "neutral": "integer",
      "positivePct": "number", "negativePct": "number", "neutralPct": "number"
    }
  }
}
```

#### 발행 방식
- **실시간 발행**: Transform과 Analysis 집계가 완료되면 즉시 `review-agg-by-job` 토픽에 발행
- **job_id 기반 조인**: Transform 집계와 Analysis 집계를 `job_id`로 조인하여 통합 결과 생성
- **Kafka Streams KTable**: 상태 저장소를 통해 누적 집계 및 실시간 업데이트 제공

## 구현 세부사항

### 애플리케이션 ID
- `review-aggregator-enhanced-v1`

### 주요 클래스 구조
```
com.example.aggregator.enhanced/
├── EnhancedReviewAggregator.java     // 메인 애플리케이션 (통합 집계 로직 포함)
├── ControlTopicManager.java          // 작업 완료 관리
├── EnhancedJsonUtils.java            // 확장된 JSON 유틸리티
└── 내부 클래스들:
    ├── TransformStats.java           // Transform 통계 데이터 클래스
    ├── AnalysisStats.java            // Analysis 통계 데이터 클래스
    ├── RatingDistribution.java       // 별점 분포 데이터 클래스
    ├── RatingCounts.java             // 별점별 개수 데이터 클래스
    ├── SentimentDistribution.java    // 감정 분포 데이터 클래스
    ├── KeywordAnalysis.java          // 키워드 분석 데이터 클래스 (쿠팡체험단 구분)
    ├── KeywordTag.java               // 키워드 태그 데이터 클래스
    ├── KeywordStats.java             // 키워드 통계 데이터 클래스
    ├── TransformStatsSerde.java      // Transform 통계 직렬화
    └── AnalysisStatsSerde.java       // Analysis 통계 직렬화
```

### 환경 변수
```bash
# 기존 변수들
BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
APPLICATION_ID=review-aggregator-enhanced-v1
INPUT_TRANSFORM_TOPIC=realtime-review-transform-topic
INPUT_ANALYSIS_TOPIC=realtime-review-analysis-topic
OUTPUT_AGG_TOPIC=review-agg-by-job
OUTPUT_ROWS_TOPIC=review-rows

# 신규 변수들
CONTROL_TOPIC=job-control-topic
PUBLISH_INTERVAL_MS=300000  # 5분
TIMEOUT_MS=1800000         # 30분
WINDOW_INACTIVITY_MS=90000 # 90초
```

### Kafka Streams 토폴로지
1. **Control Stream**: job-control-topic 모니터링 및 작업 완료 상태 관리
2. **Transform Stream**: transform 토픽 처리 및 상세 통계 집계 (별점 분포 포함)
3. **Analysis Stream**: analysis 토픽 처리 및 감정 분석 집계
4. **Join Stream**: transform과 analysis를 `job_id|review_id`로 조인하여 개별 리뷰 데이터 생성
5. **Aggregation Stream**: Transform과 Analysis 집계를 `job_id`로 조인하여 최종 통합 결과 생성
6. **Output Stream**: `review-agg-by-job` 토픽으로 최종 집계 결과 발행

## 배포 및 운영

### Docker 이미지
- `hahxowns/review-aggregator-enhanced:0.2.0`

### Kubernetes 배포
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: review-aggregator-enhanced
  namespace: core
spec:
  replicas: 1
  selector:
    matchLabels:
      app: review-aggregator-enhanced
  template:
    metadata:
      labels:
        app: review-aggregator-enhanced
    spec:
      containers:
        - name: app
          image: hahxowns/review-aggregator-enhanced:0.2.0
          imagePullPolicy: Always
          env:
            - name: BOOTSTRAP_SERVERS
              value: my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
            - name: APPLICATION_ID
              value: review-aggregator-enhanced-v1
            - name: CONTROL_TOPIC
              value: job-control-topic
            - name: PUBLISH_INTERVAL_MS
              value: "300000"
            - name: TIMEOUT_MS
              value: "1800000"
          resources:
            requests:
              cpu: "500m"
              memory: "1Gi"
            limits:
              cpu: "2"
              memory: "2Gi"
```

### 모니터링 지표
- **처리량**: 초당 처리 리뷰 수
- **지연시간**: 데이터 도착부터 집계 완료까지
- **완료율**: job 완료 성공률
- **실패율**: 각 단계별 실패율
- **집계 정확도**: 예상 수량과 실제 수량 일치율

## 구현 상태

### ✅ 완료된 기능
1. **Control 토픽 관리**: 작업 완료 상태 추적 및 관리
2. **Transform 집계**: 기본 통계 + 별점 분포 (all/coupang_trial/regular/empty_review)
3. **Analysis 집계**: 감정 분석 통계 (all/coupang_trial/regular)
4. **키워드 분석 집계**: 키워드별 태그 집계 (전체/쿠팡체험단/일반 구분)
5. **Raw Data 조인**: Transform과 Analysis를 `job_id|review_id`로 조인
6. **최종 집계**: Transform과 Analysis를 `job_id`로 조인하여 `review-agg-by-job` 토픽에 발행
7. **실시간 처리**: Kafka Streams KTable을 통한 실시간 집계 및 상태 관리

### 🚧 진행 중인 기능
1. **성능 최적화**: 메모리 사용량 및 처리 속도 개선

### 📋 구현 예정 기능
1. **모니터링 강화**: 상세한 메트릭 및 알림 시스템
2. **확장성 개선**: 대용량 데이터 처리 최적화

## 테스트 시나리오

### 단위 테스트
- 각 집계 로직별 테스트
- 키워드 분석 정확도 테스트
- 완료 조건 검증 테스트

### 통합 테스트
- 전체 파이프라인 테스트
- 실패 시나리오 테스트
- 성능 부하 테스트

### 운영 테스트
- 실제 데이터로 검증
- 장애 복구 테스트
- 모니터링 시스템 연동 테스트

## 위험 요소 및 대응 방안

### 위험 요소
1. **메모리 사용량 증가**: 더 많은 집계 데이터 저장
2. **처리 지연**: 복잡한 집계 로직으로 인한 지연
3. **데이터 일관성**: 분산 환경에서의 상태 동기화

### 대응 방안
1. **메모리 최적화**: 효율적인 데이터 구조 사용
2. **성능 튜닝**: 병렬 처리 및 캐싱 활용
3. **상태 관리**: Kafka Streams의 exactly-once 보장 활용

## 결론

이 향상된 Kafka Streams 시스템은 기존 기능을 유지하면서 작업 완료 관리, 상세한 집계 통계, 그리고 주기적 발행 기능을 추가합니다. 점진적인 마이그레이션을 통해 안정적인 운영이 가능하며, 확장 가능한 아키텍처로 향후 요구사항 변화에 대응할 수 있습니다.
